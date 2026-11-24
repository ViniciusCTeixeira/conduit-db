from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
import pymysql

from conduit.inspector import TableMeta
from conduit.writer import DataWriter


@dataclass
class TableSyncStats:
    table_name: str
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    unchanged: int = 0
    time_taken_seconds: float = 0.0

    @property
    def total_processed(self) -> int:
        return self.inserted + self.updated + self.deleted + self.unchanged


class DataSynchronizer:
    """
    Differential synchronization engine for MySQL/MariaDB.
    Uses windowed BIT_XOR(CRC32) hashing to rapidly skip identical blocks,
    and performs targeted INSERT, UPDATE, and optional DELETE (prune).
    """

    def __init__(
        self,
        source_conn: pymysql.Connection,
        target_conn: pymysql.Connection,
        chunk_size: int = 5000,
        prune: bool = False,
        dry_run: bool = False,
    ):
        self.source = source_conn
        self.target = target_conn
        self.chunk_size = chunk_size
        self.prune = prune
        self.dry_run = dry_run
        self.writer = DataWriter(self.target)

    def sync_table(
        self,
        meta: TableMeta,
        progress_callback: Optional[Callable[[int, int, int, int], None]] = None,
    ) -> TableSyncStats:
        t0 = time.time()
        stats = TableSyncStats(table_name=meta.name)

        col_names = [c.name for c in meta.columns]
        pk = meta.primary_key

        if not pk:
            # Fallback for tables without PK: full stream upsert/insert
            return self._sync_without_pk(meta, stats, progress_callback)

        escaped_cols = ", ".join(f"`{c}`" for c in col_names)
        cols_coalesce = ", '#', ".join(f"COALESCE(`{c}`, '')" for c in col_names)
        block_crc_expr = f"BIT_XOR(CRC32(CONCAT_WS('#', {cols_coalesce})))"
        row_crc_expr = f"CRC32(CONCAT_WS('#', {cols_coalesce}))"

        current_cursor: Optional[Any] = None

        while True:
            # 1. Fetch next keyset window from source
            params: list = []
            if current_cursor is not None:
                query_pks = (
                    f"SELECT `{pk}` FROM `{meta.name}` "
                    f"WHERE `{pk}` > %s "
                    f"ORDER BY `{pk}` ASC "
                    f"LIMIT {self.chunk_size}"
                )
                params.append(current_cursor)
            else:
                query_pks = (
                    f"SELECT `{pk}` FROM `{meta.name}` "
                    f"ORDER BY `{pk}` ASC "
                    f"LIMIT {self.chunk_size}"
                )

            with self.source.cursor() as cur:
                cur.execute(query_pks, params)
                pk_rows = cur.fetchall()

            if not pk_rows:
                break

            min_pk = pk_rows[0][0]
            max_pk = pk_rows[-1][0]
            current_cursor = max_pk

            # 2. Compute block hash on source and target for range [min_pk, max_pk]
            chk_query = (
                f"SELECT COUNT(*), {block_crc_expr} FROM `{meta.name}` "
                f"WHERE `{pk}` >= %s AND `{pk}` <= %s"
            )

            with self.source.cursor() as s_cur:
                s_cur.execute(chk_query, (min_pk, max_pk))
                s_count, s_hash = s_cur.fetchone()

            with self.target.cursor() as t_cur:
                t_cur.execute(chk_query, (min_pk, max_pk))
                t_count, t_hash = t_cur.fetchone()

            # 3. If block is identical: fast path skip
            if s_count == t_count and str(s_hash) == str(t_hash) and s_count > 0:
                stats.unchanged += s_count
                if progress_callback:
                    progress_callback(stats.inserted, stats.updated, stats.deleted, stats.unchanged)
                if len(pk_rows) < self.chunk_size:
                    break
                continue

            # 4. Slow path: resolve diff inside this block
            diff_src_query = (
                f"SELECT `{pk}`, {row_crc_expr}, {escaped_cols} FROM `{meta.name}` "
                f"WHERE `{pk}` >= %s AND `{pk}` <= %s"
            )
            with self.source.cursor() as s_cur:
                s_cur.execute(diff_src_query, (min_pk, max_pk))
                src_rows = s_cur.fetchall()

            diff_tgt_query = (
                f"SELECT `{pk}`, {row_crc_expr} FROM `{meta.name}` "
                f"WHERE `{pk}` >= %s AND `{pk}` <= %s"
            )
            with self.target.cursor() as t_cur:
                t_cur.execute(diff_tgt_query, (min_pk, max_pk))
                tgt_rows = t_cur.fetchall()

            tgt_map: Dict[Any, Any] = {r[0]: str(r[1]) for r in tgt_rows}
            src_pks_set = set()

            to_insert: List[Tuple[Any, ...]] = []
            to_update: List[Tuple[Any, ...]] = []

            for row in src_rows:
                row_pk = row[0]
                row_crc = str(row[1])
                full_row_data = row[2:]
                src_pks_set.add(row_pk)

                if row_pk not in tgt_map:
                    to_insert.append(full_row_data)
                elif tgt_map[row_pk] != row_crc:
                    to_update.append(full_row_data)
                else:
                    stats.unchanged += 1

            to_delete: List[Any] = []
            if self.prune:
                for tgt_pk in tgt_map:
                    if tgt_pk not in src_pks_set:
                        to_delete.append(tgt_pk)

            # 5. Apply changes to target
            if not self.dry_run:
                if to_insert:
                    self.writer.write_batch(meta.name, col_names, to_insert, upsert=False)
                if to_update:
                    self.writer.write_batch(meta.name, col_names, to_update, upsert=True)
                if to_delete:
                    self.writer.batch_delete(meta.name, pk, to_delete)

            stats.inserted += len(to_insert)
            stats.updated += len(to_update)
            stats.deleted += len(to_delete)

            if progress_callback:
                progress_callback(stats.inserted, stats.updated, stats.deleted, stats.unchanged)

            if len(pk_rows) < self.chunk_size:
                break

        # 6. Global prune pass: delete rows in target outside source range
        if self.prune and not self.dry_run:
            self._prune_orphaned_edges(meta.name, pk)

        stats.time_taken_seconds = round(time.time() - t0, 3)
        return stats

    def _sync_without_pk(
        self,
        meta: TableMeta,
        stats: TableSyncStats,
        callback: Optional[Callable[[int, int, int, int], None]] = None,
    ) -> TableSyncStats:
        col_names = [c.name for c in meta.columns]
        escaped_cols = ", ".join(f"`{c}`" for c in col_names)
        query = f"SELECT {escaped_cols} FROM `{meta.name}`"

        with self.source.cursor() as cur:
            cur.execute(query)
            while True:
                rows = cur.fetchmany(self.chunk_size)
                if not rows:
                    break
                if not self.dry_run:
                    self.writer.write_batch(meta.name, col_names, rows, upsert=False)
                stats.inserted += len(rows)
                if callback:
                    callback(stats.inserted, stats.updated, stats.deleted, stats.unchanged)

        return stats

    def _prune_orphaned_edges(self, table_name: str, pk: str) -> None:
        """Finds and deletes target records whose PKs don't exist in source at all."""
        with self.source.cursor() as s_cur:
            s_cur.execute(f"SELECT MIN(`{pk}`), MAX(`{pk}`) FROM `{table_name}`")
            src_min, src_max = s_cur.fetchone()

        if src_min is None or src_max is None:
            # Source is completely empty, wipe target if prune
            with self.target.cursor() as t_cur:
                t_cur.execute(f"TRUNCATE TABLE `{table_name}`")
            self.target.commit()
            return

        with self.target.cursor() as t_cur:
            t_cur.execute(
                f"DELETE FROM `{table_name}` WHERE `{pk}` < %s OR `{pk}` > %s",
                (src_min, src_max),
            )
        self.target.commit()
