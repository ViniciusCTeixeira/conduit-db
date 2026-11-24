from __future__ import annotations

from typing import Any, Generator, List, Optional, Tuple
import pymysql
import pymysql.cursors

from conduit.inspector import TableMeta


class DataStreamer:
    """Streams data from source database chunk-by-chunk using Keyset Pagination and SSCursor."""

    def __init__(self, conn: pymysql.Connection, chunk_size: int = 5000):
        self.conn = conn
        self.chunk_size = chunk_size

    def stream_table(
        self,
        meta: TableMeta,
        resume_cursor: Optional[Any] = None,
    ) -> Generator[Tuple[List[Tuple[Any, ...]], List[str], Optional[Any]], None, None]:
        """
        Yields (rows_batch, column_names, next_cursor_value) chunks.
        """
        column_names = [col.name for col in meta.columns]
        escaped_cols = ", ".join(f"`{col}`" for col in column_names)

        pks = meta.primary_keys

        if len(pks) == 1:
            # High-performance Keyset Pagination (single PK)
            yield from self._stream_by_keyset(meta.name, escaped_cols, column_names, pks[0], resume_cursor)
        elif len(pks) > 1:
            # High-performance Keyset Pagination (composite PK)
            yield from self._stream_by_composite_keyset(meta.name, escaped_cols, column_names, pks, resume_cursor)
        else:
            # Fallback to Server-Side unbuffered streaming cursor
            yield from self._stream_by_unbuffered_cursor(meta.name, escaped_cols, column_names)

    def _stream_by_keyset(
        self,
        table_name: str,
        escaped_cols: str,
        col_names: List[str],
        pk: str,
        resume_cursor: Optional[Any],
    ) -> Generator[Tuple[List[Tuple[Any, ...]], List[str], Optional[Any]], None, None]:
        pk_idx = col_names.index(pk)
        current_cursor = resume_cursor

        while True:
            params: list = []
            if current_cursor is not None:
                query = (
                    f"SELECT {escaped_cols} FROM `{table_name}` "
                    f"WHERE `{pk}` > %s "
                    f"ORDER BY `{pk}` ASC "
                    f"LIMIT {self.chunk_size}"
                )
                params.append(current_cursor)
            else:
                query = (
                    f"SELECT {escaped_cols} FROM `{table_name}` "
                    f"ORDER BY `{pk}` ASC "
                    f"LIMIT {self.chunk_size}"
                )

            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()

            if not rows:
                break

            last_row = rows[-1]
            last_pk = last_row[pk_idx]
            current_cursor = last_pk

            yield rows, col_names, last_pk

            if len(rows) < self.chunk_size:
                break

    def _stream_by_composite_keyset(
        self,
        table_name: str,
        escaped_cols: str,
        col_names: List[str],
        pks: List[str],
        resume_cursor: Optional[Any],
    ) -> Generator[Tuple[List[Tuple[Any, ...]], List[str], Optional[Any]], None, None]:
        import json

        pk_indices = [col_names.index(pk) for pk in pks]
        escaped_pks = ", ".join(f"`{pk}`" for pk in pks)
        order_clause = ", ".join(f"`{pk}` ASC" for pk in pks)
        placeholders = ", ".join(["%s"] * len(pks))

        current_cursor: Optional[Tuple[Any, ...]] = None
        if resume_cursor is not None:
            if isinstance(resume_cursor, str):
                try:
                    parsed = json.loads(resume_cursor)
                    current_cursor = tuple(parsed) if isinstance(parsed, list) else None
                except Exception:
                    current_cursor = None
            elif isinstance(resume_cursor, (list, tuple)):
                current_cursor = tuple(resume_cursor)

        while True:
            params: list = []
            if current_cursor is not None:
                query = (
                    f"SELECT {escaped_cols} FROM `{table_name}` "
                    f"WHERE ({escaped_pks}) > ({placeholders}) "
                    f"ORDER BY {order_clause} "
                    f"LIMIT {self.chunk_size}"
                )
                params.extend(current_cursor)
            else:
                query = (
                    f"SELECT {escaped_cols} FROM `{table_name}` "
                    f"ORDER BY {order_clause} "
                    f"LIMIT {self.chunk_size}"
                )

            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()

            if not rows:
                break

            last_row = rows[-1]
            last_pk = tuple(last_row[idx] for idx in pk_indices)
            current_cursor = last_pk

            yield rows, col_names, last_pk

            if len(rows) < self.chunk_size:
                break

    def _stream_by_unbuffered_cursor(
        self,
        table_name: str,
        escaped_cols: str,
        col_names: List[str],
    ) -> Generator[Tuple[List[Tuple[Any, ...]], List[str], Optional[Any]], None, None]:
        query = f"SELECT {escaped_cols} FROM `{table_name}`"

        ss_cursor = self.conn.cursor(pymysql.cursors.SSCursor)
        try:
            ss_cursor.execute(query)
            while True:
                rows = ss_cursor.fetchmany(self.chunk_size)
                if not rows:
                    break
                yield rows, col_names, None
        except (GeneratorExit, KeyboardInterrupt):
            # Prevent PyMySQL unbuffered drain loop on interrupt
            if hasattr(ss_cursor, "_result") and ss_cursor._result is not None:
                ss_cursor._result.unbuffered_active = False
                ss_cursor._result.connection = None
            if hasattr(self.conn, "_result") and self.conn._result is getattr(ss_cursor, "_result", None):
                self.conn._result = None
            raise
        finally:
            try:
                if hasattr(ss_cursor, "_result") and ss_cursor._result is not None:
                    ss_cursor._result.unbuffered_active = False
                    ss_cursor._result.connection = None
                ss_cursor.close()
            except Exception:
                pass
