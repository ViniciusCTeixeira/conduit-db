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

        pk = meta.primary_key

        if pk:
            # High-performance Keyset Pagination
            yield from self._stream_by_keyset(meta.name, escaped_cols, column_names, pk, resume_cursor)
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

    def _stream_by_unbuffered_cursor(
        self,
        table_name: str,
        escaped_cols: str,
        col_names: List[str],
    ) -> Generator[Tuple[List[Tuple[Any, ...]], List[str], Optional[Any]], None, None]:
        query = f"SELECT {escaped_cols} FROM `{table_name}`"

        # Using SSCursor (Server Side unbuffered streaming)
        with self.conn.cursor(pymysql.cursors.SSCursor) as ss_cursor:
            ss_cursor.execute(query)
            while True:
                rows = ss_cursor.fetchmany(self.chunk_size)
                if not rows:
                    break
                yield rows, col_names, None
