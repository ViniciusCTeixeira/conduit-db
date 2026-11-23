from __future__ import annotations

from typing import Any, List, Sequence, Tuple
import pymysql


class DataWriter:
    """High-performance batch writer for MySQL/MariaDB with disabled safety locks during migration."""

    def __init__(self, conn: pymysql.Connection):
        self.conn = conn
        self._init_session()

    def _init_session(self) -> None:
        with self.conn.cursor() as cursor:
            # Disable constraints and checks for maximum bulk write speed
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            cursor.execute("SET UNIQUE_CHECKS = 0;")
            cursor.execute("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';")
            cursor.execute("SET autocommit = 0;")
        self.conn.commit()

    def restore_session(self) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            cursor.execute("SET UNIQUE_CHECKS = 1;")
            cursor.execute("SET autocommit = 1;")
        self.conn.commit()

    def write_batch(
        self,
        table_name: str,
        columns: Sequence[str],
        rows: Sequence[Sequence[Any]],
        upsert: bool = False,
    ) -> int:
        if not rows:
            return 0

        escaped_cols = ", ".join(f"`{c}`" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))

        if upsert:
            updates = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in columns)
            sql = f"INSERT INTO `{table_name}` ({escaped_cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        else:
            sql = f"INSERT INTO `{table_name}` ({escaped_cols}) VALUES ({placeholders})"

        with self.conn.cursor() as cursor:
            cursor.executemany(sql, rows)

        self.conn.commit()
        return len(rows)

    def truncate_table(self, table_name: str) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        self.conn.commit()
