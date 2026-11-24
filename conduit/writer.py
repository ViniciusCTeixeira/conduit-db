from __future__ import annotations

from typing import Any, List, Sequence, Tuple
import pymysql
import pymysql.err


class DataWriter:
    """High-performance batch writer for MySQL/MariaDB with disabled safety locks during migration."""

    def __init__(self, conn: pymysql.Connection):
        self.conn = conn
        self._init_session()

    def _ensure_connected(self) -> None:
        """Ensures the MySQL connection is alive; re-initializes session if reconnected."""
        try:
            self.conn.ping(reconnect=False)
        except Exception:
            self.conn.ping(reconnect=True)
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
        self._ensure_connected()
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

        self._ensure_connected()
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
        self._ensure_connected()
        with self.conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        self.conn.commit()

    def drop_secondary_index(self, table_name: str, index_name: str) -> bool:
        """Drops a secondary index to maximize bulk insert throughput."""
        self._ensure_connected()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor.execute(f"ALTER TABLE `{table_name}` DROP INDEX `{index_name}`")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            self.conn.commit()
            return True
        except pymysql.err.OperationalError as e:
            # 1091: Can't DROP '...'; check that column/key exists
            if e.args and e.args[0] == 1091:
                return True
            raise

    def recreate_secondary_index(self, table_name: str, definition: str) -> bool:
        """Recreates a secondary index using its original DDL clause."""
        self._ensure_connected()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor.execute(f"ALTER TABLE `{table_name}` ADD {definition}")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            self.conn.commit()
            return True
        except pymysql.err.OperationalError as e:
            # 1061: Duplicate key name (already exists / finished)
            if e.args and e.args[0] == 1061:
                return True
            raise

    def batch_delete(self, table_name: str, pk_column: str, pk_values: Sequence[Any]) -> int:
        """Deletes a batch of rows by Primary Key."""
        if not pk_values:
            return 0
        self._ensure_connected()
        placeholders = ", ".join(["%s"] * len(pk_values))
        sql = f"DELETE FROM `{table_name}` WHERE `{pk_column}` IN ({placeholders})"
        with self.conn.cursor() as cursor:
            cursor.execute(sql, tuple(pk_values))
        self.conn.commit()
        return len(pk_values)
