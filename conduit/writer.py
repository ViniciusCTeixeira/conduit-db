from __future__ import annotations

import socket
from typing import Any, List, Sequence, Tuple
import pymysql
import pymysql.err


def configure_socket_keepalive(conn: Any) -> None:
    """Configures aggressive OS-level TCP Keepalive on the underlying MySQL socket.

    This ensures that intermediate NAT routers, firewalls, and cloud security groups
    never terminate long-running idle connections (e.g. during large ALTER TABLE or bulk writes),
    even when connecting directly to remote databases without SSH.
    """
    try:
        sock = getattr(conn, "_sock", None)
        if sock is not None and hasattr(sock, "setsockopt"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, "TCP_KEEPIDLE"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
            if hasattr(socket, "TCP_KEEPINTVL"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 15)
            if hasattr(socket, "TCP_KEEPCNT"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
            elif hasattr(socket, "TCP_KEEPALIVE"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, 30)
    except Exception:
        pass


class DataWriter:
    """High-performance batch writer for MySQL/MariaDB with disabled safety locks during migration."""

    def __init__(self, conn: pymysql.Connection):
        self.conn = conn
        configure_socket_keepalive(self.conn)
        self._init_session()

    def _ensure_connected(self) -> None:
        """Ensures the MySQL connection is alive; re-initializes session if reconnected."""
        try:
            self.conn.ping(reconnect=False)
        except Exception:
            self.conn.ping(reconnect=True)
            configure_socket_keepalive(self.conn)
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

    def recreate_secondary_indexes_batch(self, table_name: str, definitions: Sequence[str]) -> bool:
        """Recreates multiple secondary indexes in a SINGLE ALTER TABLE statement.

        This allows MySQL InnoDB to scan the clustered index only ONCE and build
        all secondary indexes concurrently in sort buffers, reducing index creation
        time by 70-80% compared to individual sequential statements.
        """
        if not definitions:
            return True
        self._ensure_connected()

        # If only 1 index, delegate directly to single recreation
        if len(definitions) == 1:
            return self.recreate_secondary_index(table_name, definitions[0])

        clauses = ", ".join(f"ADD {d}" for d in definitions)
        sql = f"ALTER TABLE `{table_name}` {clauses}"

        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor.execute(sql)
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            self.conn.commit()
            return True
        except pymysql.err.OperationalError as e:
            # 1061: Duplicate key name (one or more indexes already exist)
            if e.args and e.args[0] == 1061:
                # Fall back to restoring each index individually so existing ones are skipped idempotently
                for d in definitions:
                    self.recreate_secondary_index(table_name, d)
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
