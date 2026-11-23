from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple
import pymysql


class SchemaMigrator:
    """Extracts, transforms, and synchronizes DDL between databases."""

    def __init__(self, source_conn: pymysql.Connection, target_conn: pymysql.Connection):
        self.source = source_conn
        self.target = target_conn

    def get_create_table_sql(self, table_name: str) -> str:
        with self.source.cursor() as cursor:
            cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
            row = cursor.fetchone()
            if not row or len(row) < 2:
                raise ValueError(f"Could not retrieve DDL for table: {table_name}")
            return str(row[1])

    @staticmethod
    def strip_foreign_keys(create_sql: str) -> Tuple[str, List[str]]:
        """
        Separates table definition from foreign key constraints so tables can be
        created without deadlock or missing parent issues.
        """
        lines = create_sql.split("\n")
        filtered_lines: List[str] = []
        fk_constraints: List[str] = []

        # Matches lines like: CONSTRAINT `fk_name` FOREIGN KEY (`col`) REFERENCES `parent` (`id`)
        fk_pattern = re.compile(r"^\s*(?:CONSTRAINT\s+`?[^`\s]+`?\s+)?FOREIGN\s+KEY\s*\(.+?\)\s*REFERENCES\s*`?.+?`?\s*\(.+?\)", re.IGNORECASE)

        for line in lines:
            stripped = line.strip().rstrip(",")
            if fk_pattern.match(stripped):
                fk_constraints.append(stripped)
            else:
                filtered_lines.append(line)

        # Fix trailing commas before the closing parenthesis
        cleaned_sql = "\n".join(filtered_lines)
        cleaned_sql = re.sub(r",\s*\n\s*\)", "\n)", cleaned_sql)

        return cleaned_sql, fk_constraints

    def table_exists_in_target(self, table_name: str) -> bool:
        with self.target.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                """,
                (table_name,),
            )
            row = cursor.fetchone()
            return bool(row and row[0] > 0)

    def apply_schema(self, table_name: str, drop_if_exists: bool = False) -> None:
        raw_ddl = self.get_create_table_sql(table_name)
        clean_ddl, _ = self.strip_foreign_keys(raw_ddl)

        with self.target.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            if drop_if_exists:
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
            cursor.execute(clean_ddl)
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        self.target.commit()

    def apply_foreign_keys(self, table_name: str) -> None:
        raw_ddl = self.get_create_table_sql(table_name)
        _, fks = self.strip_foreign_keys(raw_ddl)
        if not fks:
            return

        with self.target.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            for fk in fks:
                alter_sql = f"ALTER TABLE `{table_name}` ADD {fk}"
                try:
                    cursor.execute(alter_sql)
                except Exception:
                    # Ignore if constraint already exists
                    pass
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        self.target.commit()
