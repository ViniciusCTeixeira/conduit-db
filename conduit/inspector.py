from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
import pymysql

from conduit.config import DatabaseConfig


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    column_type: str
    is_nullable: bool
    column_default: Optional[str]
    extra: str
    column_comment: str


@dataclass
class TableMeta:
    name: str
    primary_key: Optional[str]  # Single PK column name if simple PK
    composite_pks: List[str] = field(default_factory=list)
    pk_type: Optional[str] = None
    columns: List[ColumnInfo] = field(default_factory=list)
    estimated_rows: int = 0
    depends_on: Set[str] = field(default_factory=set)


class DatabaseInspector:
    """Agnostic introspection engine querying information_schema."""

    def __init__(self, conn: pymysql.Connection, database_name: str):
        self.conn = conn
        self.database_name = database_name

    def list_tables(self) -> List[str]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT TABLE_NAME 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME ASC
                """,
                (self.database_name,),
            )
            rows = cursor.fetchall()
            return [r[0] for r in rows]

    def get_table_metadata(self, table_name: str) -> TableMeta:
        with self.conn.cursor() as cursor:
            # 1. Columns
            cursor.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION ASC
                """,
                (self.database_name, table_name),
            )
            col_rows = cursor.fetchall()
            columns: List[ColumnInfo] = []
            for col in col_rows:
                columns.append(
                    ColumnInfo(
                        name=col[0],
                        data_type=col[1].lower(),
                        column_type=col[2],
                        is_nullable=col[3].upper() == "YES",
                        column_default=col[4],
                        extra=col[5],
                        column_comment=col[6],
                    )
                )

            # 2. Primary Key
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION ASC
                """,
                (self.database_name, table_name),
            )
            pk_rows = cursor.fetchall()
            pks = [r[0] for r in pk_rows]

            single_pk: Optional[str] = None
            pk_type: Optional[str] = None
            if len(pks) == 1:
                single_pk = pks[0]
                for col in columns:
                    if col.name == single_pk:
                        pk_type = col.data_type
                        break

            # 3. Foreign key dependencies
            cursor.execute(
                """
                SELECT REFERENCED_TABLE_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s 
                  AND REFERENCED_TABLE_NAME IS NOT NULL
                """,
                (self.database_name, table_name),
            )
            fk_rows = cursor.fetchall()
            dependencies = {r[0] for r in fk_rows if r[0] and r[0] != table_name}

            # 4. Estimated rows from TABLES
            cursor.execute(
                """
                SELECT TABLE_ROWS
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (self.database_name, table_name),
            )
            stat_row = cursor.fetchone()
            estimated_rows = int(stat_row[0]) if stat_row and stat_row[0] is not None else 0

        return TableMeta(
            name=table_name,
            primary_key=single_pk,
            composite_pks=pks if len(pks) > 1 else [],
            pk_type=pk_type,
            columns=columns,
            estimated_rows=estimated_rows,
            depends_on=dependencies,
        )

    def get_exact_row_count(self, table_name: str) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            row = cursor.fetchone()
            return int(row[0]) if row else 0

    def sort_tables_topologically(self, tables: List[str]) -> List[str]:
        """Orders tables so referenced parent tables are migrated before child tables."""
        meta_map: Dict[str, TableMeta] = {}
        for t in tables:
            meta_map[t] = self.get_table_metadata(t)

        sorted_tables: List[str] = []
        visited: Set[str] = set()
        visiting: Set[str] = set()

        def visit(table: str) -> None:
            if table in visiting:
                # Circular dependency detected: break cycle
                return
            if table in visited:
                return

            visiting.add(table)
            meta = meta_map.get(table)
            if meta:
                for dep in meta.depends_on:
                    if dep in meta_map:
                        visit(dep)
            visiting.remove(table)
            visited.add(table)
            sorted_tables.append(table)

        for t in tables:
            if t not in visited:
                visit(t)

        return sorted_tables
