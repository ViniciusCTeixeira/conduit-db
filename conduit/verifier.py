from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional
import pymysql


@dataclass
class VerificationResult:
    table_name: str
    source_count: int
    target_count: int
    count_matches: bool
    source_checksum: Optional[str] = None
    target_checksum: Optional[str] = None
    checksum_matches: bool = True
    error_message: Optional[str] = None


class DataVerifier:
    """Audits migrated tables by validating row counts and chunked checksums."""

    def __init__(self, source_conn: pymysql.Connection, target_conn: pymysql.Connection):
        self.source = source_conn
        self.target = target_conn

    def verify_table(self, table_name: str, check_hash: bool = False) -> VerificationResult:
        try:
            # 1. Exact Row Count
            with self.source.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                src_count = int(cur.fetchone()[0])

            with self.target.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                dst_count = int(cur.fetchone()[0])

            count_matches = (src_count == dst_count)

            src_hash: Optional[str] = None
            dst_hash: Optional[str] = None
            hash_matches = True

            # 2. Checksum (BIT_XOR of CRC32 over rows) if requested
            if check_hash and count_matches and src_count > 0:
                with self.source.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COLUMN_NAME FROM information_schema.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                        ORDER BY ORDINAL_POSITION ASC
                        """,
                        (table_name,),
                    )
                    cols = [f"COALESCE(`{r[0]}`, '')" for r in cur.fetchall()]

                cols_expr = ", '#', ".join(cols)
                hash_sql = f"SELECT BIT_XOR(CRC32(CONCAT_WS('#', {cols_expr}))) FROM `{table_name}`"

                with self.source.cursor() as cur:
                    cur.execute(hash_sql)
                    src_hash = str(cur.fetchone()[0])

                with self.target.cursor() as cur:
                    cur.execute(hash_sql)
                    dst_hash = str(cur.fetchone()[0])

                hash_matches = (src_hash == dst_hash)

            return VerificationResult(
                table_name=table_name,
                source_count=src_count,
                target_count=dst_count,
                count_matches=count_matches,
                source_checksum=src_hash,
                target_checksum=dst_hash,
                checksum_matches=hash_matches,
            )
        except Exception as e:
            return VerificationResult(
                table_name=table_name,
                source_count=-1,
                target_count=-1,
                count_matches=False,
                checksum_matches=False,
                error_message=str(e),
            )
