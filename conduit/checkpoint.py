from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class TableCheckpoint:
    table_name: str
    pk_column: Optional[str]
    pk_type: Optional[str]
    total_rows: int
    migrated_rows: int
    last_cursor_value: Optional[str]
    status: str  # PENDING, IN_PROGRESS, COMPLETED, FAILED
    updated_at: str
    error_message: Optional[str] = None


class CheckpointStore:
    """Manages persistent SQLite-based checkpoints for resuming interrupted migrations."""

    def __init__(self, db_path: str | Path = ".conduit_state.db"):
        self.db_path = Path(db_path)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS conduit_jobs (
                    job_id TEXT PRIMARY KEY,
                    source_name TEXT NOT NULL,
                    target_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS conduit_tables (
                    job_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    pk_column TEXT,
                    pk_type TEXT,
                    total_rows INTEGER DEFAULT 0,
                    migrated_rows INTEGER DEFAULT 0,
                    last_cursor_value TEXT,
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    error_message TEXT,
                    PRIMARY KEY (job_id, table_name),
                    FOREIGN KEY (job_id) REFERENCES conduit_jobs(job_id) ON DELETE CASCADE
                );
            """)
            conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def get_or_create_job(self, source_name: str, target_name: str, custom_job_id: Optional[str] = None) -> str:
        job_id = custom_job_id or f"{source_name}_to_{target_name}"
        now = self._now()
        with self._get_connection() as conn:
            row = conn.execute("SELECT job_id FROM conduit_jobs WHERE job_id = ?", (job_id,)).fetchone()
            if not row:
                conn.execute(
                    "INSERT INTO conduit_jobs (job_id, source_name, target_name, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (job_id, source_name, target_name, "IN_PROGRESS", now, now),
                )
                conn.commit()
        return job_id

    def init_table(
        self,
        job_id: str,
        table_name: str,
        pk_column: Optional[str],
        pk_type: Optional[str],
        total_rows: int = 0,
    ) -> TableCheckpoint:
        now = self._now()
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM conduit_tables WHERE job_id = ? AND table_name = ?",
                (job_id, table_name),
            ).fetchone()

            if row:
                return TableCheckpoint(
                    table_name=row["table_name"],
                    pk_column=row["pk_column"],
                    pk_type=row["pk_type"],
                    total_rows=row["total_rows"],
                    migrated_rows=row["migrated_rows"],
                    last_cursor_value=row["last_cursor_value"],
                    status=row["status"],
                    updated_at=row["updated_at"],
                    error_message=row["error_message"],
                )

            conn.execute(
                """INSERT INTO conduit_tables 
                   (job_id, table_name, pk_column, pk_type, total_rows, migrated_rows, last_cursor_value, status, updated_at)
                   VALUES (?, ?, ?, ?, ?, 0, NULL, 'PENDING', ?)""",
                (job_id, table_name, pk_column, pk_type, total_rows, now),
            )
            conn.commit()

        return TableCheckpoint(
            table_name=table_name,
            pk_column=pk_column,
            pk_type=pk_type,
            total_rows=total_rows,
            migrated_rows=0,
            last_cursor_value=None,
            status="PENDING",
            updated_at=now,
        )

    def update_cursor(
        self,
        job_id: str,
        table_name: str,
        last_cursor_value: Any,
        newly_migrated_count: int,
    ) -> None:
        now = self._now()
        str_cursor = str(last_cursor_value) if last_cursor_value is not None else None
        with self._get_connection() as conn:
            conn.execute(
                """UPDATE conduit_tables 
                   SET last_cursor_value = ?,
                       migrated_rows = migrated_rows + ?,
                       status = 'IN_PROGRESS',
                       updated_at = ?
                   WHERE job_id = ? AND table_name = ?""",
                (str_cursor, newly_migrated_count, now, job_id, table_name),
            )
            conn.commit()

    def mark_completed(self, job_id: str, table_name: str) -> None:
        now = self._now()
        with self._get_connection() as conn:
            conn.execute(
                """UPDATE conduit_tables 
                   SET status = 'COMPLETED', updated_at = ?, error_message = NULL
                   WHERE job_id = ? AND table_name = ?""",
                (now, job_id, table_name),
            )
            conn.commit()

    def mark_failed(self, job_id: str, table_name: str, error_msg: str) -> None:
        now = self._now()
        with self._get_connection() as conn:
            conn.execute(
                """UPDATE conduit_tables 
                   SET status = 'FAILED', updated_at = ?, error_message = ?
                   WHERE job_id = ? AND table_name = ?""",
                (now, error_msg, job_id, table_name),
            )
            conn.commit()

    def get_table_checkpoint(self, job_id: str, table_name: str) -> Optional[TableCheckpoint]:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM conduit_tables WHERE job_id = ? AND table_name = ?",
                (job_id, table_name),
            ).fetchone()
            if not row:
                return None
            return TableCheckpoint(
                table_name=row["table_name"],
                pk_column=row["pk_column"],
                pk_type=row["pk_type"],
                total_rows=row["total_rows"],
                migrated_rows=row["migrated_rows"],
                last_cursor_value=row["last_cursor_value"],
                status=row["status"],
                updated_at=row["updated_at"],
                error_message=row["error_message"],
            )

    def list_tables(self, job_id: str) -> List[TableCheckpoint]:
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM conduit_tables WHERE job_id = ? ORDER BY table_name ASC",
                (job_id,),
            ).fetchall()
            return [
                TableCheckpoint(
                    table_name=r["table_name"],
                    pk_column=r["pk_column"],
                    pk_type=r["pk_type"],
                    total_rows=r["total_rows"],
                    migrated_rows=r["migrated_rows"],
                    last_cursor_value=r["last_cursor_value"],
                    status=r["status"],
                    updated_at=r["updated_at"],
                    error_message=r["error_message"],
                )
                for r in rows
            ]

    def reset_job(self, job_id: str) -> None:
        with self._get_connection() as conn:
            conn.execute("DELETE FROM conduit_tables WHERE job_id = ?", (job_id,))
            conn.execute("DELETE FROM conduit_jobs WHERE job_id = ?", (job_id,))
            conn.commit()
