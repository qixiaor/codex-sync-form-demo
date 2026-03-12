from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from . import STATUS_DONE, STATUS_PENDING, STATUS_RUNNING, VALID_STATUSES


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: datetime | None) -> str | None:
    return value.isoformat(timespec="seconds") if value else None


class TaskStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(
            self.db_path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            yield connection
        finally:
            connection.close()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('未开始', '执行中', '已完成')),
                    claimed_by TEXT,
                    lease_expires_at TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    result_summary TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                )
                """
            )

    def add_task(self, title: str, detail: str, status: str = STATUS_PENDING) -> dict[str, Any]:
        if status not in VALID_STATUSES:
            raise ValueError(f"invalid status: {status}")
        now = to_iso(utc_now())
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO tasks (title, detail, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (title, detail, status, now, now),
            )
            task_id = cursor.lastrowid
        task = self.get_task(task_id)
        if task is None:
            raise RuntimeError("task insert failed")
        return task

    def list_tasks(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM tasks ORDER BY id").fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_task(self, task_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return self._row_to_dict(row) if row else None

    def claim_next_task(self, worker_id: str, lease_seconds: int) -> dict[str, Any] | None:
        now = utc_now()
        lease_expires = now + timedelta(seconds=lease_seconds)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._requeue_expired_locked(conn, now)
            row = conn.execute(
                """
                SELECT id
                FROM tasks
                WHERE status = ?
                ORDER BY id
                LIMIT 1
                """,
                (STATUS_PENDING,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            result = conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    claimed_by = ?,
                    lease_expires_at = ?,
                    attempt_count = attempt_count + 1,
                    updated_at = ?,
                    last_error = NULL
                WHERE id = ?
                  AND status = ?
                """,
                (
                    STATUS_RUNNING,
                    worker_id,
                    to_iso(lease_expires),
                    to_iso(now),
                    row["id"],
                    STATUS_PENDING,
                ),
            )
            if result.rowcount != 1:
                conn.execute("ROLLBACK")
                return None
            task = conn.execute("SELECT * FROM tasks WHERE id = ?", (row["id"],)).fetchone()
            conn.execute("COMMIT")
        return self._row_to_dict(task)

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any] | None:
        now = utc_now()
        lease_expires = now + timedelta(seconds=lease_seconds)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            result = conn.execute(
                """
                UPDATE tasks
                SET lease_expires_at = ?,
                    updated_at = ?
                WHERE id = ?
                  AND status = ?
                  AND claimed_by = ?
                """,
                (to_iso(lease_expires), to_iso(now), task_id, STATUS_RUNNING, worker_id),
            )
            if result.rowcount != 1:
                conn.execute("ROLLBACK")
                return None
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            conn.execute("COMMIT")
        return self._row_to_dict(row)

    def complete_task(self, task_id: int, worker_id: str, result_summary: str | None) -> dict[str, Any] | None:
        now = utc_now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            result = conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    claimed_by = NULL,
                    lease_expires_at = NULL,
                    result_summary = ?,
                    updated_at = ?,
                    completed_at = ?
                WHERE id = ?
                  AND status = ?
                  AND claimed_by = ?
                """,
                (
                    STATUS_DONE,
                    result_summary,
                    to_iso(now),
                    to_iso(now),
                    task_id,
                    STATUS_RUNNING,
                    worker_id,
                ),
            )
            if result.rowcount != 1:
                conn.execute("ROLLBACK")
                return None
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            conn.execute("COMMIT")
        return self._row_to_dict(row)

    def release_task(self, task_id: int, worker_id: str, error_message: str | None) -> dict[str, Any] | None:
        now = utc_now()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            result = conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    claimed_by = NULL,
                    lease_expires_at = NULL,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
                  AND status = ?
                  AND claimed_by = ?
                """,
                (STATUS_PENDING, error_message, to_iso(now), task_id, STATUS_RUNNING, worker_id),
            )
            if result.rowcount != 1:
                conn.execute("ROLLBACK")
                return None
            row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            conn.execute("COMMIT")
        return self._row_to_dict(row)

    def _requeue_expired_locked(self, conn: sqlite3.Connection, now: datetime) -> None:
        conn.execute(
            """
            UPDATE tasks
            SET status = ?,
                claimed_by = NULL,
                lease_expires_at = NULL,
                updated_at = ?,
                last_error = COALESCE(last_error, 'worker lease expired')
            WHERE status = ?
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at <= ?
            """,
            (STATUS_PENDING, to_iso(now), STATUS_RUNNING, to_iso(now)),
        )

    def _row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "title": row["title"],
            "detail": row["detail"],
            "status": row["status"],
            "claimed_by": row["claimed_by"],
            "lease_expires_at": row["lease_expires_at"],
            "attempt_count": row["attempt_count"],
            "result_summary": row["result_summary"],
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "completed_at": row["completed_at"],
        }
