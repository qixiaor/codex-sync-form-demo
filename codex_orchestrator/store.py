from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import parse_qs, unquote, urlparse

from . import STATUS_DONE, STATUS_PENDING, STATUS_RUNNING, VALID_STATUSES


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(value: datetime | None) -> str | None:
    return value.isoformat(timespec="seconds") if value else None


class _DBResult:
    def __init__(self, rows: list[Any], rowcount: int, lastrowid: int | None) -> None:
        self._rows = rows
        self.rowcount = rowcount
        self.lastrowid = lastrowid

    def fetchone(self) -> Any | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[Any]:
        return list(self._rows)


class _DBConnection:
    def __init__(self, dialect: str, raw_connection: Any) -> None:
        self.dialect = dialect
        self.raw_connection = raw_connection

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _DBResult:
        compiled = self._compile_sql(sql)
        if self.dialect == "sqlite":
            cursor = self.raw_connection.execute(compiled, params)
            rows = cursor.fetchall() if cursor.description else []
            return _DBResult(rows=rows, rowcount=cursor.rowcount, lastrowid=cursor.lastrowid)

        cursor = self.raw_connection.cursor()
        try:
            cursor.execute(compiled, params)
            rows = list(cursor.fetchall()) if cursor.description else []
            return _DBResult(rows=rows, rowcount=cursor.rowcount, lastrowid=cursor.lastrowid)
        finally:
            cursor.close()

    def begin(self) -> None:
        if self.dialect == "sqlite":
            self.raw_connection.execute("BEGIN IMMEDIATE")
            return
        self.raw_connection.begin()

    def commit(self) -> None:
        self.raw_connection.commit()

    def rollback(self) -> None:
        self.raw_connection.rollback()

    def close(self) -> None:
        self.raw_connection.close()

    def _compile_sql(self, sql: str) -> str:
        if self.dialect == "mysql":
            return sql.replace("?", "%s")
        return sql


class TaskStore:
    def __init__(self, db_target: str | Path) -> None:
        self.db_target = str(db_target)
        self.dialect, self.db_path, self.mysql_config = _parse_db_target(db_target)
        if self.dialect == "sqlite" and self.db_path is not None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _connect(self) -> Iterator[_DBConnection]:
        if self.dialect == "sqlite":
            raw_connection = sqlite3.connect(
                self.db_path,
                timeout=30,
                isolation_level=None,
                check_same_thread=False,
            )
            raw_connection.row_factory = sqlite3.Row
            connection = _DBConnection("sqlite", raw_connection)
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                yield connection
            finally:
                connection.close()
            return

        raw_connection = _open_mysql_connection(self.mysql_config)
        connection = _DBConnection("mysql", raw_connection)
        try:
            yield connection
        finally:
            connection.close()

    def _init_db(self) -> None:
        with self._connect() as conn:
            if self.dialect == "sqlite":
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
                self._ensure_task_columns(conn)
                conn.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_source
                    ON tasks (source_name, source_task_key)
                    WHERE source_name IS NOT NULL AND source_task_key IS NOT NULL
                    """
                )
                return

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    title TEXT NOT NULL,
                    detail LONGTEXT NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    claimed_by VARCHAR(255) NULL,
                    lease_expires_at VARCHAR(64) NULL,
                    attempt_count INT NOT NULL DEFAULT 0,
                    result_summary LONGTEXT NULL,
                    last_error LONGTEXT NULL,
                    created_at VARCHAR(64) NOT NULL,
                    updated_at VARCHAR(64) NOT NULL,
                    completed_at VARCHAR(64) NULL,
                    source_name VARCHAR(255) NULL,
                    source_task_key VARCHAR(255) NULL,
                    source_updated_at VARCHAR(64) NULL,
                    source_status VARCHAR(16) NULL,
                    INDEX idx_tasks_status (status),
                    UNIQUE KEY idx_tasks_source (source_name, source_task_key)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            self._ensure_task_columns(conn)
            self._ensure_mysql_indexes(conn)

    def _ensure_task_columns(self, conn: _DBConnection) -> None:
        columns = set(self._task_columns(conn))
        missing_columns = {
            "source_name": "TEXT" if self.dialect == "sqlite" else "VARCHAR(255) NULL",
            "source_task_key": "TEXT" if self.dialect == "sqlite" else "VARCHAR(255) NULL",
            "source_updated_at": "TEXT" if self.dialect == "sqlite" else "VARCHAR(64) NULL",
            "source_status": "TEXT" if self.dialect == "sqlite" else "VARCHAR(16) NULL",
        }
        for name, column_type in missing_columns.items():
            if name not in columns:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {name} {column_type}")
        if "source_status" in self._task_columns(conn):
            conn.execute("UPDATE tasks SET source_status = status WHERE source_status IS NULL")

    def _ensure_mysql_indexes(self, conn: _DBConnection) -> None:
        if self.dialect != "mysql":
            return
        indexes = {
            str(row.get("Key_name", "")).strip()
            for row in conn.execute("SHOW INDEX FROM tasks").fetchall()
            if isinstance(row, dict)
        }
        if "idx_tasks_source" not in indexes:
            conn.execute("CREATE UNIQUE INDEX idx_tasks_source ON tasks (source_name, source_task_key)")
        if "idx_tasks_status" not in indexes:
            conn.execute("CREATE INDEX idx_tasks_status ON tasks (status)")

    def _task_columns(self, conn: _DBConnection) -> list[str]:
        if self.dialect == "sqlite":
            return [str(row["name"]) for row in conn.execute("PRAGMA table_info(tasks)").fetchall()]
        return [str(row.get("Field", "")).strip() for row in conn.execute("SHOW COLUMNS FROM tasks").fetchall()]

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
        task = self.get_task(int(task_id))
        if task is None:
            raise RuntimeError("task insert failed")
        return task

    def upsert_external_task(
        self,
        source_name: str,
        source_task_key: str,
        title: str,
        detail: str,
        status: str,
    ) -> dict[str, Any]:
        if status not in VALID_STATUSES:
            raise ValueError(f"invalid status: {status}")
        now = to_iso(utc_now())
        with self._connect() as conn:
            conn.begin()
            try:
                row = conn.execute(
                    """
                    SELECT *
                    FROM tasks
                    WHERE source_name = ?
                      AND source_task_key = ?
                    """,
                    (source_name, source_task_key),
                ).fetchone()
                if row is None:
                    cursor = conn.execute(
                        """
                        INSERT INTO tasks (
                            title,
                            detail,
                            status,
                            source_name,
                            source_task_key,
                            source_updated_at,
                            source_status,
                            created_at,
                            updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (title, detail, status, source_name, source_task_key, now, status, now, now),
                    )
                    task_id = int(cursor.lastrowid)
                else:
                    previous_source_status = row["source_status"] if "source_status" in row.keys() else row["status"]
                    source_status_changed = previous_source_status != status
                    update_fields: list[str] = [
                        "title = ?",
                        "detail = ?",
                        "source_updated_at = ?",
                        "source_status = ?",
                        "updated_at = ?",
                    ]
                    update_values: list[Any] = [title, detail, now, status, now]

                    if row["status"] != STATUS_RUNNING and source_status_changed and status != row["status"]:
                        if status == STATUS_PENDING:
                            update_fields.extend(
                                [
                                    "status = ?",
                                    "claimed_by = NULL",
                                    "lease_expires_at = NULL",
                                    "result_summary = NULL",
                                    "last_error = NULL",
                                    "completed_at = NULL",
                                ]
                            )
                            update_values.append(status)
                        elif status == STATUS_DONE:
                            update_fields.extend(
                                [
                                    "status = ?",
                                    "claimed_by = NULL",
                                    "lease_expires_at = NULL",
                                    "result_summary = NULL",
                                    "last_error = NULL",
                                    "completed_at = ?",
                                ]
                            )
                            update_values.extend([status, now])

                    update_values.append(row["id"])
                    conn.execute(
                        f"""
                        UPDATE tasks
                        SET {", ".join(update_fields)}
                        WHERE id = ?
                        """,
                        tuple(update_values),
                    )
                    task_id = int(row["id"])
                task = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return self._row_to_dict(task)

    def list_tasks_for_source(self, source_name: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM tasks
                WHERE source_name = ?
                ORDER BY id
                """,
                (source_name,),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def list_sources(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT source_name
                FROM tasks
                WHERE source_name IS NOT NULL
                  AND TRIM(source_name) != ''
                ORDER BY source_name
                """
            ).fetchall()
        return [str(row["source_name"]) for row in rows]

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
            conn.begin()
            try:
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
                    conn.commit()
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
                    conn.rollback()
                    return None
                task = conn.execute("SELECT * FROM tasks WHERE id = ?", (row["id"],)).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return self._row_to_dict(task)

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any] | None:
        now = utc_now()
        lease_expires = now + timedelta(seconds=lease_seconds)
        with self._connect() as conn:
            conn.begin()
            try:
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
                    conn.rollback()
                    return None
                row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return self._row_to_dict(row)

    def complete_task(self, task_id: int, worker_id: str, result_summary: str | None) -> dict[str, Any] | None:
        now = utc_now()
        with self._connect() as conn:
            conn.begin()
            try:
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
                    conn.rollback()
                    return None
                row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return self._row_to_dict(row)

    def release_task(self, task_id: int, worker_id: str, error_message: str | None) -> dict[str, Any] | None:
        now = utc_now()
        with self._connect() as conn:
            conn.begin()
            try:
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
                    conn.rollback()
                    return None
                row = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return self._row_to_dict(row)

    def delete_task(self, task_id: int) -> bool:
        with self._connect() as conn:
            result = conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        return result.rowcount == 1

    def reset_tasks(self, source_name: str | None = None) -> int:
        with self._connect() as conn:
            if source_name and source_name.strip():
                result = conn.execute("DELETE FROM tasks WHERE source_name = ?", (source_name.strip(),))
            else:
                result = conn.execute("DELETE FROM tasks")
        return int(result.rowcount)

    def _requeue_expired_locked(self, conn: _DBConnection, now: datetime) -> None:
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

    def _row_to_dict(self, row: Any) -> dict[str, Any]:
        keys = set(row.keys())
        return {
            "id": row["id"],
            "title": row["title"],
            "detail": row["detail"],
            "status": row["status"],
            "source_name": row["source_name"] if "source_name" in keys else None,
            "source_task_key": row["source_task_key"] if "source_task_key" in keys else None,
            "source_updated_at": row["source_updated_at"] if "source_updated_at" in keys else None,
            "source_status": row["source_status"] if "source_status" in keys else None,
            "claimed_by": row["claimed_by"],
            "lease_expires_at": row["lease_expires_at"],
            "attempt_count": row["attempt_count"],
            "result_summary": row["result_summary"],
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "completed_at": row["completed_at"],
        }


def _parse_db_target(db_target: str | Path) -> tuple[str, Path | None, dict[str, Any] | None]:
    text = str(db_target).strip()
    parsed = urlparse(text)
    if parsed.scheme in {"mysql", "mysql+pymysql"}:
        return "mysql", None, _parse_mysql_config(parsed)
    if parsed.scheme == "sqlite":
        sqlite_path = _resolve_sqlite_url_path(parsed)
        return "sqlite", sqlite_path, None
    return "sqlite", Path(text), None


def _resolve_sqlite_url_path(parsed: Any) -> Path:
    if parsed.netloc and parsed.path:
        return Path(f"//{parsed.netloc}{parsed.path}")
    return Path(unquote(parsed.path))


def _parse_mysql_config(parsed: Any) -> dict[str, Any]:
    database = parsed.path.lstrip("/")
    if not database:
        raise ValueError("mysql database name is required in --db URL")
    query = parse_qs(parsed.query, keep_blank_values=True)
    charset = str(query.get("charset", ["utf8mb4"])[0]).strip() or "utf8mb4"
    connect_timeout = int(str(query.get("connect_timeout", ["10"])[0]).strip() or "10")
    return {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 3306,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": database,
        "charset": charset,
        "connect_timeout": connect_timeout,
    }


def _open_mysql_connection(config: dict[str, Any] | None) -> Any:
    if config is None:
        raise ValueError("mysql config is required")
    try:
        import pymysql
        from pymysql.cursors import DictCursor
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "PyMySQL is required for MySQL storage. Install it with: python -m pip install pymysql"
        ) from exc

    return pymysql.connect(
        host=config["host"],
        port=int(config["port"]),
        user=config["user"],
        password=config["password"],
        database=config["database"],
        charset=config["charset"],
        autocommit=True,
        connect_timeout=int(config["connect_timeout"]),
        cursorclass=DictCursor,
    )
