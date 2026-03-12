from __future__ import annotations

import json
import re
from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from .store import TaskStore


class TaskRequestHandler(BaseHTTPRequestHandler):
    server_version = "CodexTaskServer/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._write_html(self._render_home())
            return
        if parsed.path == "/api/tasks":
            self._write_json({"tasks": self.server.store.list_tasks()})
            return
        if parsed.path == "/table.tsv":
            self._write_tsv()
            return
        self.send_error(HTTPStatus.NOT_FOUND, "unknown endpoint")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            body = self._read_json()
            if parsed.path == "/api/tasks":
                title = self._required_text(body, "title")
                detail = self._required_text(body, "detail")
                task = self.server.store.add_task(title=title, detail=detail)
                self._write_json(task, status=HTTPStatus.CREATED)
                return
            if parsed.path == "/api/tasks/claim":
                worker_id = self._required_text(body, "worker_id")
                lease_seconds = int(body.get("lease_seconds", 180))
                task = self.server.store.claim_next_task(worker_id, lease_seconds)
                self._write_json({"task": task}, status=HTTPStatus.OK)
                return

            match = re.fullmatch(r"/api/tasks/(\d+)/(heartbeat|complete|release)", parsed.path)
            if not match:
                self.send_error(HTTPStatus.NOT_FOUND, "unknown endpoint")
                return
            task_id = int(match.group(1))
            action = match.group(2)
            worker_id = self._required_text(body, "worker_id")
            if action == "heartbeat":
                lease_seconds = int(body.get("lease_seconds", 180))
                task = self.server.store.heartbeat(task_id, worker_id, lease_seconds)
            elif action == "complete":
                task = self.server.store.complete_task(task_id, worker_id, body.get("result_summary"))
            else:
                task = self.server.store.release_task(task_id, worker_id, body.get("error_message"))
            if task is None:
                self.send_error(HTTPStatus.CONFLICT, "task no longer owned by worker")
                return
            self._write_json(task, status=HTTPStatus.OK)
        except ValueError as exc:
            self.send_error(HTTPStatus.BAD_REQUEST, str(exc))

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _read_json(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length == 0:
            return {}
        raw = self.rfile.read(content_length)
        return json.loads(raw.decode("utf-8"))

    def _write_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _write_html(self, html: str) -> None:
        data = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _write_tsv(self) -> None:
        tasks = self.server.store.list_tasks()
        rows = ["标题\t任务详情\t状态"]
        rows.extend(f"{task['title']}\t{task['detail']}\t{task['status']}" for task in tasks)
        data = ("\n".join(rows) + "\n").encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/tab-separated-values; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _required_text(self, payload: dict[str, Any], key: str) -> str:
        value = str(payload.get(key, "")).strip()
        if not value:
            raise ValueError(f"{key} is required")
        return value

    def _render_home(self) -> str:
        rows = []
        for task in self.server.store.list_tasks():
            rows.append(
                "<tr>"
                f"<td>{task['id']}</td>"
                f"<td>{escape(task['title'])}</td>"
                f"<td>{escape(task['detail'])}</td>"
                f"<td>{escape(task['status'])}</td>"
                "</tr>"
            )
        body = "".join(rows) or "<tr><td colspan='4'>no tasks</td></tr>"
        return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>Codex Task Board</title>
  <style>
    body {{ font-family: Consolas, monospace; padding: 24px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #999; padding: 8px; vertical-align: top; }}
    th {{ background: #f5f5f5; text-align: left; }}
  </style>
</head>
<body>
  <h1>Codex Task Board</h1>
  <p>A1-C1 固定为：标题 / 任务详情 / 状态</p>
  <p>API: <code>/api/tasks</code>, <code>/api/tasks/claim</code>, <code>/table.tsv</code></p>
  <table>
    <thead><tr><th>ID</th><th>标题</th><th>任务详情</th><th>状态</th></tr></thead>
    <tbody>{body}</tbody>
  </table>
</body>
</html>"""


class TaskHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], store: TaskStore) -> None:
        super().__init__(server_address, TaskRequestHandler)
        self.store = store


def serve_forever(host: str, port: int, db_path: str) -> None:
    store = TaskStore(db_path)
    server = TaskHTTPServer((host, port), store)
    print(f"task server listening on http://{host}:{port}")
    server.serve_forever()
