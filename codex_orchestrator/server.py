from __future__ import annotations

import json
import re
from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

from .store import TaskStore


class TaskRequestHandler(BaseHTTPRequestHandler):
    server_version = "CodexTaskServer/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            query = parse_qs(parsed.query)
            message = query.get("message", [""])[0]
            error = query.get("error", [""])[0]
            self._write_html(self._render_home(message=message, error=error))
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
            if parsed.path == "/admin/delete":
                form = self._read_form()
                task_id = int(self._required_text(form, "task_id"))
                deleted = self.server.store.delete_task(task_id)
                if not deleted:
                    self._redirect_home(error=f"task {task_id} not found")
                    return
                self._redirect_home(message=f"deleted task {task_id}")
                return
            if parsed.path == "/admin/reset":
                form = self._read_form()
                scope = str(form.get("scope", "")).strip()
                source_name = str(form.get("source_name", "")).strip()
                if scope == "source" and source_name:
                    deleted_count = self.server.store.reset_tasks(source_name=source_name)
                    self._redirect_home(message=f"deleted {deleted_count} tasks from source {source_name}")
                    return
                deleted_count = self.server.store.reset_tasks()
                self._redirect_home(message=f"deleted {deleted_count} tasks from all sources")
                return
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

    def _read_form(self) -> dict[str, str]:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length).decode("utf-8") if content_length else ""
        parsed = parse_qs(raw, keep_blank_values=True)
        return {key: values[0] if values else "" for key, values in parsed.items()}

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

    def _redirect_home(self, message: str = "", error: str = "") -> None:
        location = "/"
        params = []
        if message:
            params.append(f"message={_quote_query(message)}")
        if error:
            params.append(f"error={_quote_query(error)}")
        if params:
            location = f"/?{'&'.join(params)}"
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        self.end_headers()

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

    def _render_home(self, message: str = "", error: str = "") -> str:
        tasks = self.server.store.list_tasks()
        sources = self.server.store.list_sources()
        rows = []
        for task in tasks:
            source_name = task.get("source_name") or "-"
            rows.append(
                "<tr>"
                f"<td>{task['id']}</td>"
                f"<td>{escape(source_name)}</td>"
                f"<td>{escape(task['title'])}</td>"
                f"<td>{escape(task['detail'])}</td>"
                f"<td>{escape(task['status'])}</td>"
                "<td>"
                "<form method='post' action='/admin/delete' onsubmit='return confirm(\"确认删除这条本地任务？\")'>"
                f"<input type='hidden' name='task_id' value='{task['id']}'>"
                "<button type='submit'>删除</button>"
                "</form>"
                "</td>"
                "</tr>"
            )
        body = "".join(rows) or "<tr><td colspan='6'>no tasks</td></tr>"
        source_options = "".join(
            f"<option value='{escape(source)}'>{escape(source)}</option>"
            for source in sources
        )
        notice_html = ""
        if message:
            notice_html += f"<div class='notice ok'>{escape(message)}</div>"
        if error:
            notice_html += f"<div class='notice error'>{escape(error)}</div>"
        return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>Codex Task Board</title>
  <style>
    body {{ font-family: Consolas, monospace; padding: 24px; }}
    .toolbar {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 16px 0 24px; }}
    .card {{ border: 1px solid #999; padding: 12px; min-width: 280px; background: #fafafa; }}
    .notice {{ padding: 10px 12px; margin: 0 0 16px; border: 1px solid #999; }}
    .notice.ok {{ background: #eef8ee; border-color: #77aa77; }}
    .notice.error {{ background: #fff1f1; border-color: #cc7777; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #999; padding: 8px; vertical-align: top; }}
    th {{ background: #f5f5f5; text-align: left; }}
    button {{ cursor: pointer; }}
    select, button {{ font: inherit; }}
    .danger {{ color: #8b0000; }}
  </style>
</head>
<body>
  <h1>Codex Task Board</h1>
  <p>A1-C1 固定为：标题 / 任务详情 / 状态</p>
  <p>API: <code>/api/tasks</code>, <code>/api/tasks/claim</code>, <code>/table.tsv</code></p>
  {notice_html}
  <div class="toolbar">
    <div class="card">
      <strong>按来源清空</strong>
      <form method="post" action="/admin/reset" onsubmit="return confirm('确认删除这个来源的所有本地任务？')">
        <input type="hidden" name="scope" value="source">
        <p>
          <select name="source_name" {'disabled' if not sources else ''}>
            <option value="">选择来源</option>
            {source_options}
          </select>
        </p>
        <button type="submit" {'disabled' if not sources else ''}>清空来源任务</button>
      </form>
    </div>
    <div class="card danger">
        <strong>整表重置</strong>
      <form method="post" action="/admin/reset" onsubmit="return confirm('确认删除本地 MySQL 中的全部任务？')">
        <input type="hidden" name="scope" value="all">
        <p>这不会直接删除在线表格，但下次 `sync` 会重新导入在线任务。</p>
        <button type="submit">清空本地任务表</button>
      </form>
    </div>
  </div>
  <table>
    <thead><tr><th>ID</th><th>来源</th><th>标题</th><th>任务详情</th><th>状态</th><th>操作</th></tr></thead>
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


def _quote_query(text: str) -> str:
    return quote(text, safe="")
