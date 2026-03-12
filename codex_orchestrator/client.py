from __future__ import annotations

import json
from typing import Any
from urllib import error, request


class TaskClient:
    def __init__(self, server_url: str, timeout_seconds: int = 30) -> None:
        self.server_url = server_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def list_tasks(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/api/tasks")
        return payload["tasks"]

    def add_task(self, title: str, detail: str) -> dict[str, Any]:
        return self._request("POST", "/api/tasks", {"title": title, "detail": detail})

    def claim(self, worker_id: str, lease_seconds: int) -> dict[str, Any] | None:
        payload = self._request(
            "POST",
            "/api/tasks/claim",
            {"worker_id": worker_id, "lease_seconds": lease_seconds},
        )
        return payload["task"]

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/tasks/{task_id}/heartbeat",
            {"worker_id": worker_id, "lease_seconds": lease_seconds},
        )

    def complete(self, task_id: int, worker_id: str, result_summary: str | None) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/tasks/{task_id}/complete",
            {"worker_id": worker_id, "result_summary": result_summary},
        )

    def release(self, task_id: int, worker_id: str, error_message: str | None) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/api/tasks/{task_id}/release",
            {"worker_id": worker_id, "error_message": error_message},
        )

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json; charset=utf-8"
        req = request.Request(f"{self.server_url}{path}", data=data, method=method, headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{method} {path} failed: {exc.code} {body}") from exc
