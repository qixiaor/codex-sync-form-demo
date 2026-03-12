from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any
from urllib import parse, request

from . import STATUS_PENDING, VALID_STATUSES


@dataclass
class SourceTask:
    source_task_key: str
    title: str
    detail: str
    status: str


@dataclass
class ProviderConfig:
    provider: str
    name: str
    options: dict[str, Any]


def load_provider_config(path: str) -> ProviderConfig:
    raw = json.loads(_replace_env_tokens(open(path, "r", encoding="utf-8").read()))
    provider = str(raw.get("provider", "")).strip()
    name = str(raw.get("name", provider)).strip()
    if not provider or not name:
        raise ValueError("provider and name are required in sync config")
    return ProviderConfig(provider=provider, name=name, options=raw)


def create_provider(config: ProviderConfig) -> "SyncProvider":
    if config.provider == "google-sheets":
        return GoogleSheetsProvider(config)
    if config.provider == "generic-json":
        return GenericJsonProvider(config)
    raise ValueError(f"unsupported provider: {config.provider}")


class SyncProvider:
    def __init__(self, config: ProviderConfig) -> None:
        self.config = config
        self.name = config.name

    @property
    def can_write(self) -> bool:
        return True

    def list_tasks(self) -> list[SourceTask]:
        raise NotImplementedError

    def update_status(self, source_task_key: str, status: str) -> None:
        raise NotImplementedError


class GoogleSheetsProvider(SyncProvider):
    def __init__(self, config: ProviderConfig) -> None:
        super().__init__(config)
        self.spreadsheet_id = _required(config.options, "spreadsheet_id")
        self.sheet_name = str(config.options.get("sheet_name", "Sheet1"))
        self.header_row = int(config.options.get("header_row", 1))
        self.access_token = _resolve_access_token(config.options)
        self.timeout_seconds = int(config.options.get("timeout_seconds", 30))
        self.read_range = str(config.options.get("read_range", f"{_quote_sheet_name(self.sheet_name)}!A:C"))
        self.status_column = str(config.options.get("status_column", "C"))

    def list_tasks(self) -> list[SourceTask]:
        payload = self._request_json(
            "GET",
            f"https://sheets.googleapis.com/v4/spreadsheets/{self.spreadsheet_id}/values/{parse.quote(self.read_range, safe='')}",
        )
        rows = payload.get("values", [])
        tasks: list[SourceTask] = []
        for row_number, row in enumerate(rows[self.header_row :], start=self.header_row + 1):
            values = list(row) + ["", "", ""]
            title = str(values[0]).strip()
            detail = str(values[1]).strip()
            status = str(values[2]).strip() or STATUS_PENDING
            if not title and not detail and not status:
                continue
            if status not in VALID_STATUSES:
                continue
            tasks.append(SourceTask(source_task_key=str(row_number), title=title, detail=detail, status=status))
        return tasks

    def update_status(self, source_task_key: str, status: str) -> None:
        if status not in VALID_STATUSES:
            raise ValueError(f"invalid status: {status}")
        row_number = int(source_task_key)
        cell_range = f"{_quote_sheet_name(self.sheet_name)}!{self.status_column}{row_number}:{self.status_column}{row_number}"
        body = {"range": cell_range, "majorDimension": "ROWS", "values": [[status]]}
        self._request_json(
            "PUT",
            f"https://sheets.googleapis.com/v4/spreadsheets/{self.spreadsheet_id}/values/{parse.quote(cell_range, safe='')}?valueInputOption=RAW",
            body,
        )

    def _request_json(self, method: str, url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json; charset=utf-8"
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = request.Request(url, data=data, method=method, headers=headers)
        with request.urlopen(req, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))


class GenericJsonProvider(SyncProvider):
    def __init__(self, config: ProviderConfig) -> None:
        super().__init__(config)
        options = config.options
        self.read_url = _required(options, "read_url")
        self.write_url_template = str(options.get("write_url_template", "")).strip()
        self.timeout_seconds = int(options.get("timeout_seconds", 30))
        self.method = str(options.get("write_method", "PATCH")).upper()
        self.list_field = str(options.get("list_field", "")).strip()
        self.task_key_field = str(options.get("task_key_field", "id"))
        self.title_field = str(options.get("title_field", "title"))
        self.detail_field = str(options.get("detail_field", "detail"))
        self.status_field = str(options.get("status_field", "status"))
        self.headers = {str(key): str(value) for key, value in options.get("headers", {}).items()}

    @property
    def can_write(self) -> bool:
        return bool(self.write_url_template)

    def list_tasks(self) -> list[SourceTask]:
        payload = self._request_json("GET", self.read_url)
        items = _extract_path(payload, self.list_field) if self.list_field else payload
        if not isinstance(items, list):
            raise ValueError("generic-json list_field must resolve to a list")
        tasks: list[SourceTask] = []
        for item in items:
            title = str(item.get(self.title_field, "")).strip()
            detail = str(item.get(self.detail_field, "")).strip()
            status = str(item.get(self.status_field, "")).strip() or STATUS_PENDING
            if status not in VALID_STATUSES:
                continue
            tasks.append(
                SourceTask(
                    source_task_key=str(item[self.task_key_field]),
                    title=title,
                    detail=detail,
                    status=status,
                )
            )
        return tasks

    def update_status(self, source_task_key: str, status: str) -> None:
        if not self.write_url_template:
            raise RuntimeError("generic-json provider is read-only without write_url_template")
        url = self.write_url_template.replace("{task_key}", parse.quote(str(source_task_key), safe=""))
        self._request_json(self.method, url, {self.status_field: status})

    def _request_json(self, method: str, url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = dict(self.headers)
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json; charset=utf-8"
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = request.Request(url, data=data, method=method, headers=headers)
        with request.urlopen(req, timeout=self.timeout_seconds) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw.strip() else {}


def _required(payload: dict[str, Any], key: str) -> str:
    value = str(payload.get(key, "")).strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _resolve_access_token(options: dict[str, Any]) -> str:
    direct = str(options.get("access_token", "")).strip()
    if direct:
        return direct
    env_name = str(options.get("access_token_env", "GOOGLE_ACCESS_TOKEN")).strip()
    value = os.environ.get(env_name, "").strip()
    if value:
        return value
    raise ValueError("google-sheets provider requires access_token or access_token_env")


def _quote_sheet_name(sheet_name: str) -> str:
    escaped = sheet_name.replace("'", "''")
    return f"'{escaped}'"


def _extract_path(payload: Any, path: str) -> Any:
    current = payload
    for part in path.split("."):
        if not part:
            continue
        if not isinstance(current, dict):
            raise ValueError(f"cannot traverse list_field path '{path}'")
        current = current.get(part)
    return current


def _replace_env_tokens(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        return os.environ.get(match.group(1), "")

    return re.sub(r"\$\{([A-Z0-9_]+)\}", repl, text)
