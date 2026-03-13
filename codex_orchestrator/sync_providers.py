from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request

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
        self.spreadsheet_id = _resolve_spreadsheet_id(config.options)
        self.sheet_name = str(config.options.get("sheet_name", "Sheet1"))
        self.header_row = int(config.options.get("header_row", 1))
        self.api_key = _resolve_api_key(config.options)
        self._direct_access_token = _resolve_direct_access_token(config.options)
        self._service_account_credentials = _resolve_service_account_credentials(config.options)
        self._google_request = None
        self.timeout_seconds = int(config.options.get("timeout_seconds", 30))
        self.read_range = str(config.options.get("read_range", f"{_quote_sheet_name(self.sheet_name)}!A:C"))
        self.status_column = str(config.options.get("status_column", "C"))

    @property
    def can_write(self) -> bool:
        return bool(self._direct_access_token or self._service_account_credentials)

    @property
    def access_token(self) -> str:
        return self._get_access_token()

    def list_tasks(self) -> list[SourceTask]:
        params = {}
        if self.api_key:
            params["key"] = self.api_key
        payload = self._request_json(
            "GET",
            f"https://sheets.googleapis.com/v4/spreadsheets/{self.spreadsheet_id}/values/{parse.quote(self.read_range, safe='')}",
            params=params,
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
        if not self.can_write:
            raise RuntimeError("google-sheets writeback requires access_token or service_account_file")
        row_number = int(source_task_key)
        cell_range = f"{_quote_sheet_name(self.sheet_name)}!{self.status_column}{row_number}:{self.status_column}{row_number}"
        body = {"range": cell_range, "majorDimension": "ROWS", "values": [[status]]}
        self._request_json(
            "PUT",
            f"https://sheets.googleapis.com/v4/spreadsheets/{self.spreadsheet_id}/values/{parse.quote(cell_range, safe='')}?valueInputOption=RAW",
            body,
        )

    def _request_json(
        self,
        method: str,
        url: str,
        body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers: dict[str, str] = {}
        access_token = self._get_access_token()
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json; charset=utf-8"
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        if params:
            query = parse.urlencode(params)
            joiner = "&" if "?" in url else "?"
            url = f"{url}{joiner}{query}"
        req = request.Request(url, data=data, method=method, headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"google-sheets {method} {url} failed: {exc.code} {body}") from exc

    def _get_access_token(self) -> str:
        if self._direct_access_token:
            return self._direct_access_token
        if not self._service_account_credentials:
            return ""

        credentials = self._service_account_credentials
        if not credentials.valid or not credentials.token:
            if self._google_request is None:
                self._google_request = _new_google_request()
            credentials.refresh(self._google_request)
        token = credentials.token or ""
        if not token:
            raise RuntimeError("failed to obtain access token from service account")
        return token


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
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw.strip() else {}
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"generic-json {method} {url} failed: {exc.code} {body}") from exc


def _required(payload: dict[str, Any], key: str) -> str:
    value = str(payload.get(key, "")).strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _resolve_direct_access_token(options: dict[str, Any]) -> str:
    direct = str(options.get("access_token", "")).strip()
    if direct:
        return direct
    env_name = str(options.get("access_token_env", "GOOGLE_ACCESS_TOKEN")).strip()
    value = os.environ.get(env_name, "").strip()
    if value:
        return value
    return ""


def _resolve_service_account_credentials(options: dict[str, Any]) -> Any | None:
    service_account_file = str(options.get("service_account_file", "")).strip()
    if not service_account_file:
        return None
    return _build_service_account_credentials(
        service_account_file=service_account_file,
        scopes=options.get(
            "scopes",
            ["https://www.googleapis.com/auth/spreadsheets"],
        ),
        subject=str(options.get("service_account_subject", "")).strip() or None,
    )


def _resolve_api_key(options: dict[str, Any]) -> str:
    direct = str(options.get("api_key", "")).strip()
    if direct:
        return direct
    env_name = str(options.get("api_key_env", "GOOGLE_API_KEY")).strip()
    value = os.environ.get(env_name, "").strip()
    if value:
        return value
    return ""


def _quote_sheet_name(sheet_name: str) -> str:
    escaped = sheet_name.replace("'", "''")
    return f"'{escaped}'"


def _resolve_spreadsheet_id(options: dict[str, Any]) -> str:
    spreadsheet_id = str(options.get("spreadsheet_id", "")).strip()
    if spreadsheet_id:
        return spreadsheet_id
    spreadsheet_url = str(options.get("spreadsheet_url", "")).strip()
    if spreadsheet_url:
        match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", spreadsheet_url)
        if match:
            return match.group(1)
        raise ValueError("cannot extract spreadsheet_id from spreadsheet_url")
    raise ValueError("google-sheets provider requires spreadsheet_id or spreadsheet_url")


_SERVICE_ACCOUNT_CREDENTIALS_CACHE: dict[tuple[str, tuple[str, ...], str | None], Any] = {}


def _build_service_account_credentials(
    service_account_file: str,
    scopes: list[str] | tuple[str, ...] | Any,
    subject: str | None,
) -> Any:
    try:
        from google.oauth2 import service_account
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "google-auth is required for service_account_file support. "
            "Install it with: python -m pip install google-auth"
        ) from exc

    cache_key = (os.path.abspath(service_account_file), tuple(scopes), subject)
    credentials = _SERVICE_ACCOUNT_CREDENTIALS_CACHE.get(cache_key)
    if credentials is not None:
        return credentials

    credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=list(scopes))
    if subject:
        credentials = credentials.with_subject(subject)
    _SERVICE_ACCOUNT_CREDENTIALS_CACHE[cache_key] = credentials
    return credentials


def _new_google_request() -> Any:
    try:
        from google.auth.transport.requests import Request
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "google-auth is required for service_account_file support. "
            "Install it with: python -m pip install google-auth"
        ) from exc
    return Request()


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
