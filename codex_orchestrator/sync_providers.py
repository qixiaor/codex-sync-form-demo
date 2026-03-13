from __future__ import annotations

import json
import os
import re
import asyncio
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
    if config.provider == "dingtalk-base":
        return DingTalkBaseProvider(config)
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
        self.status_aliases = _build_status_aliases(config.options)

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
            status = _normalize_status(str(values[2]).strip(), self.status_aliases)
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


class DingTalkBaseProvider(SyncProvider):
    def __init__(self, config: ProviderConfig) -> None:
        super().__init__(config)
        options = config.options
        self.mcp_url = _required(options, "mcp_url")
        self.dentry_uuid = _required(options, "dentry_uuid")
        self.sheet_id_or_name = str(options.get("sheet_id_or_name", "Sheet1")).strip() or "Sheet1"
        self.title_field = str(options.get("title_field", "标题")).strip() or "标题"
        self.detail_field = str(options.get("detail_field", "任务详情")).strip() or "任务详情"
        self.status_field = str(options.get("status_field", "状态")).strip() or "状态"
        self.timeout_seconds = int(options.get("timeout_seconds", 30))
        self.write_enabled = _as_bool(options.get("write_enabled", True))
        self.status_aliases = _build_status_aliases(options)
        self.search_tool_name = str(options.get("search_tool_name", "search_base_record")).strip() or "search_base_record"
        self.update_tool_name = str(options.get("update_tool_name", "update_records")).strip() or "update_records"
        self.record_fields_key = str(options.get("record_fields_key", "fields")).strip() or "fields"
        self.record_id_key = str(options.get("record_id_key", "id")).strip() or "id"
        self.record_id_arg_name = str(options.get("record_id_arg_name", "recordId")).strip() or "recordId"
        self.fields_arg_name = str(options.get("fields_arg_name", "fields")).strip() or "fields"
        self.update_records_arg_name = str(options.get("update_records_arg_name", "recordIds")).strip() or "recordIds"

    @property
    def can_write(self) -> bool:
        return self.write_enabled

    def list_tasks(self) -> list[SourceTask]:
        cursor = ""
        tasks: list[SourceTask] = []
        while True:
            arguments = {
                "dentryUuid": self.dentry_uuid,
                "sheetIdOrName": self.sheet_id_or_name,
            }
            if cursor:
                arguments["cursor"] = cursor
            payload = _call_mcp_tool_with_fallbacks(
                server_url=self.mcp_url,
                tool_names=_tool_aliases(self.search_tool_name),
                arguments=arguments,
                timeout_seconds=self.timeout_seconds,
            )
            result = _unwrap_dingtalk_mcp_payload(payload)
            records = result.get("records", [])
            if not isinstance(records, list):
                raise ValueError("dingtalk-base search result must contain a records list")
            for record in records:
                if not isinstance(record, dict):
                    continue
                record_id = str(record.get("id", "")).strip()
                if not record_id:
                    continue
                fields = record.get(self.record_fields_key, record.get("fields", {}))
                if not isinstance(fields, dict):
                    continue
                title = str(fields.get(self.title_field, "")).strip()
                detail = str(fields.get(self.detail_field, "")).strip()
                status = _normalize_status(str(fields.get(self.status_field, "")).strip(), self.status_aliases)
                if not title and not detail and not status:
                    continue
                if status not in VALID_STATUSES:
                    continue
                tasks.append(
                    SourceTask(
                        source_task_key=record_id,
                        title=title,
                        detail=detail,
                        status=status,
                    )
                )
            has_more = bool(result.get("hasMore"))
            cursor = str(result.get("cursor", "") or "").strip()
            if not has_more:
                break
        return tasks

    def update_status(self, source_task_key: str, status: str) -> None:
        if status not in VALID_STATUSES:
            raise ValueError(f"invalid status: {status}")
        arguments = self._build_update_arguments(source_task_key, status)
        payload = _call_mcp_tool_with_fallbacks(
            server_url=self.mcp_url,
            tool_names=_tool_aliases(self.update_tool_name),
            arguments=arguments,
            timeout_seconds=self.timeout_seconds,
        )
        _unwrap_dingtalk_mcp_payload(payload)

    def _build_update_arguments(self, source_task_key: str, status: str) -> dict[str, Any]:
        base_arguments: dict[str, Any] = {
            "dentryUuid": self.dentry_uuid,
            "sheetIdOrName": self.sheet_id_or_name,
        }
        fields = {self.status_field: status}
        if self.update_tool_name == "update_records":
            base_arguments[self.update_records_arg_name] = [
                {
                    self.record_id_key: source_task_key,
                    self.fields_arg_name: fields,
                }
            ]
            return base_arguments

        base_arguments[self.record_id_arg_name] = source_task_key
        base_arguments[self.fields_arg_name] = fields
        return base_arguments


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
        self.status_aliases = _build_status_aliases(options)

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
            status = _normalize_status(str(item.get(self.status_field, "")).strip(), self.status_aliases)
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


def _build_status_aliases(options: dict[str, Any]) -> dict[str, str]:
    aliases = {
        "未完成": STATUS_PENDING,
        "待开始": STATUS_PENDING,
        "进行中": "执行中",
        "处理中": "执行中",
        "完成": "已完成",
    }
    raw = options.get("status_aliases", {})
    if isinstance(raw, dict):
        for key, value in raw.items():
            source = str(key).strip()
            target = str(value).strip()
            if source and target:
                aliases[source] = target
    return aliases


def _normalize_status(status: str, aliases: dict[str, str]) -> str:
    status = status.strip()
    if not status:
        return STATUS_PENDING
    return aliases.get(status, status)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"0", "false", "no", "off", ""}:
        return False
    return True


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


def _unwrap_dingtalk_mcp_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        raise ValueError("dingtalk-base MCP response must be a JSON object")
    success = payload.get("success")
    if success is False:
        raise RuntimeError(
            f"dingtalk-base MCP tool failed: errorCode={payload.get('errorCode')} "
            f"errorMsg={payload.get('errorMsg')}"
        )
    result = payload.get("result")
    if result is None:
        raise ValueError(f"dingtalk-base MCP response must contain a result field: {json.dumps(payload, ensure_ascii=False)}")
    return result


def _call_mcp_tool(
    server_url: str,
    tool_name: str,
    arguments: dict[str, Any],
    timeout_seconds: int,
) -> dict[str, Any]:
    return asyncio.run(
        asyncio.wait_for(
            _call_mcp_tool_async(
                server_url=server_url,
                tool_name=tool_name,
                arguments=arguments,
            ),
            timeout=timeout_seconds,
        )
    )


def _call_mcp_tool_with_fallbacks(
    server_url: str,
    tool_names: list[str],
    arguments: dict[str, Any],
    timeout_seconds: int,
) -> dict[str, Any]:
    return asyncio.run(
        asyncio.wait_for(
            _call_mcp_tool_with_fallbacks_async(
                server_url=server_url,
                tool_names=tool_names,
                arguments=arguments,
            ),
            timeout=timeout_seconds,
        )
    )


async def _call_mcp_tool_async(
    server_url: str,
    tool_name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    ClientSession, streamable_http_client = _load_mcp_client()
    async with streamable_http_client(server_url) as (read_stream, write_stream, _):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, arguments=arguments)
            return _parse_mcp_call_result(result)


async def _call_mcp_tool_with_fallbacks_async(
    server_url: str,
    tool_names: list[str],
    arguments: dict[str, Any],
) -> dict[str, Any]:
    ClientSession, streamable_http_client = _load_mcp_client()
    async with streamable_http_client(server_url) as (read_stream, write_stream, _):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            available_tools = await _list_mcp_tool_names(session)
            names_to_try = _prioritize_available_tools(tool_names, available_tools)
            last_error: Exception | None = None
            for tool_name in names_to_try:
                try:
                    result = await session.call_tool(tool_name, arguments=arguments)
                    return _parse_mcp_call_result(result)
                except Exception as exc:
                    last_error = exc
            available_text = ", ".join(sorted(available_tools)) if available_tools else "(server did not return any tools)"
            wanted_text = ", ".join(tool_names)
            if last_error is None:
                raise RuntimeError(
                    f"MCP tool not available. wanted=[{wanted_text}] available=[{available_text}]"
                )
            raise RuntimeError(
                f"MCP tool call failed. wanted=[{wanted_text}] available=[{available_text}] last_error={last_error}"
            ) from last_error


def _load_mcp_client() -> tuple[Any, Any]:
    try:
        from mcp import ClientSession
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "mcp is required for dingtalk-base provider. "
            "Install it with: python -m pip install mcp"
        ) from exc

    try:
        from mcp.client.streamable_http import streamable_http_client
    except ImportError:
        try:
            from mcp.client.streamablehttp_client import streamablehttp_client as streamable_http_client
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "mcp streamable HTTP client is unavailable. "
                "Upgrade MCP SDK with: python -m pip install -U mcp"
            ) from exc
    return ClientSession, streamable_http_client


def _parse_mcp_call_result(result: Any) -> dict[str, Any]:
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict):
        return structured

    for content in getattr(result, "content", []):
        text = getattr(content, "text", None)
        if isinstance(text, str) and text.strip():
            parsed = _try_parse_json(text)
            if isinstance(parsed, dict):
                return parsed

    raise ValueError("MCP tool did not return JSON content")


def _try_parse_json(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"failed to parse MCP JSON response: {exc}") from exc


async def _list_mcp_tool_names(session: Any) -> set[str]:
    listing = await session.list_tools()
    tools = getattr(listing, "tools", None)
    if not isinstance(tools, list):
        return set()
    names: set[str] = set()
    for tool in tools:
        name = getattr(tool, "name", None)
        if isinstance(name, str) and name.strip():
            names.add(name.strip())
    return names


def _tool_aliases(tool_name: str) -> list[str]:
    tool_name = tool_name.strip()
    aliases = [tool_name]
    if tool_name == "search_base_record":
        aliases.append("search_base_records")
    elif tool_name == "search_base_records":
        aliases.append("search_base_record")
    elif tool_name == "update_base_records":
        aliases.append("update_base_record")
    elif tool_name == "update_base_record":
        aliases.append("update_base_records")
        aliases.append("update_records")
    elif tool_name == "update_records":
        aliases.append("update_base_record")
        aliases.append("update_base_records")
    return aliases


def _prioritize_available_tools(tool_names: list[str], available_tools: set[str]) -> list[str]:
    present = [tool_name for tool_name in tool_names if tool_name in available_tools]
    missing = [tool_name for tool_name in tool_names if tool_name not in available_tools]
    ordered = present + missing
    deduped: list[str] = []
    for tool_name in ordered:
        if tool_name not in deduped:
            deduped.append(tool_name)
    return deduped
