from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class StackProcessSpec:
    name: str
    command: list[str]


def run_stack(config_path: str | Path) -> None:
    specs = build_stack_process_specs(config_path)
    processes: dict[str, subprocess.Popen[str]] = {}
    try:
        for spec in specs:
            print(f"[stack] starting {spec.name}")
            processes[spec.name] = subprocess.Popen(spec.command, text=True)

        while True:
            time.sleep(2)
            for spec in specs:
                process = processes[spec.name]
                exit_code = process.poll()
                if exit_code is None:
                    continue
                print(f"[stack] {spec.name} exited with code {exit_code}, restarting")
                processes[spec.name] = subprocess.Popen(spec.command, text=True)
    except KeyboardInterrupt:
        pass
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.terminate()
        for process in processes.values():
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()


def print_stack(config_path: str | Path) -> list[StackProcessSpec]:
    specs = build_stack_process_specs(config_path)
    for spec in specs:
        print(json.dumps({"name": spec.name, "command": spec.command}, ensure_ascii=False, indent=2))
    return specs


def build_stack_process_specs(config_path: str | Path) -> list[StackProcessSpec]:
    config_file = Path(config_path).resolve()
    config = load_stack_config(config_file)
    base_dir = config_file.parent

    database_url = _required_text(config, "database_url")
    serve_options = _dict(config.get("serve"), "serve")
    sync_options = _dict(config.get("sync"), "sync")
    pool_options = _dict(config.get("pool"), "pool")

    host = str(serve_options.get("host", "127.0.0.1")).strip() or "127.0.0.1"
    port = int(serve_options.get("port", 8000))
    server_url = str(pool_options.get("server_url", f"http://{host}:{port}")).strip()
    sync_config_path = _resolve_path(base_dir, _required_text(sync_options, "config"))

    serve_command = [
        sys.executable,
        "-m",
        "codex_orchestrator",
        "serve",
        "--host",
        host,
        "--port",
        str(port),
        "--db",
        database_url,
    ]

    sync_command = [
        sys.executable,
        "-m",
        "codex_orchestrator",
        "sync",
        "loop",
        "--db",
        database_url,
        "--config",
        str(sync_config_path),
        "--interval-seconds",
        str(int(sync_options.get("interval_seconds", 15))),
    ]
    proxy_url = str(sync_options.get("proxy_url", "")).strip()
    if proxy_url:
        sync_command.extend(["--proxy-url", proxy_url])
    if bool(sync_options.get("disable_auto_proxy", False)):
        sync_command.append("--disable-auto-proxy")

    pool_command = [
        sys.executable,
        "-m",
        "codex_orchestrator",
        "pool",
        "--server-url",
        server_url,
        "--workers",
        str(int(pool_options.get("workers", 2))),
        "--template-dir",
        str(_resolve_path(base_dir, _required_text(pool_options, "template_dir"))),
        "--runtime-dir",
        str(_resolve_path(base_dir, str(pool_options.get("runtime_dir", ".codex-runtime")).strip() or ".codex-runtime")),
        "--agent-type",
        str(pool_options.get("agent_type", "codex")).strip() or "codex",
        "--server-timeout-seconds",
        str(int(pool_options.get("server_timeout_seconds", 10))),
        "--workspace-cleanup",
        str(pool_options.get("workspace_cleanup", "after-sync-back")).strip() or "after-sync-back",
        "--workspace-sync-back",
        str(pool_options.get("workspace_sync_back", "on-success")).strip() or "on-success",
    ]

    results_dir = str(pool_options.get("results_dir", "")).strip()
    if results_dir:
        pool_command.extend(["--results-dir", str(_resolve_path(base_dir, results_dir))])
    agent_bin = str(pool_options.get("agent_bin", "")).strip()
    if agent_bin:
        pool_command.extend(["--agent-bin", agent_bin])
    agent_model = str(pool_options.get("agent_model", "")).strip()
    if agent_model:
        pool_command.extend(["--agent-model", agent_model])
    agent_timeout = pool_options.get("agent_timeout_seconds")
    if agent_timeout is not None:
        pool_command.extend(["--agent-timeout-seconds", str(int(agent_timeout))])
    agent_template = str(pool_options.get("agent_command_template", "")).strip()
    if agent_template:
        pool_command.extend(["--agent-command-template", agent_template])
    if "agent_use_stdin" in pool_options:
        if bool(pool_options.get("agent_use_stdin")):
            pool_command.append("--agent-use-stdin")
        else:
            pool_command.append("--agent-no-stdin")
    for value in _string_list(pool_options.get("agent_args", []), "pool.agent_args"):
        pool_command.extend(["--agent-arg", value])
    codex_bin = str(pool_options.get("codex_bin", "")).strip()
    if codex_bin:
        pool_command.extend(["--codex-bin", codex_bin])
    codex_model = str(pool_options.get("codex_model", "")).strip()
    if codex_model:
        pool_command.extend(["--codex-model", codex_model])
    codex_timeout = pool_options.get("codex_timeout_seconds")
    if codex_timeout is not None:
        pool_command.extend(["--codex-timeout-seconds", str(int(codex_timeout))])
    lease_seconds = pool_options.get("lease_seconds")
    if lease_seconds is not None:
        pool_command.extend(["--lease-seconds", str(int(lease_seconds))])
    poll_interval = pool_options.get("poll_interval")
    if poll_interval is not None:
        pool_command.extend(["--poll-interval", str(int(poll_interval))])
    pool_proxy_url = str(pool_options.get("proxy_url", "")).strip()
    if pool_proxy_url:
        pool_command.extend(["--proxy-url", pool_proxy_url])
    if bool(pool_options.get("disable_auto_proxy", False)):
        pool_command.append("--disable-auto-proxy")
    for value in _string_list(pool_options.get("codex_args", []), "pool.codex_args"):
        pool_command.extend(["--codex-arg", value])

    return [
        StackProcessSpec(name="serve", command=serve_command),
        StackProcessSpec(name="sync", command=sync_command),
        StackProcessSpec(name="pool", command=pool_command),
    ]


def load_stack_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    raw = config_path.read_text(encoding="utf-8")
    return _dict(json.loads(_replace_env_tokens(raw)), "root")


def _replace_env_tokens(text: str) -> str:
    def repl(match: Any) -> str:
        return os.environ.get(match.group(1), "")

    import re

    return re.sub(r"\$\{([A-Z0-9_]+)\}", repl, text)


def _resolve_path(base_dir: Path, value: str) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (base_dir / candidate).resolve()


def _required_text(payload: dict[str, Any], key: str) -> str:
    value = str(payload.get(key, "")).strip()
    if not value:
        raise ValueError(f"{key} is required in stack config")
    return value


def _dict(value: Any, label: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def _string_list(value: Any, label: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{label} must be a string array")
    return [item for item in value if item.strip()]
