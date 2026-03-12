from __future__ import annotations

import json
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .client import TaskClient


@dataclass
class WorkerConfig:
    server_url: str
    worker_id: str
    template_dir: Path
    runtime_dir: Path
    codex_bin: str = "codex"
    codex_model: str | None = None
    lease_seconds: int = 180
    poll_interval: int = 5
    codex_timeout_seconds: int = 900
    codex_extra_args: list[str] = field(default_factory=list)


def run_worker(config: WorkerConfig) -> None:
    client = TaskClient(config.server_url)
    config.runtime_dir.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            task = client.claim(config.worker_id, config.lease_seconds)
        except Exception as exc:
            print(f"[{config.worker_id}] claim failed: {exc}", file=sys.stderr)
            time.sleep(config.poll_interval)
            continue
        if task is None:
            time.sleep(config.poll_interval)
            continue
        process_task(client, config, task)


def process_task(client: TaskClient, config: WorkerConfig, task: dict[str, object]) -> None:
    task_id = int(task["id"])
    print(f"[{config.worker_id}] claimed task {task_id}: {task['title']}")
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = config.runtime_dir / f"task-{task_id}-{run_stamp}"
    workspace_dir = run_dir / "workspace"
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    copy_template(config.template_dir, workspace_dir, config.runtime_dir)

    stop_heartbeat = threading.Event()
    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(client, task_id, config.worker_id, config.lease_seconds, stop_heartbeat),
        daemon=True,
    )
    heartbeat_thread.start()

    try:
        try:
            print(f"[{config.worker_id}] starting codex for task {task_id}")
            result = run_codex(config, task, workspace_dir, logs_dir)
        except Exception as exc:
            client.release(task_id, config.worker_id, f"worker execution failed: {exc}")
            print(f"[{config.worker_id}] task {task_id} failed before codex completed: {exc}", file=sys.stderr)
            return
        if result["returncode"] == 0:
            client.complete(task_id, config.worker_id, result["summary"])
            print(f"[{config.worker_id}] completed task {task_id}")
        else:
            client.release(task_id, config.worker_id, result["summary"])
            print(f"[{config.worker_id}] released task {task_id} after codex exit {result['returncode']}", file=sys.stderr)
    finally:
        stop_heartbeat.set()
        heartbeat_thread.join(timeout=5)


def copy_template(template_dir: Path, workspace_dir: Path, runtime_dir: Path) -> None:
    ignore_names = {
        ".git",
        ".venv",
        "__pycache__",
        ".pytest_cache",
    }
    ignore_names.add(runtime_dir.name)
    try:
        runtime_relative = runtime_dir.resolve().relative_to(template_dir.resolve())
        if runtime_relative.parts:
            ignore_names.add(runtime_relative.parts[0])
    except ValueError:
        pass

    def _ignore(_: str, names: list[str]) -> set[str]:
        return {name for name in names if name in ignore_names}

    shutil.copytree(template_dir, workspace_dir, ignore=_ignore)


def build_prompt(task: dict[str, object]) -> str:
    title = str(task["title"]).strip()
    detail = str(task["detail"]).strip()
    return (
        "你现在是在一个全新的 Codex CLI 会话中执行单个任务。\n"
        "要求：\n"
        "1. 先检查当前工作目录内容，再开始修改。\n"
        "2. 直接完成任务，不要只给方案。\n"
        "3. 如有代码改动，请自行验证能否运行或测试；如果无法验证，要明确说明原因。\n"
        "4. 最终输出一段简短总结，说明改了什么、如何验证、还有什么风险。\n\n"
        f"任务标题：{title}\n"
        f"任务详情：{detail}\n"
    )


def resolve_codex_launcher(codex_bin: str) -> list[str]:
    candidate = Path(codex_bin)
    resolved = None
    if candidate.is_file():
        resolved = str(candidate)
    else:
        resolved = shutil.which(codex_bin)
    if not resolved:
        raise FileNotFoundError(
            f"cannot find Codex executable '{codex_bin}'. "
            "On Windows, pass --codex-bin codex.cmd or an absolute path to codex.exe/codex.cmd."
        )

    resolved_path = Path(resolved)
    if resolved_path.suffix.lower() == ".ps1":
        return [
            "powershell",
            "-NoLogo",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(resolved_path),
        ]
    return [str(resolved_path)]


def run_codex(
    config: WorkerConfig,
    task: dict[str, object],
    workspace_dir: Path,
    logs_dir: Path,
) -> dict[str, object]:
    prompt = build_prompt(task)
    final_message_path = logs_dir / "final_message.txt"
    stdout_path = logs_dir / "stdout.txt"
    stderr_path = logs_dir / "stderr.txt"
    metadata_path = logs_dir / "metadata.json"

    command = [
        *resolve_codex_launcher(config.codex_bin),
        "-a",
        "never",
        "exec",
        "--skip-git-repo-check",
        "-C",
        str(workspace_dir),
        "-o",
        str(final_message_path),
        "-s",
        "danger-full-access",
    ]
    if config.codex_model:
        command.extend(["-m", config.codex_model])
    command.extend(config.codex_extra_args)
    command.append("-")

    process = subprocess.run(
        command,
        input=prompt,
        text=True,
        capture_output=True,
        check=False,
        timeout=config.codex_timeout_seconds,
    )
    stdout_path.write_text(process.stdout, encoding="utf-8")
    stderr_path.write_text(process.stderr, encoding="utf-8")

    summary = None
    if final_message_path.exists():
        summary = final_message_path.read_text(encoding="utf-8").strip()
    if not summary:
        summary = process.stderr.strip() or process.stdout.strip() or f"codex exit code {process.returncode}"
    metadata_path.write_text(
        json.dumps(
            {
                "command": command,
                "returncode": process.returncode,
                "workspace_dir": str(workspace_dir),
                "task_id": task["id"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {"returncode": process.returncode, "summary": summary}


def _heartbeat_loop(
    client: TaskClient,
    task_id: int,
    worker_id: str,
    lease_seconds: int,
    stop_event: threading.Event,
) -> None:
    interval = max(5, lease_seconds // 3)
    while not stop_event.wait(interval):
        try:
            client.heartbeat(task_id, worker_id, lease_seconds)
        except Exception as exc:  # pragma: no cover
            print(f"[{worker_id}] heartbeat failed for task {task_id}: {exc}", file=sys.stderr)
