from __future__ import annotations

import json
import os
import re
import shutil
import shlex
import subprocess
import sys
import threading
import time
import hashlib
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .client import TaskClient
from .network import apply_proxy_to_env


def _text_subprocess_kwargs() -> dict[str, object]:
    return {
        "text": True,
        "encoding": "utf-8",
        "errors": "replace",
    }


@dataclass
class WorkerConfig:
    server_url: str
    worker_id: str
    template_dir: Path
    runtime_dir: Path
    results_dir: Path
    agent_type: str = "codex"
    agent_bin: str | None = None
    agent_model: str | None = None
    agent_timeout_seconds: int | None = None
    agent_command_template: str | None = None
    agent_use_stdin: bool | None = None
    agent_extra_args: list[str] = field(default_factory=list)
    codex_bin: str | None = None
    codex_model: str | None = None
    lease_seconds: int = 180
    poll_interval: int = 5
    server_timeout_seconds: int = 10
    codex_timeout_seconds: int | None = None
    proxy_url: str | None = None
    auto_proxy: bool = True
    codex_extra_args: list[str] = field(default_factory=list)
    workspace_cleanup: str = "after-sync-back"
    workspace_sync_back: str = "on-success"

    def __post_init__(self) -> None:
        if self.agent_bin is None:
            self.agent_bin = self.codex_bin or "codex"
        if self.agent_model is None:
            self.agent_model = self.codex_model
        if self.agent_timeout_seconds is None:
            self.agent_timeout_seconds = self.codex_timeout_seconds or 900
        if not self.agent_extra_args and self.codex_extra_args:
            self.agent_extra_args = list(self.codex_extra_args)
        if self.agent_use_stdin is None:
            agent_name = Path(self.agent_bin or "").stem.lower()
            self.agent_use_stdin = self.agent_type == "codex" or (
                self.agent_type == "command-template" and "claude" in agent_name
            )
        if self.workspace_cleanup not in {"on-success", "always", "never", "after-sync-back"}:
            raise ValueError("workspace_cleanup must be one of: on-success, always, never, after-sync-back")
        if self.workspace_sync_back not in {"never", "on-success", "always"}:
            raise ValueError("workspace_sync_back must be one of: never, on-success, always")


def run_worker(config: WorkerConfig) -> None:
    client = TaskClient(config.server_url, timeout_seconds=config.server_timeout_seconds)
    config.runtime_dir.mkdir(parents=True, exist_ok=True)
    idle_polls = 0
    while True:
        try:
            task = client.claim(config.worker_id, config.lease_seconds)
        except Exception as exc:
            print(f"[{config.worker_id}] claim failed: {exc}", file=sys.stderr)
            time.sleep(config.poll_interval)
            continue
        if task is None:
            idle_polls += 1
            if idle_polls == 1 or idle_polls % max(1, 30 // max(1, config.poll_interval)) == 0:
                print(f"[{config.worker_id}] waiting for pending task")
            time.sleep(config.poll_interval)
            continue
        idle_polls = 0
        process_task(client, config, task)


def process_task(client: TaskClient, config: WorkerConfig, task: dict[str, object]) -> None:
    task_id = int(task["id"])
    print(f"[{config.worker_id}] claimed task {task_id}: {task['title']}")
    task_started_epoch = time.time()
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = config.runtime_dir / f"task-{task_id:04d}-{slugify(str(task['title']))}-{run_stamp}"
    workspace_dir = run_dir / "workspace"
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    copy_template(config.template_dir, workspace_dir, config.runtime_dir)
    prepare_workspace_for_agent(config, workspace_dir)

    stop_heartbeat = threading.Event()
    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(client, task_id, config.worker_id, config.lease_seconds, stop_heartbeat),
        daemon=True,
    )
    heartbeat_thread.start()
    execution_status = "released"

    try:
        try:
            print(f"[{config.worker_id}] starting {config.agent_type} for task {task_id}")
            result = run_agent(config, task, workspace_dir, logs_dir)
        except Exception as exc:
            released_task = client.release(task_id, config.worker_id, f"worker execution failed: {exc}")
            write_task_result(
                config=config,
                task=released_task or task,
                execution_status="released",
                run_dir=run_dir,
                workspace_dir=workspace_dir,
                logs_dir=logs_dir,
                result_summary=f"worker execution failed: {exc}",
                codex_returncode=None,
            )
            print(f"[{config.worker_id}] task {task_id} failed before agent completed: {exc}", file=sys.stderr)
            return
        if result["returncode"] == 0:
            execution_status = "completed"
            completed_task = client.complete(task_id, config.worker_id, result["summary"])
            write_task_result(
                config=config,
                task=completed_task or task,
                execution_status="completed",
                run_dir=run_dir,
                workspace_dir=workspace_dir,
                logs_dir=logs_dir,
                result_summary=result["summary"],
                codex_returncode=result["returncode"],
            )
            print(f"[{config.worker_id}] completed task {task_id}")
        else:
            execution_status = "released"
            released_task = client.release(task_id, config.worker_id, result["summary"])
            write_task_result(
                config=config,
                task=released_task or task,
                execution_status="released",
                run_dir=run_dir,
                workspace_dir=workspace_dir,
                logs_dir=logs_dir,
                result_summary=result["summary"],
                codex_returncode=result["returncode"],
            )
            print(f"[{config.worker_id}] released task {task_id} after agent exit {result['returncode']}", file=sys.stderr)
    finally:
        stop_heartbeat.set()
        heartbeat_thread.join(timeout=5)
        sync_back_succeeded = maybe_sync_workspace_back(
            config,
            workspace_dir,
            logs_dir,
            execution_status,
            task_started_epoch,
        )
        maybe_cleanup_workspace(config, workspace_dir, execution_status, sync_back_succeeded)


def copy_template(template_dir: Path, workspace_dir: Path, runtime_dir: Path) -> None:
    ignore_names = {
        ".git",
        ".venv",
        "__pycache__",
        ".pytest_cache",
        ".codex-runtime",
        ".claude-runtime",
    }
    ignore_names.add(runtime_dir.name)
    try:
        runtime_relative = runtime_dir.resolve().relative_to(template_dir.resolve())
        if runtime_relative.parts:
            ignore_names.add(runtime_relative.parts[0])
    except ValueError:
        pass

    def _ignore(_: str, names: list[str]) -> set[str]:
        ignored: set[str] = set()
        for name in names:
            if name in ignore_names:
                ignored.add(name)
                continue
            if name.startswith(".") and name.endswith("-runtime"):
                ignored.add(name)
                continue
            if name.endswith(".db-wal") or name.endswith(".db-shm"):
                ignored.add(name)
        return ignored

    shutil.copytree(template_dir, workspace_dir, ignore=_ignore)


def prepare_workspace_for_agent(config: WorkerConfig, workspace_dir: Path) -> None:
    marker_path = workspace_dir / ".codex_orchestrator_workspace_root"
    marker_path.write_text(str(workspace_dir), encoding="utf-8")
    if not _is_claude_agent(config.agent_bin):
        return
    git_dir = workspace_dir / ".git"
    if git_dir.exists():
        return
    try:
        process = subprocess.run(
            ["git", "init"],
            cwd=str(workspace_dir),
            capture_output=True,
            check=False,
            timeout=20,
            **_text_subprocess_kwargs(),
        )
        if process.returncode == 0:
            print(f"[{config.worker_id}] initialized isolated git root in workspace: {workspace_dir}")
    except Exception:
        return


def _is_claude_agent(agent_bin: str | None) -> bool:
    return "claude" in Path(agent_bin or "").stem.lower()


def should_cleanup_workspace(cleanup_mode: str, execution_status: str, sync_back_succeeded: bool = False) -> bool:
    if cleanup_mode == "always":
        return True
    if cleanup_mode == "on-success":
        return execution_status == "completed"
    if cleanup_mode == "after-sync-back":
        return execution_status == "completed" and sync_back_succeeded
    if cleanup_mode == "never":
        return False
    raise ValueError("workspace_cleanup must be one of: on-success, always, never, after-sync-back")


def maybe_cleanup_workspace(
    config: WorkerConfig,
    workspace_dir: Path,
    execution_status: str,
    sync_back_succeeded: bool,
) -> None:
    if not should_cleanup_workspace(config.workspace_cleanup, execution_status, sync_back_succeeded):
        return
    if not workspace_dir.exists():
        return
    try:
        shutil.rmtree(workspace_dir, ignore_errors=False)
        print(
            f"[{config.worker_id}] cleaned workspace ({config.workspace_cleanup}) after {execution_status}: {workspace_dir}"
        )
    except Exception as exc:  # pragma: no cover
        print(f"[{config.worker_id}] cleanup failed for workspace {workspace_dir}: {exc}", file=sys.stderr)


def should_sync_workspace_back(sync_mode: str, execution_status: str) -> bool:
    if sync_mode == "always":
        return True
    if sync_mode == "on-success":
        return execution_status == "completed"
    if sync_mode == "never":
        return False
    raise ValueError("workspace_sync_back must be one of: never, on-success, always")


def maybe_sync_workspace_back(
    config: WorkerConfig,
    workspace_dir: Path,
    logs_dir: Path,
    execution_status: str,
    task_started_epoch: float,
) -> bool:
    if not should_sync_workspace_back(config.workspace_sync_back, execution_status):
        return False
    if not workspace_dir.exists():
        return False
    if not config.template_dir.exists():
        print(
            f"[{config.worker_id}] sync-back skipped, template directory not found: {config.template_dir}",
            file=sys.stderr,
        )
        return False
    if not config.template_dir.is_dir():
        print(
            f"[{config.worker_id}] sync-back skipped, template path is not a directory: {config.template_dir}",
            file=sys.stderr,
        )
        return False
    try:
        stats = sync_workspace_to_template(
            workspace_dir=workspace_dir,
            template_dir=config.template_dir,
            task_started_epoch=task_started_epoch,
        )
        (logs_dir / "sync_back.json").write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            f"[{config.worker_id}] sync-back completed ({config.workspace_sync_back}): "
            f"synced={stats['synced_files']} created={stats['created_files']} "
            f"updated={stats['updated_files']} conflicts={stats['conflict_files']} "
            f"errors={stats['error_files']}"
        )
        return stats["conflict_files"] == 0 and stats["error_files"] == 0
    except Exception as exc:  # pragma: no cover
        print(f"[{config.worker_id}] sync-back failed for workspace {workspace_dir}: {exc}", file=sys.stderr)
        return False


def sync_workspace_to_template(
    workspace_dir: Path,
    template_dir: Path,
    task_started_epoch: float,
    lock_timeout_seconds: int = 60,
) -> dict[str, int]:
    lock_path = template_dir / ".codex-orchestrator-sync-back.lock"
    stats = {
        "scanned_files": 0,
        "synced_files": 0,
        "created_files": 0,
        "updated_files": 0,
        "conflict_files": 0,
        "error_files": 0,
    }
    with _exclusive_file_lock(lock_path, timeout_seconds=lock_timeout_seconds):
        for workspace_file in workspace_dir.rglob("*"):
            if not workspace_file.is_file():
                continue
            relative_path = workspace_file.relative_to(workspace_dir)
            if _should_skip_sync_back_path(relative_path):
                continue
            target_file = template_dir / relative_path
            stats["scanned_files"] += 1
            try:
                if not _file_needs_sync(workspace_file, target_file):
                    continue
                if target_file.exists() and _target_changed_after_task_start(target_file, task_started_epoch):
                    stats["conflict_files"] += 1
                    continue
                target_file.parent.mkdir(parents=True, exist_ok=True)
                target_existed = target_file.exists()
                shutil.copy2(workspace_file, target_file)
                stats["synced_files"] += 1
                if target_existed:
                    stats["updated_files"] += 1
                else:
                    stats["created_files"] += 1
            except Exception:
                stats["error_files"] += 1
    return stats


def _should_skip_sync_back_path(relative_path: Path) -> bool:
    if not relative_path.parts:
        return False
    top = relative_path.parts[0]
    skip_top_level = {
        ".git",
        ".codex-runtime",
        ".claude-runtime",
        "__pycache__",
        ".pytest_cache",
    }
    if top in skip_top_level:
        return True
    filename = relative_path.name
    if filename.endswith(".db-wal") or filename.endswith(".db-shm"):
        return True
    if filename.startswith(".codex_orchestrator_"):
        return True
    return False


def _file_needs_sync(workspace_file: Path, target_file: Path) -> bool:
    if not target_file.exists():
        return True
    if not target_file.is_file():
        return True
    workspace_stat = workspace_file.stat()
    target_stat = target_file.stat()
    if workspace_stat.st_size != target_stat.st_size:
        return True
    if workspace_stat.st_mtime_ns == target_stat.st_mtime_ns:
        return False
    return _sha256_file(workspace_file) != _sha256_file(target_file)


def _target_changed_after_task_start(target_file: Path, task_started_epoch: float) -> bool:
    try:
        return target_file.stat().st_mtime > task_started_epoch
    except FileNotFoundError:
        return False


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


@contextmanager
def _exclusive_file_lock(lock_path: Path, timeout_seconds: int = 60):
    start = time.time()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd: int | None = None
    while fd is None:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            os.write(fd, str(os.getpid()).encode("utf-8", errors="ignore"))
            break
        except FileExistsError:
            _clear_stale_lock(lock_path, stale_after_seconds=max(timeout_seconds * 2, 120))
            if time.time() - start >= timeout_seconds:
                raise TimeoutError(f"failed to acquire sync-back lock within {timeout_seconds}s: {lock_path}")
            time.sleep(0.2)
    try:
        yield
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def _clear_stale_lock(lock_path: Path, stale_after_seconds: int) -> None:
    try:
        age_seconds = time.time() - lock_path.stat().st_mtime
    except FileNotFoundError:
        return
    if age_seconds < stale_after_seconds:
        return
    try:
        lock_path.unlink()
    except FileNotFoundError:
        return


def build_prompt(task: dict[str, object], workspace_dir: Path) -> str:
    title = str(task["title"]).strip()
    detail = str(task["detail"]).strip()
    return (
        "你现在是在一个全新的智能体 CLI 会话中执行单个任务。\n"
        "要求：\n"
        "1. 直接完成任务，不要只给方案。\n"
        "2. 如有代码改动，请自行验证能否运行或测试；如果无法验证，要明确说明原因。\n"
        "3. 最终输出一段简短总结，说明改了什么、如何验证、还有什么风险。\n\n"
        f"4. 只允许访问 workspace 目录及其子目录，不要访问父目录或其他项目：{workspace_dir}\n\n"
        f"任务标题：{title}\n"
        f"任务详情：{detail}\n"
    )


def slugify(value: str) -> str:
    normalized = re.sub(r"\s+", "-", value.strip())
    normalized = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff_-]", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-_")
    return normalized[:40] or "untitled"


def write_task_result(
    config: WorkerConfig,
    task: dict[str, object],
    execution_status: str,
    run_dir: Path,
    workspace_dir: Path,
    logs_dir: Path,
    result_summary: str,
    codex_returncode: int | None,
) -> None:
    config.results_dir.mkdir(parents=True, exist_ok=True)
    task_id = int(task["id"])
    base_name = f"task-{task_id:04d}-{slugify(str(task['title']))}"
    payload = {
        "task_id": task_id,
        "title": task["title"],
        "detail": task["detail"],
        "task_status": task.get("status"),
        "execution_status": execution_status,
        "worker_id": config.worker_id,
        "agent_type": config.agent_type,
        "attempt_count": task.get("attempt_count"),
        "claimed_by": task.get("claimed_by"),
        "result_summary": result_summary,
        "agent_returncode": codex_returncode,
        "codex_returncode": codex_returncode,
        "run_dir": str(run_dir),
        "workspace_dir": str(workspace_dir),
        "logs_dir": str(logs_dir),
        "stdout_path": str(logs_dir / "stdout.txt"),
        "stderr_path": str(logs_dir / "stderr.txt"),
        "final_message_path": str(logs_dir / "final_message.txt"),
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    json_path = config.results_dir / f"{base_name}.json"
    txt_path = config.results_dir / f"{base_name}.txt"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    txt_path.write_text(
        "\n".join(
            [
                f"task_id: {task_id}",
                f"title: {task['title']}",
                f"detail: {task['detail']}",
                f"task_status: {task.get('status')}",
                f"execution_status: {execution_status}",
                f"worker_id: {config.worker_id}",
                f"agent_type: {config.agent_type}",
                f"attempt_count: {task.get('attempt_count')}",
                f"agent_returncode: {codex_returncode}",
                f"codex_returncode: {codex_returncode}",
                f"run_dir: {run_dir}",
                f"workspace_dir: {workspace_dir}",
                f"logs_dir: {logs_dir}",
                "result_summary:",
                str(result_summary).strip(),
                "",
            ]
        ),
        encoding="utf-8",
    )


def build_codex_env(config: WorkerConfig) -> dict[str, str]:
    env = os.environ.copy()
    apply_proxy_to_env(env, proxy_url=config.proxy_url, auto_proxy=config.auto_proxy)
    return env


def resolve_agent_launcher(agent_bin: str) -> list[str]:
    candidate = Path(agent_bin)
    resolved = None
    if candidate.is_file():
        resolved = str(candidate)
    else:
        resolved = shutil.which(agent_bin)
    if not resolved:
        raise FileNotFoundError(
            f"cannot find agent executable '{agent_bin}'. "
            "On Windows, pass --agent-bin codex.cmd/claude.cmd or an absolute path."
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


def resolve_codex_launcher(codex_bin: str) -> list[str]:
    return resolve_agent_launcher(codex_bin)


def run_agent(
    config: WorkerConfig,
    task: dict[str, object],
    workspace_dir: Path,
    logs_dir: Path,
) -> dict[str, object]:
    prompt = build_prompt(task, workspace_dir)
    prompt_path = logs_dir / "prompt.txt"
    final_message_path = logs_dir / "final_message.txt"
    stdout_path = logs_dir / "stdout.txt"
    stderr_path = logs_dir / "stderr.txt"
    metadata_path = logs_dir / "metadata.json"
    prompt_path.write_text(prompt, encoding="utf-8")
    command = build_agent_command(config, task, workspace_dir, final_message_path, prompt_path, prompt)
    env = build_codex_env(config)
    proxy_url = env.get("HTTPS_PROXY")
    if proxy_url:
        print(f"[{config.worker_id}] agent proxy enabled: {proxy_url}")

    process = subprocess.run(
        command,
        input=prompt if config.agent_use_stdin else None,
        capture_output=True,
        check=False,
        timeout=config.agent_timeout_seconds,
        env=env,
        cwd=str(workspace_dir),
        **_text_subprocess_kwargs(),
    )
    stdout_path.write_text(process.stdout, encoding="utf-8")
    stderr_path.write_text(process.stderr, encoding="utf-8")

    summary = None
    if final_message_path.exists():
        summary = final_message_path.read_text(encoding="utf-8").strip()
    if not summary:
        summary = process.stderr.strip() or process.stdout.strip() or f"{config.agent_type} exit code {process.returncode}"
    metadata_path.write_text(
        json.dumps(
            {
                "agent_type": config.agent_type,
                "agent_bin": config.agent_bin,
                "command": command,
                "returncode": process.returncode,
                "workspace_dir": str(workspace_dir),
                "task_id": task["id"],
                "prompt_path": str(prompt_path),
                "agent_command_template": config.agent_command_template,
                "proxy_url": proxy_url,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {"returncode": process.returncode, "summary": summary}


def run_codex(
    config: WorkerConfig,
    task: dict[str, object],
    workspace_dir: Path,
    logs_dir: Path,
) -> dict[str, object]:
    return run_agent(config, task, workspace_dir, logs_dir)


def build_agent_command(
    config: WorkerConfig,
    task: dict[str, object],
    workspace_dir: Path,
    final_message_path: Path,
    prompt_path: Path,
    prompt: str,
) -> list[str]:
    if config.agent_type == "codex":
        return _build_codex_command(config, workspace_dir, final_message_path)
    if config.agent_type == "command-template":
        return _build_template_command(config, task, workspace_dir, final_message_path, prompt_path, prompt)
    raise ValueError(f"unsupported agent_type: {config.agent_type}")


def _build_codex_command(config: WorkerConfig, workspace_dir: Path, final_message_path: Path) -> list[str]:
    command = [
        *resolve_agent_launcher(config.agent_bin or "codex"),
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
    if config.agent_model:
        command.extend(["-m", config.agent_model])
    command.extend(config.agent_extra_args)
    command.append("-")
    return command


def _build_template_command(
    config: WorkerConfig,
    task: dict[str, object],
    workspace_dir: Path,
    final_message_path: Path,
    prompt_path: Path,
    prompt: str,
) -> list[str]:
    template = resolve_agent_command_template(config)
    replacements = {
        "agent_bin": config.agent_bin or "",
        "workspace_dir": str(workspace_dir),
        "final_message_path": str(final_message_path),
        "prompt_path": str(prompt_path),
        "prompt": prompt,
        "task_id": str(task["id"]),
        "title": str(task["title"]),
        "detail": str(task["detail"]),
        "model": config.agent_model or "",
    }
    command = [_format_template_arg(arg, replacements) for arg in template]
    command = [arg for arg in command if arg != ""]
    command.extend(config.agent_extra_args)
    return command


def resolve_agent_command_template(config: WorkerConfig) -> list[str]:
    if config.agent_command_template:
        return load_agent_command_template(config.agent_command_template)
    return default_agent_command_template(config.agent_bin or "", use_stdin=bool(config.agent_use_stdin))


def default_agent_command_template(agent_bin: str, use_stdin: bool = False) -> list[str]:
    agent_name = Path(agent_bin).stem.lower()
    if "claude" in agent_name:
        command = [
            "{agent_bin}",
            "--print",
            "--output-format",
            "text",
            "--permission-mode",
            "bypassPermissions",
            "--setting-sources",
            "user",
            "--add-dir",
            "{workspace_dir}",
        ]
        if not use_stdin:
            command.append("{prompt}")
        return command
    if use_stdin:
        return ["{agent_bin}"]
    return ["{agent_bin}", "{prompt}"]


def load_agent_command_template(template_text: str) -> list[str]:
    candidate = Path(template_text)
    if candidate.is_file():
        raw = candidate.read_text(encoding="utf-8")
    else:
        raw = template_text
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = shlex.split(raw, posix=False)
    if not isinstance(payload, list) or not all(isinstance(item, str) for item in payload):
        raise ValueError("agent_command_template must be a JSON string array or a shell-style command string")
    return list(payload)


def _format_template_arg(template: str, replacements: dict[str, str]) -> str:
    result = template
    for key, value in replacements.items():
        result = result.replace("{" + key + "}", value)
    return result


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
