from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path


def run_pool(
    worker_count: int,
    server_url: str,
    template_dir: Path,
    runtime_dir: Path,
    codex_bin: str,
    codex_model: str | None,
    lease_seconds: int,
    poll_interval: int,
    codex_timeout_seconds: int,
    codex_extra_args: list[str],
) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    processes: dict[int, subprocess.Popen[str]] = {}
    try:
        for index in range(worker_count):
            processes[index] = _spawn_worker(
                index=index,
                server_url=server_url,
                template_dir=template_dir,
                runtime_dir=runtime_dir,
                codex_bin=codex_bin,
                codex_model=codex_model,
                lease_seconds=lease_seconds,
                poll_interval=poll_interval,
                codex_timeout_seconds=codex_timeout_seconds,
                codex_extra_args=codex_extra_args,
            )

        while True:
            time.sleep(2)
            for index, process in list(processes.items()):
                exit_code = process.poll()
                if exit_code is None:
                    continue
                print(f"worker-{index} exited with code {exit_code}, restarting")
                processes[index] = _spawn_worker(
                    index=index,
                    server_url=server_url,
                    template_dir=template_dir,
                    runtime_dir=runtime_dir,
                    codex_bin=codex_bin,
                    codex_model=codex_model,
                    lease_seconds=lease_seconds,
                    poll_interval=poll_interval,
                    codex_timeout_seconds=codex_timeout_seconds,
                    codex_extra_args=codex_extra_args,
                )
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


def _spawn_worker(
    index: int,
    server_url: str,
    template_dir: Path,
    runtime_dir: Path,
    codex_bin: str,
    codex_model: str | None,
    lease_seconds: int,
    poll_interval: int,
    codex_timeout_seconds: int,
    codex_extra_args: list[str],
) -> subprocess.Popen[str]:
    worker_runtime = runtime_dir / f"worker-{index}"
    worker_runtime.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "codex_orchestrator",
        "worker",
        "--server-url",
        server_url,
        "--worker-id",
        f"worker-{index}",
        "--template-dir",
        str(template_dir),
        "--runtime-dir",
        str(worker_runtime),
        "--lease-seconds",
        str(lease_seconds),
        "--poll-interval",
        str(poll_interval),
        "--codex-timeout-seconds",
        str(codex_timeout_seconds),
        "--codex-bin",
        codex_bin,
    ]
    if codex_model:
        command.extend(["--codex-model", codex_model])
    for arg in codex_extra_args:
        command.extend(["--codex-arg", arg])
    return subprocess.Popen(command, text=True)
