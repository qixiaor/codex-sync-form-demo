from __future__ import annotations

import argparse
import json
from pathlib import Path

from .client import TaskClient
from .pool import run_pool
from .server import serve_forever
from .worker import WorkerConfig, run_worker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Codex task orchestrator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="start the task HTTP server")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)
    serve_parser.add_argument("--db", default=".codex-runtime/tasks.db")

    add_parser = subparsers.add_parser("add", help="add a new task")
    add_parser.add_argument("--server-url", required=True)
    add_parser.add_argument("--title", required=True)
    add_parser.add_argument("--detail", required=True)

    list_parser = subparsers.add_parser("list", help="list tasks")
    list_parser.add_argument("--server-url", required=True)

    worker_parser = subparsers.add_parser("worker", help="run one worker loop")
    _add_worker_args(worker_parser)
    worker_parser.add_argument("--worker-id", required=True)

    pool_parser = subparsers.add_parser("pool", help="spawn multiple workers")
    _add_worker_args(pool_parser)
    pool_parser.add_argument("--workers", type=int, default=2)

    return parser


def _add_worker_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--server-url", required=True)
    parser.add_argument("--template-dir", default=".")
    parser.add_argument("--runtime-dir", default=".codex-runtime")
    parser.add_argument("--results-dir")
    parser.add_argument("--codex-bin", default="codex")
    parser.add_argument("--codex-model")
    parser.add_argument("--lease-seconds", type=int, default=180)
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument("--server-timeout-seconds", type=int, default=10)
    parser.add_argument("--codex-timeout-seconds", type=int, default=900)
    parser.add_argument("--proxy-url")
    parser.add_argument("--disable-auto-proxy", action="store_true")
    parser.add_argument("--codex-arg", action="append", default=[])


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "serve":
        serve_forever(args.host, args.port, args.db)
        return

    if args.command == "add":
        client = TaskClient(args.server_url)
        print(json.dumps(client.add_task(args.title, args.detail), ensure_ascii=False, indent=2))
        return

    if args.command == "list":
        client = TaskClient(args.server_url)
        print(json.dumps(client.list_tasks(), ensure_ascii=False, indent=2))
        return

    if args.command == "worker":
        runtime_dir = Path(args.runtime_dir).resolve()
        results_dir = Path(args.results_dir).resolve() if args.results_dir else _default_results_dir(runtime_dir)
        config = WorkerConfig(
            server_url=args.server_url,
            worker_id=args.worker_id,
            template_dir=Path(args.template_dir).resolve(),
            runtime_dir=runtime_dir,
            results_dir=results_dir,
            codex_bin=args.codex_bin,
            codex_model=args.codex_model,
            lease_seconds=args.lease_seconds,
            poll_interval=args.poll_interval,
            server_timeout_seconds=args.server_timeout_seconds,
            codex_timeout_seconds=args.codex_timeout_seconds,
            proxy_url=args.proxy_url,
            auto_proxy=not args.disable_auto_proxy,
            codex_extra_args=args.codex_arg,
        )
        run_worker(config)
        return

    if args.command == "pool":
        runtime_dir = Path(args.runtime_dir).resolve()
        results_dir = Path(args.results_dir).resolve() if args.results_dir else _default_results_dir(runtime_dir)
        run_pool(
            worker_count=args.workers,
            server_url=args.server_url,
            template_dir=Path(args.template_dir).resolve(),
            runtime_dir=runtime_dir,
            results_dir=results_dir,
            codex_bin=args.codex_bin,
            codex_model=args.codex_model,
            lease_seconds=args.lease_seconds,
            poll_interval=args.poll_interval,
            server_timeout_seconds=args.server_timeout_seconds,
            codex_timeout_seconds=args.codex_timeout_seconds,
            proxy_url=args.proxy_url,
            auto_proxy=not args.disable_auto_proxy,
            codex_extra_args=args.codex_arg,
        )
        return

    parser.error("unknown command")


def _default_results_dir(runtime_dir: Path) -> Path:
    if runtime_dir.name.startswith("worker-"):
        return (runtime_dir.parent / "task-results").resolve()
    return (runtime_dir / "task-results").resolve()


if __name__ == "__main__":
    main()
