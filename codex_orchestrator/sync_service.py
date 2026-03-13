from __future__ import annotations

import sys
import time
from pathlib import Path

from .store import TaskStore
from .sync_providers import SyncProvider, create_provider, load_provider_config


def sync_once(
    db_path: str | Path,
    config_path: str | Path,
    provider: SyncProvider | None = None,
) -> dict[str, int]:
    store = TaskStore(db_path)
    provider = provider or create_provider(load_provider_config(str(config_path)))
    return _sync_with_provider(store, provider)


def _sync_with_provider(store: TaskStore, provider: SyncProvider) -> dict[str, int]:
    imported = 0
    updated = 0
    writeback_errors = 0

    for source_task in provider.list_tasks():
        store.upsert_external_task(
            source_name=provider.name,
            source_task_key=source_task.source_task_key,
            title=source_task.title,
            detail=source_task.detail,
            status=source_task.status,
        )
        imported += 1

    if provider.can_write:
        for task in store.list_tasks_for_source(provider.name):
            try:
                provider.update_status(str(task["source_task_key"]), str(task["status"]))
                updated += 1
            except Exception as exc:
                writeback_errors += 1
                print(
                    f"sync writeback failed: source={provider.name} "
                    f"task_key={task['source_task_key']} status={task['status']} error={exc}",
                    file=sys.stderr,
                )

    return {"imported": imported, "updated": updated, "writeback_errors": writeback_errors}


def sync_loop(db_path: str | Path, config_path: str | Path, interval_seconds: int) -> None:
    store = TaskStore(db_path)
    provider = create_provider(load_provider_config(str(config_path)))
    while True:
        try:
            result = _sync_with_provider(store, provider)
            print(
                f"sync completed: imported={result['imported']} "
                f"updated={result['updated']} "
                f"writeback_errors={result['writeback_errors']} config={config_path}"
            )
        except Exception as exc:
            print(f"sync failed: config={config_path} error={exc}", file=sys.stderr)
        time.sleep(interval_seconds)
