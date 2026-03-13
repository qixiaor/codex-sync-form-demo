import tempfile
import threading
import unittest
import os
from pathlib import Path
from unittest import mock

from codex_orchestrator.network import apply_process_proxy
from codex_orchestrator.store import TaskStore
from codex_orchestrator.sync_providers import (
    GoogleSheetsProvider,
    ProviderConfig,
    SourceTask,
    _resolve_spreadsheet_id,
)
from codex_orchestrator.sync_service import sync_once
from codex_orchestrator.worker import (
    WorkerConfig,
    build_codex_env,
    copy_template,
    resolve_codex_launcher,
    write_task_result,
)


class TaskStoreTests(unittest.TestCase):
    def test_concurrent_claims_are_unique(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TaskStore(Path(tmpdir) / "tasks.db")
            for index in range(10):
                store.add_task(f"title-{index}", f"detail-{index}")

            barrier = threading.Barrier(10)
            claimed_ids: list[int] = []
            lock = threading.Lock()

            def worker(index: int) -> None:
                barrier.wait()
                task = store.claim_next_task(f"worker-{index}", lease_seconds=60)
                if task is None:
                    return
                with lock:
                    claimed_ids.append(int(task["id"]))

            threads = [threading.Thread(target=worker, args=(index,)) for index in range(10)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            self.assertEqual(10, len(claimed_ids))
            self.assertEqual(10, len(set(claimed_ids)))

    def test_expired_lease_can_be_reclaimed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "tasks.db"
            store = TaskStore(db_path)
            task = store.add_task("title", "detail")
            claimed = store.claim_next_task("worker-a", lease_seconds=60)
            self.assertEqual(task["id"], claimed["id"])

            with store._connect() as conn:
                conn.execute(
                    """
                    UPDATE tasks
                    SET lease_expires_at = '2000-01-01T00:00:00+00:00'
                    WHERE id = ?
                    """,
                    (task["id"],),
                )

            reclaimed = store.claim_next_task("worker-b", lease_seconds=60)
            self.assertIsNotNone(reclaimed)
            self.assertEqual(task["id"], reclaimed["id"])
            self.assertEqual("worker-b", reclaimed["claimed_by"])

    def test_copy_template_skips_runtime_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            template_dir = root / "repo"
            runtime_dir = template_dir / ".codex-runtime" / "worker-0"
            workspace_dir = root / "workspace"
            template_dir.mkdir()
            runtime_dir.mkdir(parents=True)
            (template_dir / "app.txt").write_text("keep", encoding="utf-8")
            (template_dir / ".codex-runtime" / "tasks.db").write_text("skip", encoding="utf-8")

            copy_template(template_dir, workspace_dir, runtime_dir)

            self.assertTrue((workspace_dir / "app.txt").exists())
            self.assertFalse((workspace_dir / ".codex-runtime").exists())

    def test_resolve_codex_launcher_uses_cmd_on_windows(self) -> None:
        with mock.patch("codex_orchestrator.worker.shutil.which", return_value=r"E:\nodejs\codex.cmd"):
            self.assertEqual([r"E:\nodejs\codex.cmd"], resolve_codex_launcher("codex"))

    def test_resolve_codex_launcher_wraps_ps1(self) -> None:
        with mock.patch("codex_orchestrator.worker.shutil.which", return_value=r"E:\nodejs\codex.ps1"):
            self.assertEqual(
                [
                    "powershell",
                    "-NoLogo",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    r"E:\nodejs\codex.ps1",
                ],
                resolve_codex_launcher("codex"),
            )

    def test_build_codex_env_uses_explicit_proxy(self) -> None:
        config = WorkerConfig(
            server_url="http://127.0.0.1:8000",
            worker_id="worker-0",
            template_dir=Path("."),
            runtime_dir=Path(".codex-runtime"),
            results_dir=Path(".codex-runtime/task-results"),
            proxy_url="http://127.0.0.1:7890",
            auto_proxy=False,
        )
        env = build_codex_env(config)
        self.assertEqual("http://127.0.0.1:7890", env["HTTP_PROXY"])
        self.assertEqual("127.0.0.1,localhost,::1", env["NO_PROXY"])

    def test_build_codex_env_auto_detects_local_proxy(self) -> None:
        config = WorkerConfig(
            server_url="http://127.0.0.1:8000",
            worker_id="worker-0",
            template_dir=Path("."),
            runtime_dir=Path(".codex-runtime"),
            results_dir=Path(".codex-runtime/task-results"),
        )
        with mock.patch("codex_orchestrator.network.is_proxy_reachable", return_value=True):
            env = build_codex_env(config)
        self.assertEqual("http://127.0.0.1:7890", env["HTTPS_PROXY"])

    def test_apply_process_proxy_sets_environment(self) -> None:
        keys = ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy"]
        previous = {key: os.environ.get(key) for key in keys}
        try:
            with mock.patch("codex_orchestrator.network.is_proxy_reachable", return_value=True):
                proxy_url = apply_process_proxy(None, auto_proxy=True)
            self.assertEqual("http://127.0.0.1:7890", proxy_url)
            self.assertEqual("http://127.0.0.1:7890", os.environ["HTTPS_PROXY"])
            self.assertEqual("127.0.0.1,localhost,::1", os.environ["NO_PROXY"])
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_write_task_result_creates_task_named_summary_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config = WorkerConfig(
                server_url="http://127.0.0.1:8000",
                worker_id="worker-0",
                template_dir=root,
                runtime_dir=root / "runtime",
                results_dir=root / "runtime" / "task-results",
            )
            run_dir = root / "runtime" / "worker-0" / "task-0001-demo"
            logs_dir = run_dir / "logs"
            workspace_dir = run_dir / "workspace"
            logs_dir.mkdir(parents=True)
            workspace_dir.mkdir(parents=True)

            write_task_result(
                config=config,
                task={
                    "id": 1,
                    "title": "demo task",
                    "detail": "detail",
                    "status": "已完成",
                    "attempt_count": 1,
                    "claimed_by": None,
                },
                execution_status="completed",
                run_dir=run_dir,
                workspace_dir=workspace_dir,
                logs_dir=logs_dir,
                result_summary="done",
                codex_returncode=0,
            )

            self.assertTrue((config.results_dir / "task-0001-demo-task.json").exists())
            self.assertTrue((config.results_dir / "task-0001-demo-task.txt").exists())

    def test_upsert_external_task_binds_source_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TaskStore(Path(tmpdir) / "tasks.db")
            task = store.upsert_external_task(
                source_name="google-sheet-demo",
                source_task_key="2",
                title="sheet task",
                detail="detail",
                status="未开始",
            )
            self.assertEqual("google-sheet-demo", task["source_name"])
            self.assertEqual("2", task["source_task_key"])

            updated = store.upsert_external_task(
                source_name="google-sheet-demo",
                source_task_key="2",
                title="sheet task updated",
                detail="detail 2",
                status="已完成",
            )
            self.assertEqual(task["id"], updated["id"])
            self.assertEqual("sheet task updated", updated["title"])
            self.assertEqual("detail 2", updated["detail"])
            self.assertEqual("未开始", updated["status"])

    def test_sync_once_imports_and_pushes_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "tasks.db"

            provider = mock.Mock()
            provider.name = "sheet-demo"
            provider.can_write = True
            provider.list_tasks.return_value = [
                SourceTask(source_task_key="2", title="task a", detail="detail a", status="未开始"),
                SourceTask(source_task_key="3", title="task b", detail="detail b", status="未开始"),
            ]

            with mock.patch("codex_orchestrator.sync_service.load_provider_config"), mock.patch(
                "codex_orchestrator.sync_service.create_provider",
                return_value=provider,
            ):
                result = sync_once(db_path, Path(tmpdir) / "provider.json")

            self.assertEqual({"imported": 2, "updated": 2}, result)
            store = TaskStore(db_path)
            tasks = store.list_tasks_for_source("sheet-demo")
            self.assertEqual(2, len(tasks))
            provider.update_status.assert_any_call("2", "未开始")
            provider.update_status.assert_any_call("3", "未开始")

    def test_resolve_spreadsheet_id_from_share_url(self) -> None:
        spreadsheet_id = _resolve_spreadsheet_id(
            {
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/1AbCdEfGhIjKlMnOpQrStUvWxYz/edit?gid=0#gid=0"
            }
        )
        self.assertEqual("1AbCdEfGhIjKlMnOpQrStUvWxYz", spreadsheet_id)

    def test_google_provider_accepts_spreadsheet_url_with_direct_token(self) -> None:
        provider = GoogleSheetsProvider(
            ProviderConfig(
                provider="google-sheets",
                name="sheet-demo",
                options={
                    "spreadsheet_url": "https://docs.google.com/spreadsheets/d/1AbCdEfGhIjKlMnOpQrStUvWxYz/edit#gid=0",
                    "sheet_name": "Sheet1",
                    "access_token": "token",
                },
            )
        )
        self.assertEqual("1AbCdEfGhIjKlMnOpQrStUvWxYz", provider.spreadsheet_id)
        self.assertTrue(provider.can_write)

    def test_google_provider_accepts_api_key_only_as_read_only(self) -> None:
        provider = GoogleSheetsProvider(
            ProviderConfig(
                provider="google-sheets",
                name="sheet-demo",
                options={
                    "spreadsheet_url": "https://docs.google.com/spreadsheets/d/1AbCdEfGhIjKlMnOpQrStUvWxYz/edit#gid=0",
                    "sheet_name": "Sheet1",
                    "api_key": "api-key",
                },
            )
        )
        self.assertEqual("api-key", provider.api_key)
        self.assertFalse(provider.can_write)

    def test_google_provider_uses_service_account_file(self) -> None:
        credentials = mock.Mock()
        credentials.valid = False
        credentials.token = ""

        def refresh(_: object) -> None:
            credentials.valid = True
            credentials.token = "service-token"

        credentials.refresh.side_effect = refresh

        with mock.patch(
            "codex_orchestrator.sync_providers._build_service_account_credentials",
            return_value=credentials,
        ) as credentials_builder, mock.patch(
            "codex_orchestrator.sync_providers._new_google_request",
            return_value=object(),
        ):
            provider = GoogleSheetsProvider(
                ProviderConfig(
                    provider="google-sheets",
                    name="sheet-demo",
                    options={
                        "spreadsheet_id": "sheet-id",
                        "sheet_name": "Sheet1",
                        "service_account_file": "service-account.json",
                    },
                )
            )
        self.assertEqual("service-token", provider.access_token)
        self.assertEqual("service-token", provider.access_token)
        credentials_builder.assert_called_once()
        credentials.refresh.assert_called_once()
        self.assertTrue(provider.can_write)

    def test_sync_loop_keeps_running_after_failure(self) -> None:
        from codex_orchestrator import sync_service

        provider = mock.Mock()
        provider.name = "sheet-demo"
        provider.can_write = False
        provider.list_tasks.side_effect = RuntimeError("boom")

        with mock.patch("codex_orchestrator.sync_service.load_provider_config"), mock.patch(
            "codex_orchestrator.sync_service.create_provider",
            return_value=provider,
        ), mock.patch(
            "codex_orchestrator.sync_service.time.sleep",
            side_effect=KeyboardInterrupt,
        ), mock.patch("sys.stderr"):
            with self.assertRaises(KeyboardInterrupt):
                sync_service.sync_loop("tasks.db", "provider.json", 1)

    def test_sync_loop_reuses_provider_instance(self) -> None:
        from codex_orchestrator import sync_service

        provider = mock.Mock()
        provider.name = "sheet-demo"
        provider.can_write = False
        provider.list_tasks.return_value = []

        with mock.patch("codex_orchestrator.sync_service.load_provider_config"), mock.patch(
            "codex_orchestrator.sync_service.create_provider",
            return_value=provider,
        ) as create_provider, mock.patch(
            "codex_orchestrator.sync_service.time.sleep",
            side_effect=KeyboardInterrupt,
        ):
            with self.assertRaises(KeyboardInterrupt):
                sync_service.sync_loop("tasks.db", "provider.json", 1)

        create_provider.assert_called_once()


if __name__ == "__main__":
    unittest.main()
