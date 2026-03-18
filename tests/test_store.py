import tempfile
import threading
import unittest
import os
import shutil
import time
import io
import json
from pathlib import Path
from unittest import mock
from contextlib import redirect_stderr

from codex_orchestrator.network import apply_process_proxy
from codex_orchestrator.__main__ import _resolve_existing_dir, build_parser
from codex_orchestrator.stack import build_stack_process_specs
from codex_orchestrator.store import TaskStore, _parse_db_target
from codex_orchestrator.sync_providers import (
    DingTalkBaseProvider,
    GoogleSheetsProvider,
    ProviderConfig,
    SourceTask,
    _resolve_spreadsheet_id,
    _prioritize_available_tools,
    _tool_aliases,
    _unwrap_dingtalk_mcp_payload,
)
from codex_orchestrator.sync_service import _format_exception, sync_once
from codex_orchestrator.worker import (
    WorkerConfig,
    build_agent_command,
    build_codex_env,
    copy_template,
    default_agent_command_template,
    load_agent_command_template,
    prepare_workspace_for_agent,
    resolve_codex_launcher,
    should_sync_workspace_back,
    sync_workspace_to_template,
    should_cleanup_workspace,
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

    def test_copy_template_skips_other_runtime_dirs_and_sqlite_transients(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            template_dir = root / "repo"
            runtime_dir = template_dir / ".claude-runtime" / "worker-0"
            workspace_dir = root / "workspace"
            template_dir.mkdir()
            runtime_dir.mkdir(parents=True)
            (template_dir / "app.txt").write_text("keep", encoding="utf-8")
            codex_runtime = template_dir / ".codex-runtime"
            codex_runtime.mkdir(parents=True)
            (codex_runtime / "tasks.db").write_text("skip", encoding="utf-8")
            (template_dir / "temp.db-wal").write_text("skip", encoding="utf-8")
            (template_dir / "temp.db-shm").write_text("skip", encoding="utf-8")

            copy_template(template_dir, workspace_dir, runtime_dir)

            self.assertTrue((workspace_dir / "app.txt").exists())
            self.assertFalse((workspace_dir / ".codex-runtime").exists())
            self.assertFalse((workspace_dir / ".claude-runtime").exists())
            self.assertFalse((workspace_dir / "temp.db-wal").exists())
            self.assertFalse((workspace_dir / "temp.db-shm").exists())

    def test_prepare_workspace_for_agent_writes_workspace_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workspace_dir = root / "workspace"
            workspace_dir.mkdir()
            config = WorkerConfig(
                server_url="http://127.0.0.1:8000",
                worker_id="worker-0",
                template_dir=root,
                runtime_dir=root / "runtime",
                results_dir=root / "results",
                agent_type="command-template",
                agent_bin="my-agent",
            )

            prepare_workspace_for_agent(config, workspace_dir)

            marker = workspace_dir / ".codex_orchestrator_workspace_root"
            self.assertTrue(marker.exists())
            self.assertEqual(str(workspace_dir), marker.read_text(encoding="utf-8"))

    def test_should_cleanup_workspace_on_success(self) -> None:
        self.assertTrue(should_cleanup_workspace("on-success", "completed"))
        self.assertFalse(should_cleanup_workspace("on-success", "released"))

    def test_should_cleanup_workspace_always(self) -> None:
        self.assertTrue(should_cleanup_workspace("always", "completed"))
        self.assertTrue(should_cleanup_workspace("always", "released"))

    def test_should_cleanup_workspace_never(self) -> None:
        self.assertFalse(should_cleanup_workspace("never", "completed"))
        self.assertFalse(should_cleanup_workspace("never", "released"))

    def test_should_cleanup_workspace_after_sync_back(self) -> None:
        self.assertTrue(should_cleanup_workspace("after-sync-back", "completed", True))
        self.assertFalse(should_cleanup_workspace("after-sync-back", "completed", False))
        self.assertFalse(should_cleanup_workspace("after-sync-back", "released", True))

    def test_should_sync_workspace_back_modes(self) -> None:
        self.assertFalse(should_sync_workspace_back("never", "completed"))
        self.assertTrue(should_sync_workspace_back("always", "released"))
        self.assertTrue(should_sync_workspace_back("on-success", "completed"))
        self.assertFalse(should_sync_workspace_back("on-success", "released"))

    def test_sync_workspace_to_template_syncs_changed_and_new_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            template_dir = root / "template"
            workspace_dir = root / "workspace"
            template_dir.mkdir()
            (template_dir / "a.txt").write_text("old", encoding="utf-8")
            shutil.copytree(template_dir, workspace_dir)

            (workspace_dir / "a.txt").write_text("new", encoding="utf-8")
            (workspace_dir / "b.txt").write_text("add", encoding="utf-8")
            task_started_epoch = time.time()

            stats = sync_workspace_to_template(
                workspace_dir=workspace_dir,
                template_dir=template_dir,
                task_started_epoch=task_started_epoch,
            )

            self.assertEqual("new", (template_dir / "a.txt").read_text(encoding="utf-8"))
            self.assertEqual("add", (template_dir / "b.txt").read_text(encoding="utf-8"))
            self.assertEqual(2, stats["synced_files"])
            self.assertEqual(1, stats["updated_files"])
            self.assertEqual(1, stats["created_files"])
            self.assertEqual(0, stats["conflict_files"])

    def test_sync_workspace_to_template_skips_conflict_if_target_changed_after_start(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            template_dir = root / "template"
            workspace_dir = root / "workspace"
            template_dir.mkdir()
            source_file = template_dir / "a.txt"
            source_file.write_text("source-new", encoding="utf-8")
            shutil.copytree(template_dir, workspace_dir)

            (workspace_dir / "a.txt").write_text("workspace-new", encoding="utf-8")
            task_started_epoch = time.time() - 60
            future_time = task_started_epoch + 120
            os.utime(source_file, (future_time, future_time))

            stats = sync_workspace_to_template(
                workspace_dir=workspace_dir,
                template_dir=template_dir,
                task_started_epoch=task_started_epoch,
            )

            self.assertEqual("source-new", source_file.read_text(encoding="utf-8"))
            self.assertEqual(0, stats["synced_files"])
            self.assertEqual(1, stats["conflict_files"])

    def test_sync_workspace_to_template_skips_workspace_metadata_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            template_dir = root / "template"
            workspace_dir = root / "workspace"
            template_dir.mkdir()
            workspace_dir.mkdir()
            git_dir = workspace_dir / ".git"
            git_dir.mkdir()
            (git_dir / "config").write_text("fake", encoding="utf-8")
            (workspace_dir / ".codex_orchestrator_workspace_root").write_text("root", encoding="utf-8")
            (workspace_dir / "app.txt").write_text("changed", encoding="utf-8")

            stats = sync_workspace_to_template(
                workspace_dir=workspace_dir,
                template_dir=template_dir,
                task_started_epoch=time.time(),
            )

            self.assertTrue((template_dir / "app.txt").exists())
            self.assertFalse((template_dir / ".git").exists())
            self.assertFalse((template_dir / ".codex_orchestrator_workspace_root").exists())
            self.assertEqual(1, stats["synced_files"])

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

    def test_worker_config_keeps_legacy_codex_defaults(self) -> None:
        config = WorkerConfig(
            server_url="http://127.0.0.1:8000",
            worker_id="worker-0",
            template_dir=Path("."),
            runtime_dir=Path(".codex-runtime"),
            results_dir=Path(".codex-runtime/task-results"),
            codex_bin="codex.cmd",
            codex_model="gpt-5-codex",
            codex_timeout_seconds=321,
            codex_extra_args=["--foo"],
        )
        self.assertEqual("codex", config.agent_type)
        self.assertEqual("codex.cmd", config.agent_bin)
        self.assertEqual("gpt-5-codex", config.agent_model)
        self.assertEqual(321, config.agent_timeout_seconds)
        self.assertEqual(["--foo"], config.agent_extra_args)
        self.assertTrue(config.agent_use_stdin)
        self.assertEqual("after-sync-back", config.workspace_cleanup)
        self.assertEqual("on-success", config.workspace_sync_back)

    def test_worker_config_defaults_claude_command_template_to_stdin(self) -> None:
        config = WorkerConfig(
            server_url="http://127.0.0.1:8000",
            worker_id="worker-0",
            template_dir=Path("."),
            runtime_dir=Path(".codex-runtime"),
            results_dir=Path(".codex-runtime/task-results"),
            agent_type="command-template",
            agent_bin="claude.cmd",
        )
        self.assertTrue(config.agent_use_stdin)

    def test_worker_config_rejects_invalid_workspace_cleanup(self) -> None:
        with self.assertRaises(ValueError):
            WorkerConfig(
                server_url="http://127.0.0.1:8000",
                worker_id="worker-0",
                template_dir=Path("."),
                runtime_dir=Path(".codex-runtime"),
                results_dir=Path(".codex-runtime/task-results"),
                workspace_cleanup="invalid-mode",
            )

    def test_worker_config_rejects_invalid_workspace_sync_back(self) -> None:
        with self.assertRaises(ValueError):
            WorkerConfig(
                server_url="http://127.0.0.1:8000",
                worker_id="worker-0",
                template_dir=Path("."),
                runtime_dir=Path(".codex-runtime"),
                results_dir=Path(".codex-runtime/task-results"),
                workspace_sync_back="invalid-mode",
            )

    def test_load_agent_command_template_accepts_json_array(self) -> None:
        template = load_agent_command_template('["claude", "-p", "{prompt_path}"]')
        self.assertEqual(["claude", "-p", "{prompt_path}"], template)

    def test_build_agent_command_supports_command_template(self) -> None:
        config = WorkerConfig(
            server_url="http://127.0.0.1:8000",
            worker_id="worker-0",
            template_dir=Path("."),
            runtime_dir=Path(".codex-runtime"),
            results_dir=Path(".codex-runtime/task-results"),
            agent_type="command-template",
            agent_bin="claude",
            agent_model="sonnet",
            agent_command_template='["{agent_bin}", "--print", "--cwd", "{workspace_dir}", "--prompt-file", "{prompt_path}", "--output", "{final_message_path}", "--model", "{model}"]',
            agent_extra_args=["--dangerously-skip-permissions"],
            agent_use_stdin=False,
        )
        command = build_agent_command(
            config=config,
            task={"id": 7, "title": "demo", "detail": "detail"},
            workspace_dir=Path("F:/tmp/workspace"),
            final_message_path=Path("F:/tmp/final.txt"),
            prompt_path=Path("F:/tmp/prompt.txt"),
            prompt="hello",
        )
        self.assertEqual(
            [
                "claude",
                "--print",
                "--cwd",
                str(Path("F:/tmp/workspace")),
                "--prompt-file",
                str(Path("F:/tmp/prompt.txt")),
                "--output",
                str(Path("F:/tmp/final.txt")),
                "--model",
                "sonnet",
                "--dangerously-skip-permissions",
            ],
            command,
        )

    def test_default_agent_command_template_uses_claude_preset(self) -> None:
        self.assertEqual(
            [
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
                "{prompt}",
            ],
            default_agent_command_template("claude"),
        )

    def test_default_agent_command_template_uses_claude_stdin_preset(self) -> None:
        self.assertEqual(
            [
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
            ],
            default_agent_command_template("claude", use_stdin=True),
        )

    def test_default_agent_command_template_uses_generic_prompt_arg(self) -> None:
        self.assertEqual(["{agent_bin}", "{prompt}"], default_agent_command_template("my-agent"))

    def test_default_agent_command_template_uses_stdin_safe_fallback(self) -> None:
        self.assertEqual(["{agent_bin}"], default_agent_command_template("my-agent", use_stdin=True))

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
            self.assertEqual("已完成", updated["status"])

    def test_upsert_external_task_reopens_completed_task_when_source_returns_to_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TaskStore(Path(tmpdir) / "tasks.db")
            task = store.upsert_external_task(
                source_name="sheet-demo",
                source_task_key="2",
                title="task",
                detail="detail",
                status="未开始",
            )
            claimed = store.claim_next_task("worker-a", lease_seconds=60)
            self.assertIsNotNone(claimed)
            completed = store.complete_task(int(task["id"]), "worker-a", "done")
            self.assertIsNotNone(completed)
            reopened = store.upsert_external_task(
                source_name="sheet-demo",
                source_task_key="2",
                title="task reopened",
                detail="detail reopened",
                status="未开始",
            )
            self.assertEqual("未开始", reopened["status"])
            self.assertIsNone(reopened["completed_at"])
            self.assertIsNone(reopened["result_summary"])
            self.assertEqual("task reopened", reopened["title"])

    def test_upsert_external_task_can_mark_non_running_task_done_from_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TaskStore(Path(tmpdir) / "tasks.db")
            task = store.upsert_external_task(
                source_name="sheet-demo",
                source_task_key="2",
                title="task",
                detail="detail",
                status="未开始",
            )
            updated = store.upsert_external_task(
                source_name="sheet-demo",
                source_task_key="2",
                title="task",
                detail="detail",
                status="已完成",
            )
            self.assertEqual(task["id"], updated["id"])
            self.assertEqual("已完成", updated["status"])
            self.assertIsNotNone(updated["completed_at"])
            self.assertIsNone(updated["result_summary"])

    def test_upsert_external_task_does_not_override_running_task_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TaskStore(Path(tmpdir) / "tasks.db")
            store.upsert_external_task(
                source_name="sheet-demo",
                source_task_key="2",
                title="task",
                detail="detail",
                status="未开始",
            )
            claimed = store.claim_next_task("worker-a", lease_seconds=60)
            self.assertIsNotNone(claimed)
            updated = store.upsert_external_task(
                source_name="sheet-demo",
                source_task_key="2",
                title="task updated",
                detail="detail updated",
                status="已完成",
            )
            self.assertEqual("执行中", updated["status"])
            self.assertEqual("worker-a", updated["claimed_by"])

    def test_delete_task_removes_single_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TaskStore(Path(tmpdir) / "tasks.db")
            task = store.add_task("title", "detail")
            deleted = store.delete_task(int(task["id"]))
            self.assertTrue(deleted)
            self.assertIsNone(store.get_task(int(task["id"])))

    def test_reset_tasks_can_clear_by_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TaskStore(Path(tmpdir) / "tasks.db")
            store.upsert_external_task("source-a", "1", "task1", "detail1", "未开始")
            store.upsert_external_task("source-a", "2", "task2", "detail2", "未开始")
            store.upsert_external_task("source-b", "1", "task3", "detail3", "未开始")
            deleted_count = store.reset_tasks(source_name="source-a")
            self.assertEqual(2, deleted_count)
            self.assertEqual(["source-b"], store.list_sources())
            self.assertEqual(1, len(store.list_tasks()))

    def test_reset_tasks_can_clear_all(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TaskStore(Path(tmpdir) / "tasks.db")
            store.add_task("title1", "detail1")
            store.add_task("title2", "detail2")
            deleted_count = store.reset_tasks()
            self.assertEqual(2, deleted_count)
            self.assertEqual([], store.list_tasks())

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

            self.assertEqual({"imported": 2, "updated": 2, "writeback_errors": 0}, result)
            store = TaskStore(db_path)
            tasks = store.list_tasks_for_source("sheet-demo")
            self.assertEqual(2, len(tasks))
            provider.update_status.assert_any_call("2", "未开始")
            provider.update_status.assert_any_call("3", "未开始")

    def test_sync_once_continues_after_single_writeback_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "tasks.db"

            provider = mock.Mock()
            provider.name = "sheet-demo"
            provider.can_write = True
            provider.list_tasks.return_value = [
                SourceTask(source_task_key="2", title="task a", detail="detail a", status="未开始"),
                SourceTask(source_task_key="3", title="task b", detail="detail b", status="未开始"),
            ]
            provider.update_status.side_effect = [RuntimeError("boom"), None]

            with mock.patch("codex_orchestrator.sync_service.load_provider_config"), mock.patch(
                "codex_orchestrator.sync_service.create_provider",
                return_value=provider,
            ), mock.patch("sys.stderr"):
                result = sync_once(db_path, Path(tmpdir) / "provider.json")

            self.assertEqual({"imported": 2, "updated": 1, "writeback_errors": 1}, result)
            self.assertEqual(2, provider.update_status.call_count)

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

    def test_google_provider_normalizes_status_alias(self) -> None:
        provider = GoogleSheetsProvider(
            ProviderConfig(
                provider="google-sheets",
                name="sheet-demo",
                options={
                    "spreadsheet_id": "sheet-id",
                    "sheet_name": "Sheet1",
                    "api_key": "api-key",
                },
            )
        )
        with mock.patch.object(
            provider,
            "_request_json",
            return_value={
                "values": [
                    ["标题", "任务详情", "状态"],
                    ["任务一", "详情一", "未完成"],
                    ["任务二", "详情二", "进行中"],
                ]
            },
        ):
            tasks = provider.list_tasks()
        self.assertEqual(
            [
                SourceTask(source_task_key="2", title="任务一", detail="详情一", status="未开始"),
                SourceTask(source_task_key="3", title="任务二", detail="详情二", status="执行中"),
            ],
            tasks,
        )

    def test_google_provider_skips_rows_with_blank_status(self) -> None:
        provider = GoogleSheetsProvider(
            ProviderConfig(
                provider="google-sheets",
                name="sheet-demo",
                options={
                    "spreadsheet_id": "sheet-id",
                    "sheet_name": "Sheet1",
                    "api_key": "api-key",
                },
            )
        )
        with mock.patch.object(
            provider,
            "_request_json",
            return_value={
                "values": [
                    ["标题", "任务详情", "状态"],
                    ["任务一", "详情一", ""],
                    ["任务二", "详情二", "未开始"],
                ]
            },
        ):
            tasks = provider.list_tasks()
        self.assertEqual(
            [
                SourceTask(source_task_key="3", title="任务二", detail="详情二", status="未开始"),
            ],
            tasks,
        )

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

    def test_dingtalk_base_provider_lists_tasks_with_pagination(self) -> None:
        with mock.patch(
            "codex_orchestrator.sync_providers._call_mcp_tool_with_fallbacks",
            side_effect=[
                {
                    "success": True,
                    "result": {
                        "hasMore": True,
                        "cursor": "next-page",
                        "records": [
                            {
                                "id": "record-1",
                                "fields": {
                                    "标题": "任务一",
                                    "任务详情": "详情一",
                                    "状态": "未开始",
                                },
                            }
                        ],
                    },
                },
                {
                    "success": True,
                    "result": {
                        "hasMore": False,
                        "cursor": "",
                        "records": [
                            {
                                "id": "record-2",
                                "fields": {
                                    "标题": "任务二",
                                    "任务详情": "详情二",
                                    "状态": "未完成",
                                },
                            }
                        ],
                    },
                },
            ],
        ) as call_mcp_tool:
            provider = DingTalkBaseProvider(
                ProviderConfig(
                    provider="dingtalk-base",
                    name="dingtalk-demo",
                    options={
                        "mcp_url": "https://example.com/mcp",
                        "dentry_uuid": "doc-id",
                        "sheet_id_or_name": "Sheet1",
                    },
                )
            )
            tasks = provider.list_tasks()

        self.assertEqual(
            [
                SourceTask(source_task_key="record-1", title="任务一", detail="详情一", status="未开始"),
                SourceTask(source_task_key="record-2", title="任务二", detail="详情二", status="未开始"),
            ],
            tasks,
        )
        self.assertEqual(2, call_mcp_tool.call_count)
        self.assertEqual("search_base_record", provider.search_tool_name)
        self.assertEqual(
            mock.call(
                server_url="https://example.com/mcp",
                tool_names=["search_base_record", "search_base_records"],
                arguments={"dentryUuid": "doc-id", "sheetIdOrName": "Sheet1"},
                timeout_seconds=30,
            ),
            call_mcp_tool.call_args_list[0],
        )
        self.assertEqual(
            mock.call(
                server_url="https://example.com/mcp",
                tool_names=["search_base_record", "search_base_records"],
                arguments={"dentryUuid": "doc-id", "sheetIdOrName": "Sheet1", "cursor": "next-page"},
                timeout_seconds=30,
            ),
            call_mcp_tool.call_args_list[1],
        )

    def test_dingtalk_base_provider_skips_blank_status_records(self) -> None:
        with mock.patch(
            "codex_orchestrator.sync_providers._call_mcp_tool_with_fallbacks",
            return_value={
                "success": True,
                "result": {
                    "hasMore": False,
                    "cursor": "",
                    "records": [
                        {
                            "id": "record-1",
                            "fields": {
                                "标题": "任务一",
                                "任务详情": "详情一",
                                "状态": "",
                            },
                        },
                        {
                            "id": "record-2",
                            "fields": {
                                "标题": "任务二",
                                "任务详情": "详情二",
                                "状态": "未开始",
                            },
                        },
                    ],
                },
            },
        ):
            provider = DingTalkBaseProvider(
                ProviderConfig(
                    provider="dingtalk-base",
                    name="dingtalk-demo",
                    options={
                        "mcp_url": "https://example.com/mcp",
                        "dentry_uuid": "doc-id",
                        "sheet_id_or_name": "Sheet1",
                    },
                )
            )
            tasks = provider.list_tasks()

        self.assertEqual(
            [
                SourceTask(source_task_key="record-2", title="任务二", detail="详情二", status="未开始"),
            ],
            tasks,
        )

    def test_dingtalk_base_provider_updates_status(self) -> None:
        with mock.patch(
            "codex_orchestrator.sync_providers._call_mcp_tool_with_fallbacks",
            return_value={"success": True, "result": [{"id": "record-1"}]},
        ) as call_mcp_tool:
            provider = DingTalkBaseProvider(
                ProviderConfig(
                    provider="dingtalk-base",
                    name="dingtalk-demo",
                    options={
                        "mcp_url": "https://example.com/mcp",
                        "dentry_uuid": "doc-id",
                        "sheet_id_or_name": "Sheet1",
                    },
                )
            )
            provider.update_status("record-1", "已完成")

        call_mcp_tool.assert_called_once_with(
            server_url="https://example.com/mcp",
            tool_names=["update_records", "update_base_record", "update_base_records"],
            arguments={
                "dentryUuid": "doc-id",
                "sheetIdOrName": "Sheet1",
                "recordIds": [{"id": "record-1", "fields": {"状态": "已完成"}}],
            },
            timeout_seconds=30,
        )

    def test_dingtalk_tool_aliases_include_singular_plural_pairs(self) -> None:
        self.assertEqual(["search_base_record", "search_base_records"], _tool_aliases("search_base_record"))
        self.assertEqual(["search_base_records", "search_base_record"], _tool_aliases("search_base_records"))
        self.assertEqual(
            ["update_base_record", "update_base_records", "update_records"],
            _tool_aliases("update_base_record"),
        )
        self.assertEqual(
            ["update_records", "update_base_record", "update_base_records"],
            _tool_aliases("update_records"),
        )

    def test_prioritize_available_tools_prefers_server_listed_name(self) -> None:
        self.assertEqual(
            ["search_base_records", "search_base_record"],
            _prioritize_available_tools(
                ["search_base_record", "search_base_records"],
                {"search_base_records"},
            ),
        )

    def test_dingtalk_base_provider_can_disable_writeback(self) -> None:
        provider = DingTalkBaseProvider(
            ProviderConfig(
                provider="dingtalk-base",
                name="dingtalk-demo",
                options={
                    "mcp_url": "https://example.com/mcp",
                    "dentry_uuid": "doc-id",
                    "sheet_id_or_name": "Sheet1",
                    "write_enabled": False,
                },
            )
        )
        self.assertFalse(provider.can_write)

    def test_unwrap_dingtalk_payload_accepts_success_with_list_result(self) -> None:
        result = _unwrap_dingtalk_mcp_payload({"success": True, "result": [{"id": "record-1"}]})
        self.assertEqual([{"id": "record-1"}], result)

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

    def test_resolve_existing_dir_accepts_existing_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            resolved = _resolve_existing_dir(tmpdir, "--template-dir")
            self.assertEqual(Path(tmpdir).resolve(), resolved)

    def test_resolve_existing_dir_rejects_file_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "a.txt"
            file_path.write_text("x", encoding="utf-8")
            with self.assertRaises(ValueError):
                _resolve_existing_dir(str(file_path), "--template-dir")

    def test_resolve_existing_dir_provides_similar_dir_hint(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "campus-runner-server").mkdir()
            with self.assertRaises(ValueError) as context:
                _resolve_existing_dir(str(root / "campus-runner-serve"), "--template-dir")
            self.assertIn("did you mean", str(context.exception))
            self.assertIn("campus-runner-server", str(context.exception))

    def test_parse_db_target_supports_mysql_url(self) -> None:
        dialect, db_path, mysql_config = _parse_db_target(
            "mysql://demo-user:demo-pass@db.example.com:3307/codex_tasks?charset=utf8mb4&connect_timeout=12"
        )
        self.assertEqual("mysql", dialect)
        self.assertIsNone(db_path)
        self.assertEqual("db.example.com", mysql_config["host"])
        self.assertEqual(3307, mysql_config["port"])
        self.assertEqual("demo-user", mysql_config["user"])
        self.assertEqual("demo-pass", mysql_config["password"])
        self.assertEqual("codex_tasks", mysql_config["database"])
        self.assertEqual("utf8mb4", mysql_config["charset"])
        self.assertEqual(12, mysql_config["connect_timeout"])

    def test_build_parser_requires_db_for_serve_and_sync(self) -> None:
        parser = build_parser()
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["serve"])
            with self.assertRaises(SystemExit):
                parser.parse_args(["sync", "once", "--config", "provider.json"])

    def test_build_stack_process_specs_resolves_relative_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "repo").mkdir()
            (root / "providers").mkdir()
            (root / "providers" / "sheet.json").write_text("{}", encoding="utf-8")
            config_path = root / "stack.json"
            config_path.write_text(
                json.dumps(
                    {
                        "database_url": "mysql://root:root@127.0.0.1:3306/agent_tasks?charset=utf8mb4",
                        "serve": {"host": "127.0.0.1", "port": 8123},
                        "sync": {"config": "./providers/sheet.json", "interval_seconds": 9},
                        "pool": {
                            "workers": 4,
                            "template_dir": "./repo",
                            "runtime_dir": "./runtime",
                            "agent_type": "command-template",
                            "agent_bin": "claude.cmd",
                            "agent_timeout_seconds": 123,
                            "proxy_url": "http://127.0.0.1:7890",
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            specs = build_stack_process_specs(config_path)

            self.assertEqual(["serve", "sync", "pool"], [spec.name for spec in specs])
            self.assertIn("mysql://root:root@127.0.0.1:3306/agent_tasks?charset=utf8mb4", specs[0].command)
            self.assertIn(str((root / "providers" / "sheet.json").resolve()), specs[1].command)
            self.assertIn(str((root / "repo").resolve()), specs[2].command)
            self.assertIn(str((root / "runtime").resolve()), specs[2].command)
            self.assertIn("http://127.0.0.1:8123", specs[2].command)

    def test_format_exception_unwraps_nested_exceptions(self) -> None:
        try:
            raise RuntimeError("inner boom")
        except RuntimeError as inner:
            outer = RuntimeError("outer boom")
            outer.__cause__ = inner
        formatted = _format_exception(outer)
        self.assertIn("RuntimeError: outer boom", formatted)
        self.assertIn("RuntimeError: inner boom", formatted)


if __name__ == "__main__":
    unittest.main()
