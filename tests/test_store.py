import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from codex_orchestrator.store import TaskStore
from codex_orchestrator.worker import copy_template, resolve_codex_launcher


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


if __name__ == "__main__":
    unittest.main()
