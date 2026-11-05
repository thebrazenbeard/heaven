"""CLI recovery workflows must not contact or mutate chunk providers."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest

from heaven.cli import _build_store, main


def _run(*arguments: str) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        code = main(list(arguments))
    return code, stdout.getvalue(), stderr.getvalue()


class CLIRecoveryTests(unittest.TestCase):
    def test_backup_inspect_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            state = base / "state"
            storage = base / "storage"
            result, _, stderr = _run("--state", str(state), "init", "--chunk-size", "4B")
            self.assertEqual(result, 0, stderr)
            result, _, stderr = _run(
                "--state", str(state), "backend", "add-local", "--name", "one",
                "--root", str(storage), "--quota", "100B",
            )
            self.assertEqual(result, 0, stderr)
            store = _build_store(state)
            try:
                store.put_file("/hello", io.BytesIO(b"federated-storage"))
            finally:
                store.close()

            backup = base / "backup.sqlite3"
            result, output, stderr = _run("--state", str(state), "backup", str(backup))
            self.assertEqual(result, 0, stderr)
            self.assertTrue(backup.is_file())
            self.assertIsInstance(json.loads(output), dict)
            frozen = backup.read_bytes()

            # Inspect must succeed without access to configured accounts or
            # even a valid --state directory: the artifact is self-describing.
            result, output, stderr = _run(
                "--state", str(base / "missing-state"), "inspect-backup", str(backup)
            )
            self.assertEqual(result, 0, stderr)
            self.assertIsInstance(json.loads(output), dict)

            result, _, stderr = _run("--state", str(state), "backup", str(backup))
            self.assertNotEqual(result, 0)
            self.assertTrue(stderr.strip())
            self.assertEqual(backup.read_bytes(), frozen)

    def test_backup_fails_on_missing_state_and_inspect_fails_on_missing_backup(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            result, _, stderr = _run(
                "--state", str(base / "absent"), "backup", str(base / "backup.sqlite3")
            )
            self.assertNotEqual(result, 0)
            self.assertTrue(stderr.strip())
            result, _, stderr = _run("inspect-backup", str(base / "backup.sqlite3"))
            self.assertNotEqual(result, 0)
            self.assertTrue(stderr.strip())


if __name__ == "__main__":
    unittest.main()
