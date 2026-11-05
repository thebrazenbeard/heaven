"""End-to-end CLI smoke tests using disposable local storage only."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class CLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = self.root / "config.json"
        self.config.write_text(
            json.dumps(
                {
                    "database": "index.sqlite3",
                    "chunk_size": 128,
                    "stores": [
                        {"name": "a", "root": "a", "capacity_bytes": 512},
                        {"name": "b", "root": "b", "capacity_bytes": 512},
                    ],
                }
            ),
            encoding="utf-8",
        )
        self.env = os.environ.copy()
        src = str(Path(__file__).resolve().parents[1] / "src")
        self.env["PYTHONPATH"] = src + os.pathsep + self.env.get("PYTHONPATH", "")

    def run_cli(self, *args: str, expect: int = 0) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, "-m", "heaven.cli", "--config", str(self.config), *args],
            capture_output=True,
            text=True,
            env=self.env,
            check=False,
        )
        self.assertEqual(expect, result.returncode, result.stdout + result.stderr)
        return result

    def test_store_verify_restore_and_refuse_implicit_overwrite(self) -> None:
        source = self.root / "source.bin"
        data = bytes(range(256)) * 3
        source.write_bytes(data)
        out = self.root / "output.bin"

        self.run_cli("put", str(source), "nested/name.bin")
        self.assertIn("nested/name.bin", self.run_cli("list").stdout)
        self.run_cli("verify", "nested/name.bin")
        self.run_cli("get", "nested/name.bin", str(out))
        self.assertEqual(data, out.read_bytes())

        out.write_bytes(b"must not silently replace me")
        self.run_cli("get", "nested/name.bin", str(out), expect=1)
        self.assertEqual(b"must not silently replace me", out.read_bytes())
        self.run_cli("get", "nested/name.bin", str(out), "--force")
        self.assertEqual(data, out.read_bytes())
        self.run_cli("delete", "nested/name.bin")
        self.assertNotIn("nested/name.bin", self.run_cli("list").stdout)


if __name__ == "__main__":
    unittest.main()
