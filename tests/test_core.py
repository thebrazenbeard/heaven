"""Black-box tests for Heaven's local multi-store storage engine.

Run without third-party dependencies:

    python -m unittest discover -s tests -v

These tests intentionally use independent storage roots and a separate manifest
database. A test should fail if the index survives but the stored bytes do not.
"""

from __future__ import annotations

import io
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from heaven import Pool, Store  # noqa: E402


CHUNK = 64 * 1024


def payload(size: int) -> bytes:
    """Repeatable nonuniform content that crosses chunk boundaries."""
    block = bytes((i * 73 + 19) % 256 for i in range(257))
    return (block * ((size // len(block)) + 1))[:size]


class PoolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / "index.sqlite3"
        self.stores = [
            Store("one", self.root / "store-one", 256 * 1024),
            Store("two", self.root / "store-two", 256 * 1024),
        ]
        self.pool = Pool(self.db, self.stores, chunk_size=CHUNK)

    def _chunks(self) -> list[Path]:
        return sorted(
            path
            for store in self.stores
            for path in store.root.rglob("*")
            if path.is_file()
        )

    def test_distributes_and_restores_file_larger_than_any_store(self) -> None:
        data = payload(6 * CHUNK + 17)
        info = self.pool.put("project/artifact.bin", io.BytesIO(data))
        self.assertEqual(info.name, "project/artifact.bin")
        self.assertEqual(info.size, len(data))
        self.assertGreater(info.chunk_count, 1)

        out = io.BytesIO()
        self.pool.get("project/artifact.bin", out)
        self.assertEqual(out.getvalue(), data)
        self.assertTrue(self.pool.verify("project/artifact.bin").ok)
        self.assertTrue(any(path.is_file() for path in self.stores[0].root.rglob("*")))
        self.assertTrue(any(path.is_file() for path in self.stores[1].root.rglob("*")))

    def test_persists_across_pool_reconstruction(self) -> None:
        data = payload(CHUNK * 3 + 7)
        self.pool.put("persistent.dat", io.BytesIO(data))
        reopened = Pool(self.db, self.stores, chunk_size=CHUNK)
        self.assertIn("persistent.dat", [item.name for item in reopened.list_files()])
        output = io.BytesIO()
        reopened.get("persistent.dat", output)
        self.assertEqual(output.getvalue(), data)

    def test_upload_consumes_source_in_bounded_reads(self) -> None:
        class BoundedReader(io.BytesIO):
            def read(self, size: int = -1) -> bytes:
                if size < 0 or size > CHUNK + 1:
                    raise AssertionError("Upload must not read entire source into RAM")
                return super().read(size)

        data = payload(4 * CHUNK + 19)
        self.pool.put("streamed.bin", BoundedReader(data))
        result = io.BytesIO()
        self.pool.get("streamed.bin", result)
        self.assertEqual(result.getvalue(), data)

    def test_empty_file_is_a_valid_persisted_object(self) -> None:
        info = self.pool.put("empty.dat", io.BytesIO())
        self.assertEqual(info.size, 0)
        self.assertEqual(info.chunk_count, 0)
        self.assertTrue(self.pool.verify("empty.dat").ok)
        result = io.BytesIO()
        Pool(self.db, self.stores, chunk_size=CHUNK).get("empty.dat", result)
        self.assertEqual(result.getvalue(), b"")

    def test_midstream_source_failure_rolls_back_manifest_and_chunks(self) -> None:
        class FailingReader(io.BytesIO):
            def __init__(self) -> None:
                super().__init__(payload(2 * CHUNK))
                self.calls = 0

            def read(self, size: int = -1) -> bytes:
                self.calls += 1
                if self.calls == 2:
                    raise OSError("simulated source failure")
                return super().read(size)

        with self.assertRaisesRegex(OSError, "simulated source failure"):
            self.pool.put("interrupted.dat", FailingReader())
        self.assertEqual(self.pool.list_files(), [])
        self.assertEqual(self._chunks(), [])

    def test_failed_object_rename_does_not_publish_manifest(self) -> None:
        with patch("heaven.core.os.replace", side_effect=OSError("simulated disk failure")):
            with self.assertRaisesRegex(OSError, "simulated disk failure"):
                self.pool.put("rename-failed.bin", io.BytesIO(payload(CHUNK)))
        self.assertEqual(self.pool.list_files(), [])
        self.assertEqual(self._chunks(), [])

    def test_database_rejects_manifest_after_chunks_written(self) -> None:
        # An insertion failure occurs after chunk files have been fsynced. The
        # final namespace entry must remain absent, and files must roll back.
        with sqlite3.connect(self.db) as db:
            db.execute(
                "CREATE TRIGGER reject_file BEFORE INSERT ON files "
                "BEGIN SELECT RAISE(ABORT, 'simulated sqlite failure'); END"
            )
        with self.assertRaises(sqlite3.DatabaseError):
            self.pool.put("rejected.bin", io.BytesIO(payload(2 * CHUNK)))
        self.assertEqual(self.pool.list_files(), [])
        self.assertEqual(self._chunks(), [])

    def test_short_writing_sink_is_completed_or_rejected(self) -> None:
        data = payload(2 * CHUNK)
        self.pool.put("short-sink.bin", io.BytesIO(data))

        class ShortWriter(io.BytesIO):
            def write(self, data: bytes) -> int:
                return super().write(data[: max(1, len(data) // 2)])

        destination = ShortWriter()
        try:
            self.pool.get("short-sink.bin", destination)
        except (OSError, ValueError):
            pass  # Rejecting short writes is safe; silently truncating is not.
        else:
            self.assertEqual(destination.getvalue(), data)

    def test_zero_progress_sink_fails_instead_of_hanging(self) -> None:
        self.pool.put("no-progress.bin", io.BytesIO(payload(CHUNK)))

        class ZeroWriter:
            def write(self, data: bytes) -> int:
                return 0

        with self.assertRaises((OSError, ValueError)):
            self.pool.get("no-progress.bin", ZeroWriter())

    def test_detects_corrupted_stored_chunk(self) -> None:
        self.pool.put("integrity.bin", io.BytesIO(payload(CHUNK + 71)))
        chunks = self._chunks()
        self.assertGreater(len(chunks), 0, "The payload must be stored on disk")
        first = chunks[0]
        contents = first.read_bytes()
        self.assertTrue(contents)
        first.write_bytes(bytes([contents[0] ^ 0xFF]) + contents[1:])

        result = self.pool.verify("integrity.bin")
        self.assertFalse(result.ok)
        self.assertTrue(result.problems)
        with self.assertRaises(Exception):
            self.pool.get("integrity.bin", io.BytesIO())

    def test_reports_missing_chunk_without_claiming_healthy(self) -> None:
        self.pool.put("missing.bin", io.BytesIO(payload(CHUNK + 17)))
        chunks = self._chunks()
        self.assertTrue(chunks)
        chunks[0].unlink()
        self.assertFalse(self.pool.verify("missing.bin").ok)
        with self.assertRaises(Exception):
            self.pool.get("missing.bin", io.BytesIO())

    def test_insufficient_capacity_does_not_publish_partial_file(self) -> None:
        small_stores = [
            Store("small-one", self.root / "small-one", CHUNK),
            Store("small-two", self.root / "small-two", CHUNK),
        ]
        pool = Pool(self.root / "small.sqlite3", small_stores, chunk_size=CHUNK)
        with self.assertRaises(Exception):
            pool.put("too-large.bin", io.BytesIO(payload(3 * CHUNK)))
        self.assertEqual(pool.list_files(), [])
        with self.assertRaises(FileNotFoundError):
            pool.get("too-large.bin", io.BytesIO())
        self.assertEqual(
            [p for s in small_stores for p in s.root.rglob("*") if p.is_file()],
            [],
            "Failed writes must not leave orphaned payload chunks",
        )

    def test_rejects_traversal_and_absolute_virtual_paths(self) -> None:
        for name in ("../escape", "/absolute", "nested/../../escape", "", "./relative"):
            with self.subTest(name=name):
                with self.assertRaises((ValueError, OSError)):
                    self.pool.put(name, io.BytesIO(b"test"))
        self.assertEqual(self.pool.list_files(), [])

    def test_duplicate_name_cannot_silently_overwrite(self) -> None:
        original = payload(CHUNK + 1)
        self.pool.put("existing.bin", io.BytesIO(original))
        with self.assertRaises(FileExistsError):
            self.pool.put("existing.bin", io.BytesIO(b"replacement"))
        out = io.BytesIO()
        self.pool.get("existing.bin", out)
        self.assertEqual(out.getvalue(), original)

    def test_delete_removes_manifest_and_releases_capacity(self) -> None:
        original = payload(3 * CHUNK)
        self.pool.put("remove.bin", io.BytesIO(original))
        self.pool.delete("remove.bin")
        self.assertEqual(self.pool.list_files(), [])
        with self.assertRaises(FileNotFoundError):
            self.pool.get("remove.bin", io.BytesIO())
        self.assertEqual(self._chunks(), [], "Deleting a file must release its chunks")
        self.pool.put("reuse.bin", io.BytesIO(original))
        output = io.BytesIO()
        self.pool.get("reuse.bin", output)
        self.assertEqual(output.getvalue(), original)


if __name__ == "__main__":
    unittest.main()
