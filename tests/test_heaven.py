"""Adversarial, provider-free behavioral tests for Heaven's storage core.

These tests intentionally use tiny fake account quotas; they never contact a
cloud provider or use any credentials.  They verify externally visible data
integrity and recovery rather than just testing metadata helper functions.
"""

from __future__ import annotations

import hashlib
import io
import tempfile
import unittest
from pathlib import Path

from heaven.backends import (
    BackendError,
    ChunkConflictError,
    InsufficientCapacityError,
    LocalDirectoryBackend,
)
from heaven.store import HeavenStore


class HeavenStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="heaven-qa-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "catalog.sqlite3"
        self.backends = [
            LocalDirectoryBackend(
                name=f"account-{index}",
                root=self.root / f"account-{index}",
                quota_bytes=16,
            )
            for index in range(3)
        ]
        self.store = HeavenStore(self.db, self.backends, chunk_size=6)

    def _put(self, path: str, data: bytes):
        return self.store.put_file(path if path.startswith("/") else "/" + path, io.BytesIO(data))

    def _read(self, path: str) -> bytes:
        destination = io.BytesIO()
        self.store.get_file(path if path.startswith("/") else "/" + path, destination)
        return destination.getvalue()

    def test_spans_multiple_accounts_and_recovers_exact_bytes(self) -> None:
        data = b"\x00\x01\x02\xffThe quick brown fox jumped!"
        self._put("books/raw.bin", data)
        self.assertEqual(self._read("books/raw.bin"), data)
        self.assertEqual(self.store.stat_file("/books/raw.bin").size, len(data))
        self.assertEqual(
            self.store.stat_file("/books/raw.bin").sha256,
            hashlib.sha256(data).hexdigest(),
        )
        # Capacity (16 bytes per account) forces this 31-byte file to span
        # at least two physical accounts; one account cannot hold the file.
        self.assertGreater(
            sum(bool([p for p in (self.root / f"account-{i}" / "chunks").rglob("*") if p.is_file()]) for i in range(3)),
            1,
        )

    def test_range_reads_are_independent_of_chunk_boundaries(self) -> None:
        data = b"abcdefghijklmnopqrstuvwxyz0123"
        self._put("ranges/test.bin", data)
        for offset, size in [(0, 4), (3, 9), (5, 17), (11, 2), (27, 3), (29, 10)]:
            with self.subTest(offset=offset, size=size):
                self.assertEqual(
                    self.store.read_range("/ranges/test.bin", offset, size),
                    data[offset : offset + size],
                )

    def test_restart_recovers_catalog_and_contents(self) -> None:
        data = b"persistent-data-on-multiple-accounts"
        self._put("checkpoint/file.dat", data)
        self.store.close()
        self.store = HeavenStore(self.db, self.backends, chunk_size=6)
        self.assertEqual(self._read("checkpoint/file.dat"), data)
        self.assertEqual(self.store.verify_file("/checkpoint/file.dat"), True)

    def test_delete_removes_virtual_file(self) -> None:
        self._put("a/b.txt", b"hello world")
        self.assertEqual(self._read("a/b.txt"), b"hello world")
        self.store.delete_file("/a/b.txt")
        self.assertRaises((FileNotFoundError, KeyError), self.store.stat_file, "/a/b.txt")
        self.assertRaises((FileNotFoundError, KeyError), self._read, "a/b.txt")

    def test_account_capacity_rejection_never_exposes_partial_file(self) -> None:
        # 3 x 16 gross bytes of quota; a 49-byte file cannot fit.
        with self.assertRaises(Exception):
            self._put("too-large.dat", b"X" * 49)
        self.assertRaises((FileNotFoundError, KeyError), self.store.stat_file, "/too-large.dat")

    def test_rejects_path_escape_and_malformed_paths(self) -> None:
        # Leading '/' is permitted as a *virtual* namespace root by the CLI;
        # escaping above that root or smuggling Windows separators is not.
        for bad in ("../outside", "/../outside", "a/../../outside", "a/./../outside", "a\\..\\outside", "a\x00b"):
            with self.subTest(path=bad):
                with self.assertRaises((ValueError, PermissionError)):
                    self.store.put_file(bad, io.BytesIO(b"unsafe"))
        self.assertFalse((self.root / "outside").exists())

    def test_corrupted_physical_chunk_is_not_silently_returned(self) -> None:
        self._put("integrity.bin", b"0123456789abcdefghijk")
        chunk_paths = [
            p
            for i in range(3)
            for p in (self.root / f"account-{i}" / "chunks").rglob("*")
            if p.is_file()
        ]
        self.assertTrue(chunk_paths, "expected physical chunk objects")
        original = chunk_paths[0].read_bytes()
        self.assertTrue(original, "expected a nonempty chunk")
        chunk_paths[0].write_bytes(bytes([original[0] ^ 0xFF]) + original[1:])
        with self.assertRaises(Exception):
            self._read("integrity.bin")

    def test_failed_replacement_preserves_previous_version(self) -> None:
        original = b"original durable contents"
        self._put("replace.bin", original)
        # Oversized attempted overwrite must not destroy valid old metadata.
        with self.assertRaises(Exception):
            self._put("replace.bin", b"N" * 100)
        self.assertEqual(self._read("replace.bin"), original)

    def test_postcommit_cleanup_failure_cannot_destroy_new_version(self) -> None:
        class FailingGcBackend(LocalDirectoryBackend):
            fail_delete = False

            def delete_chunk(self, chunk_id: str) -> None:
                if self.fail_delete:
                    raise BackendError("simulated provider outage during stale chunk GC")
                return super().delete_chunk(chunk_id)

        backend = FailingGcBackend("fragile", self.root / "fragile", 128)
        store = HeavenStore(self.root / "gc.sqlite3", [backend], chunk_size=5)
        store.put_file("/file.bin", io.BytesIO(b"OLD-immutable"))
        backend.fail_delete = True
        # The SQLite manifest has already committed the replacement before
        # cleanup of the old payloads. GC outages may leak orphans, but must
        # *never* delete the newly committed staged bytes.
        store.put_file("/file.bin", io.BytesIO(b"NEW-version-remains-available"))
        output = io.BytesIO()
        store.get_file("/file.bin", output)
        self.assertEqual(output.getvalue(), b"NEW-version-remains-available")
        self.assertTrue(store.verify_file("/file.bin"))

    def test_verify_returns_false_on_backend_unavailability(self) -> None:
        class OutageBackend(LocalDirectoryBackend):
            unavailable = False

            def get_chunk(self, chunk_id: str) -> bytes:
                if self.unavailable:
                    raise BackendError("simulated provider outage")
                return super().get_chunk(chunk_id)

        backend = OutageBackend("outage", self.root / "outage", 128)
        store = HeavenStore(self.root / "outage.sqlite3", [backend], chunk_size=5)
        store.put_file("/file.bin", io.BytesIO(b"known-good"))
        backend.unavailable = True
        self.assertFalse(store.verify_file("/file.bin"))


class LocalBackendHostileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="heaven-backend-qa-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.backend = LocalDirectoryBackend("safe", self.root / "chunks-backend", 16)

    def test_respects_hard_quota_even_after_restart(self) -> None:
        self.backend.put_chunk("00000001", b"A" * 10)
        restarted = LocalDirectoryBackend("safe", self.root / "chunks-backend", 16)
        self.assertEqual(restarted.used_bytes(), 10)
        with self.assertRaises(InsufficientCapacityError):
            restarted.put_chunk("00000002", b"B" * 7)
        self.assertEqual(restarted.get_chunk("00000001"), b"A" * 10)
        self.assertEqual(restarted.used_bytes(), 10)

    def test_existing_chunk_id_cannot_be_overwritten_with_different_bytes(self) -> None:
        self.backend.put_chunk("00000001", b"immutable")
        with self.assertRaises(ChunkConflictError):
            self.backend.put_chunk("00000001", b"different")
        self.assertEqual(self.backend.get_chunk("00000001"), b"immutable")

    def test_invalid_chunk_ids_never_escape_backend_root(self) -> None:
        for bad in ("../escape", "../../escape", "/escape", "a\\..\\b", "", "abc", "a\x00b"):
            with self.subTest(chunk=bad):
                with self.assertRaises(ValueError):
                    self.backend.put_chunk(bad, b"unsafe")
        self.assertFalse((self.root / "escape").exists())

    def test_symlinked_chunk_prefix_is_not_followed(self) -> None:
        outside = self.root / "outside"
        outside.mkdir()
        trap = self.root / "chunks-backend" / "chunks" / "aa"
        try:
            trap.symlink_to(outside, target_is_directory=True)
        except (NotImplementedError, OSError):
            self.skipTest("symlink creation unavailable on this platform")
        with self.assertRaises(Exception):
            self.backend.put_chunk("aa000001", b"unsafe")
        self.assertFalse((outside / "aa000001").exists())


if __name__ == "__main__":
    unittest.main()
