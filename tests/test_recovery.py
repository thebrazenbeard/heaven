"""Disaster-recovery and backend-identity hostile regression tests.

These do not access provider credentials or remote services.  A provider's
opaque locator is treated as part of durable storage identity: silently
repointing an existing backend name can destroy the meaning of the manifest.
"""

from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from heaven.backends import LocalDirectoryBackend, RcloneBackend
from heaven.store import (
    HeavenStore,
    BackendIdentityCollisionError,
    BackendIdentityMismatchError,
    LegacyBackendBindingRequiredError,
)
from heaven.recovery import BackupError, create_backup, inspect_backup


class BackendIdentityRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="heaven-recovery-qa-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "index.sqlite3"
        self.a = LocalDirectoryBackend("account-a", self.root / "account-a", 32)

    def test_rebinding_name_to_other_local_root_is_rejected_without_data_loss(self) -> None:
        payload = b"identity-protected bytes"
        original = HeavenStore(self.db, [self.a], chunk_size=6)
        original.put_file("/protected.bin", io.BytesIO(payload))
        physical_before = sorted(self.a.list_chunk_ids())
        wrong = LocalDirectoryBackend("account-a", self.root / "different-account", 32)
        with self.assertRaises(BackendIdentityMismatchError):
            HeavenStore(self.db, [wrong], chunk_size=6)
        self.assertFalse(wrong.list_chunk_ids(), "rejected backend must stay untouched")
        self.assertEqual(self.a.list_chunk_ids(), physical_before)
        recovered = HeavenStore(self.db, [self.a], chunk_size=6)
        output = io.BytesIO()
        recovered.get_file("/protected.bin", output)
        self.assertEqual(output.getvalue(), payload)
        self.assertTrue(recovered.verify_file("/protected.bin"))

    def test_rebinding_rclone_remote_name_is_rejected_offline(self) -> None:
        old = RcloneBackend("cloud-a", "existing-remote:heaven-one", 100)
        HeavenStore(self.db, [old], chunk_size=5)
        changed = RcloneBackend("cloud-a", "existing-remote:heaven-two", 100)
        with self.assertRaises(BackendIdentityMismatchError):
            HeavenStore(self.db, [changed], chunk_size=5)
        # No actual rclone binary or credentials are needed to detect this.

    def test_adding_another_backend_name_preserves_existing_data(self) -> None:
        original = HeavenStore(self.db, [self.a], chunk_size=5)
        original.put_file("/before.txt", io.BytesIO(b"saved-before-provider-add"))
        second = LocalDirectoryBackend("account-b", self.root / "account-b", 48)
        expanded = HeavenStore(self.db, [self.a, second], chunk_size=5)
        expanded.put_file("/after.txt", io.BytesIO(b"added-provider-can-be-used"))
        self.assertTrue(expanded.verify_file("/before.txt"))
        self.assertTrue(expanded.verify_file("/after.txt"))
        self.assertEqual(
            expanded.stat_file("/before.txt").sha256,
            hashlib.sha256(b"saved-before-provider-add").hexdigest(),
        )

    def test_missing_configured_backend_does_not_discard_manifest(self) -> None:
        second = LocalDirectoryBackend("account-b", self.root / "account-b", 32)
        original = HeavenStore(self.db, [self.a, second], chunk_size=8)
        original.put_file("/split.bin", io.BytesIO(b"0" * 40))
        partial = HeavenStore(self.db, [self.a], chunk_size=8)
        self.assertFalse(partial.verify_file("/split.bin"))
        # Restoring the original provider configuration recovers the same file.
        restored = HeavenStore(self.db, [self.a, second], chunk_size=8)
        self.assertTrue(restored.verify_file("/split.bin"))

    def test_two_backend_names_cannot_alias_one_local_root(self) -> None:
        duplicate = LocalDirectoryBackend("account-b", self.root / "account-a", 32)
        with self.assertRaises(BackendIdentityCollisionError):
            HeavenStore(self.db, [self.a, duplicate], chunk_size=6)

    def test_previous_root_cannot_be_reassigned_under_new_name_when_old_is_absent(self) -> None:
        first = HeavenStore(self.db, [self.a], chunk_size=6)
        first.put_file("/protected.bin", io.BytesIO(b"old-account-custody"))
        renamed = LocalDirectoryBackend("account-b", self.root / "account-a", 32)
        with self.assertRaises(BackendIdentityCollisionError):
            HeavenStore(self.db, [renamed], chunk_size=6)
        self.assertTrue(HeavenStore(self.db, [self.a], chunk_size=6).verify_file("/protected.bin"))

    def test_unbound_legacy_chunk_references_fail_closed(self) -> None:
        original = HeavenStore(self.db, [self.a], chunk_size=6)
        original.put_file("/legacy", io.BytesIO(b"legacy-data"))
        # Simulate opening a pre-identity-binding manifest, which contains
        # chunk-to-backend names but no authenticated locator provenance.
        with sqlite3.connect(self.db) as db:
            db.execute("DELETE FROM backend_bindings")
        with self.assertRaises(LegacyBackendBindingRequiredError):
            HeavenStore(self.db, [self.a], chunk_size=6)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM backend_bindings").fetchone()[0], 0)


class DurableCatalogBackupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="heaven-backup-qa-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "running.sqlite3"
        self.backup = self.root / "snapshot.sqlite3"
        self.backend = LocalDirectoryBackend("primary", self.root / "primary", 256)
        self.store = HeavenStore(self.db, [self.backend], chunk_size=7)

    def test_consistent_sqlite_snapshot_is_inspectable_and_recoverable(self) -> None:
        original = b"critical manifest and physical references"
        self.store.put_file("/before", io.BytesIO(original))
        receipt = create_backup(self.db, self.backup)
        self.assertIsInstance(receipt, dict)
        self.assertTrue(self.backup.exists())
        self.assertTrue((self.root / "snapshot.sqlite3.manifest.json").is_file())
        inspection = inspect_backup(self.backup)
        self.assertIsInstance(inspection, dict)
        recovered = HeavenStore(self.backup, [self.backend], chunk_size=7)
        self.assertTrue(recovered.verify_file("/before"))
        self.assertEqual(recovered.stat_file("/before").sha256, hashlib.sha256(original).hexdigest())
        # A later transaction on live catalog must not appear in the snapshot.
        self.store.put_file("/after", io.BytesIO(b"subsequent write"))
        self.assertRaises(FileNotFoundError, recovered.stat_file, "/after")

    def test_modified_backup_bytes_fail_digest_or_integrity_inspection(self) -> None:
        self.store.put_file("/before", io.BytesIO(b"known good"))
        create_backup(self.db, self.backup)
        original = self.backup.read_bytes()
        self.assertGreater(len(original), 4096)
        self.backup.write_bytes(original[:-1] + bytes([original[-1] ^ 0xA5]))
        with self.assertRaises(BackupError):
            inspect_backup(self.backup)

    def test_backup_refuses_overwrite_and_preserves_existing_destination(self) -> None:
        self.store.put_file("/before", io.BytesIO(b"known good"))
        sentinel = b"existing file that must not be replaced"
        self.backup.write_bytes(sentinel)
        with self.assertRaises((FileExistsError, BackupError)):
            create_backup(self.db, self.backup)
        self.assertEqual(self.backup.read_bytes(), sentinel)

    def test_backup_missing_receipt_fails_closed(self) -> None:
        self.store.put_file("/before", io.BytesIO(b"known good"))
        create_backup(self.db, self.backup)
        receipt_path = self.root / "snapshot.sqlite3.manifest.json"
        receipt_path.unlink()
        with self.assertRaises(BackupError):
            inspect_backup(self.backup)

    def test_recovery_config_sidecar_never_copies_unrecognized_secrets(self) -> None:
        config_path = self.root / "config.json"
        config_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "chunk_size": 7,
                    "oauth_refresh_token": "DO_NOT_COPY_TOP_LEVEL_SECRET",
                    "backends": [
                        {
                            "type": "local",
                            "name": "primary",
                            "root": str(self.root / "primary"),
                            "quota_bytes": 256,
                            "password": "DO_NOT_COPY_BACKEND_SECRET",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        self.store.put_file("/before", io.BytesIO(b"good data"))
        create_backup(self.db, self.backup, config_path=config_path)
        sanitized = (self.root / "snapshot.sqlite3.config.json").read_text(encoding="utf-8")
        self.assertNotIn("DO_NOT_COPY", sanitized)
        self.assertNotIn("oauth_refresh_token", sanitized)
        self.assertNotIn("password", sanitized)
        self.assertTrue(inspect_backup(self.backup)["verified"])


if __name__ == "__main__":
    unittest.main()
