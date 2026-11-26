"""Non-destructive, verifiable snapshots of Heaven's SQLite metadata catalog.

The source database is captured with SQLite's online backup API (including
committed WAL data), never by copying an active ``.sqlite3`` file. Chunk bytes
remain in their respective backends; this is a *catalog* backup, not a backup
of the stored files. For full recovery, retain the separately managed backend
accounts, chunk objects, and the non-secret configuration sidecar.

Receipts detect accidental corruption. A digest in a mutable receipt cannot
prove authenticity against an attacker able to replace the backup and receipt.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any


class BackupError(ValueError):
    """A backup path, SQLite catalog, or verification receipt is invalid."""


_VERSION = 1
_TABLES = frozenset({"files", "chunks"})


def _exists(path: Path) -> bool:
    # Path.exists() alone ignores a dangling symlink, which must be protected.
    return path.exists() or path.is_symlink()


def _suffix_path(destination: Path, suffix: str) -> Path:
    return destination.with_name(destination.name + suffix)


def _hash_file(path: Path) -> tuple[str, int]:
    sha256 = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            sha256.update(block)
            size += len(block)
    return sha256.hexdigest(), size


def _sqlite_uri(path: Path) -> str:
    return path.resolve().as_uri() + "?mode=ro"


def _assert_catalog(connection: sqlite3.Connection) -> None:
    check = connection.execute("PRAGMA integrity_check").fetchone()
    if check is None or check[0] != "ok":
        raise BackupError(f"SQLite integrity_check failed: {check!r}")
    tables = {
        name for (name,) in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('files','chunks')"
        )
    }
    if tables != _TABLES:
        raise BackupError("SQLite database is not a Heaven catalog (files/chunks tables missing)")


def _new_temp(destination: Path, *, prefix: str) -> Path:
    handle, name = tempfile.mkstemp(prefix=prefix, dir=destination.parent)
    if hasattr(os, "fchmod"):
        os.fchmod(handle, 0o600)
    os.close(handle)
    return Path(name)


def _write_secure_json(path: Path, value: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as output:
        json.dump(value, output, indent=2, sort_keys=True, ensure_ascii=False)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    os.chmod(path, 0o600)


def _fsync_file(path: Path) -> None:
    with path.open("rb") as source:
        os.fsync(source.fileno())


def _fsync_dir(path: Path) -> None:
    # POSIX durability; Windows does not expose directory fsync consistently.
    if not hasattr(os, "O_DIRECTORY"):
        return
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _public_config(config_path: Path) -> dict[str, Any]:
    """Copy only a whitelisted recovery configuration, never extra secrets."""
    try:
        source = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackupError(f"cannot read recovery config: {exc}") from exc
    if not isinstance(source, dict) or source.get("version") != 1:
        raise BackupError("expected Heaven config version 1")
    if not isinstance(source.get("backends"), list):
        raise BackupError("invalid backend catalog")
    chunk_size = source.get("chunk_size")
    if type(chunk_size) is not int or chunk_size <= 0:
        raise BackupError("invalid chunk_size")
    sanitized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in source["backends"]:
        if not isinstance(item, dict):
            raise BackupError("invalid backend entry")
        kind, name, quota = item.get("type"), item.get("name"), item.get("quota_bytes")
        if kind not in ("local", "rclone") or not isinstance(name, str) or not name or name in seen:
            raise BackupError("invalid or duplicate backend identity")
        if type(quota) is not int or quota <= 0:
            raise BackupError("invalid backend quota")
        seen.add(name)
        key = "root" if kind == "local" else "remote"
        value = item.get(key)
        if not isinstance(value, str) or not value:
            raise BackupError(f"backend {name!r} lacks {key}")
        if kind == "rclone" and not re.fullmatch(r"[A-Za-z0-9_-]+:[A-Za-z0-9_./ -]*", value):
            # A rclone backend should identify a preconfigured remote only,
            # never embed URL query strings, tokens or inline userinfo.
            raise BackupError("unsafe rclone remote reference; no credential-bearing URLs in backup")
        sanitized.append({"type": kind, "name": name, "quota_bytes": quota, key: value})
    return {"version": 1, "chunk_size": chunk_size, "backends": sanitized}


def _assert_backend_references(db_path: Path, config: dict[str, Any]) -> None:
    """Ensure the recovery bundle names every backend used by chunk records."""
    names = {item["name"] for item in config["backends"]}
    with sqlite3.connect(_sqlite_uri(db_path), uri=True, timeout=30.0) as db:
        referenced = {name for (name,) in db.execute("SELECT DISTINCT backend FROM chunks")}
    missing = referenced - names
    if missing:
        raise BackupError(
            "recovery config lacks referenced backend identity: " + ", ".join(sorted(missing))
        )


def create_backup(
    source_db: str | Path,
    destination: str | Path,
    *,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Online-backup catalog to a **new** path with checksums and receipt.

    None of ``destination``, ``destination.manifest.json`` or the optional
    ``destination.config.json`` may exist, even as dangling symlinks. The
    caller must choose a parent directory that already exists. No restore,
    overwrite, account mutation, or chunk deletion is performed.
    """
    source_path = Path(source_db).expanduser().resolve()
    dest = Path(destination).expanduser().absolute()
    receipt_path = _suffix_path(dest, ".manifest.json")
    config_copy_path = _suffix_path(dest, ".config.json")
    outputs = [dest, receipt_path]
    if config_path is not None:
        outputs.append(config_copy_path)
    if not source_path.is_file():
        raise BackupError(f"catalog source missing: {source_path}")
    if not dest.parent.is_dir():
        raise BackupError(f"destination parent missing: {dest.parent}")
    if any(_exists(item) for item in outputs):
        raise BackupError("backup destination or sidecar already exists; refusing overwrite")

    temporary: list[Path] = []
    backup_temp = _new_temp(dest, prefix=".heaven-sqlite-")
    temporary.append(backup_temp)
    try:
        with sqlite3.connect(_sqlite_uri(source_path), uri=True, timeout=30.0) as source:
            _assert_catalog(source)
            with sqlite3.connect(str(backup_temp), timeout=30.0) as target:
                source.backup(target, pages=256, sleep=0.1)
                _assert_catalog(target)
        _fsync_file(backup_temp)
        digest, size = _hash_file(backup_temp)

        config_info: dict[str, Any] | None = None
        config_temp: Path | None = None
        if config_path is not None:
            sanitized = _public_config(Path(config_path).expanduser().resolve())
            _assert_backend_references(backup_temp, sanitized)
            config_temp = _new_temp(dest, prefix=".heaven-config-")
            temporary.append(config_temp)
            _write_secure_json(config_temp, sanitized)
            config_hash, config_size = _hash_file(config_temp)
            config_info = {
                "name": config_copy_path.name,
                "size_bytes": config_size,
                "sha256": config_hash,
            }

        receipt: dict[str, Any] = {
            "schema_version": _VERSION,
            "kind": "heaven-catalog-backup",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "backup": {"name": dest.name, "size_bytes": size, "sha256": digest},
            "config": config_info,
            "scope": "SQLite file/chunk metadata only; chunk payloads are not backed up",
            "integrity_check": "ok",
            "trust": "self-contained SHA-256 detects corruption, not malicious coordinated modification",
        }
        receipt_temp = _new_temp(dest, prefix=".heaven-receipt-")
        temporary.append(receipt_temp)
        _write_secure_json(receipt_temp, receipt)

        # Hard-link stages into their final names with atomic no-clobber
        # semantics. The receipt goes last: its presence means the whole
        # bundle reached the publication frontier. A crash after linking the
        # backup but before the receipt can leave an orphan final file; that
        # file is deliberately never overwritten or removed automatically.
        os.link(backup_temp, dest)
        if config_temp is not None:
            os.link(config_temp, config_copy_path)
        os.link(receipt_temp, receipt_path)
        _fsync_dir(dest.parent)
        return receipt
    finally:
        for item in temporary:
            try:
                item.unlink()
            except FileNotFoundError:
                pass


def inspect_backup(path: str | Path) -> dict[str, Any]:
    """Read-only verification of a catalog bundle's receipt, hashes and SQL."""
    backup = Path(path).expanduser().absolute()
    manifest = _suffix_path(backup, ".manifest.json")
    if not backup.is_file() or not manifest.is_file():
        raise BackupError("backup and its manifest receipt must both exist")
    if backup.is_symlink() or manifest.is_symlink():
        raise BackupError("symlinked backup paths are not accepted")
    try:
        receipt = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackupError(f"invalid backup receipt: {exc}") from exc
    if not isinstance(receipt, dict) or receipt.get("schema_version") != _VERSION:
        raise BackupError("unknown backup receipt schema")
    if receipt.get("kind") != "heaven-catalog-backup":
        raise BackupError("not a Heaven catalog backup receipt")
    file_info = receipt.get("backup")
    if not isinstance(file_info, dict) or file_info.get("name") != backup.name:
        raise BackupError("receipt backup filename does not match requested path")
    digest, size = _hash_file(backup)
    if digest != file_info.get("sha256") or size != file_info.get("size_bytes"):
        raise BackupError("backup checksum or length mismatch")
    config_info = receipt.get("config")
    if config_info is not None:
        config = _suffix_path(backup, ".config.json")
        if config.is_symlink() or not config.is_file():
            raise BackupError("recovery configuration sidecar missing or symlinked")
        if not isinstance(config_info, dict) or config_info.get("name") != config.name:
            raise BackupError("receipt config filename mismatch")
        config_hash, config_size = _hash_file(config)
        if config_hash != config_info.get("sha256") or config_size != config_info.get("size_bytes"):
            raise BackupError("configuration checksum or length mismatch")
        sanitized = _public_config(config)
        _assert_backend_references(backup, sanitized)
    try:
        with sqlite3.connect(_sqlite_uri(backup), uri=True, timeout=30.0) as db:
            _assert_catalog(db)
    except sqlite3.DatabaseError as exc:
        raise BackupError(f"backup SQLite verification failed: {exc}") from exc
    return {**receipt, "verified": True}


# Compatibility for callers using the more explicit function name.
backup_catalog = create_backup
