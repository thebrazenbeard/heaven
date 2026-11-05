"""Pluggable chunk storage for Heaven.

The local backend is suitable for tests and explicitly provisioned directories.
    The rclone backend uses *existing, authorized* rclone remotes; it neither creates
    accounts nor asks for credentials. Its configured quota is a Heaven allocation,
    not a guarantee about free space in a third-party account or provider charges.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Iterator, Protocol, runtime_checkable


_CHUNK_ID = re.compile(r"[A-Za-z0-9_-]{8,128}\Z")
_REMOTE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*:[^\r\n\x00]*\Z")


class BackendError(RuntimeError):
    """A backend could not complete an operation."""


class InsufficientCapacityError(BackendError):
    """A chunk would exceed this backend's allocated capacity."""


class ChunkConflictError(BackendError):
    """An existing chunk ID contains different data."""


@runtime_checkable
class StorageBackend(Protocol):
    name: str

    def capacity_bytes(self) -> int: ...
    def used_bytes(self) -> int: ...
    def put_chunk(self, chunk_id: str, data: bytes) -> None: ...
    def get_chunk(self, chunk_id: str) -> bytes: ...
    def delete_chunk(self, chunk_id: str) -> None: ...
    def list_chunk_ids(self) -> list[str]: ...


def _check_chunk_id(chunk_id: str) -> str:
    if not isinstance(chunk_id, str) or not _CHUNK_ID.fullmatch(chunk_id):
        raise ValueError("chunk ID must be 8-128 URL-safe ASCII characters")
    return chunk_id


def _check_quota(quota_bytes: int) -> int:
    if isinstance(quota_bytes, bool) or not isinstance(quota_bytes, int) or quota_bytes <= 0:
        raise ValueError("quota_bytes must be a positive integer")
    return quota_bytes


@contextmanager
def _process_lock(path: Path) -> Iterator[None]:
    """Serialize capacity checks for local concurrent processes.

    Network filesystem and administrator-level tampering are not covered by this
    local-host lock; multi-host writers require a stronger custody mechanism.
    """
    with path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _fsync_directory(path: Path) -> None:
    """Best-effort POSIX parent-directory sync after publishing a chunk."""
    if os.name == "nt":
        return
    descriptor: int | None = None
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        os.fsync(descriptor)
    except OSError:
        # Not every mounted filesystem supports fsync on directory descriptors.
        # A durable multi-machine backend requires its own verified write barrier.
        pass
    finally:
        if descriptor is not None:
            os.close(descriptor)


class LocalDirectoryBackend:
    """Atomic, quota-bounded storage rooted in one local directory.

    Capacity checks scan every chunk before writes (O(N) per chunk, O(N^2)
    across a batch). This is an intentionally simple small-scale backend, not a
    scalable 1,000-account allocator. Directory fsync is best effort and actual
    host power-failure durability must be measured for the target filesystem.
    """

    def __init__(self, name: str, root: str | Path, quota_bytes: int):
        if not name or any(x in name for x in ("/", "\\", "\x00")):
            raise ValueError("invalid backend name")
        self.name = name
        self.root = Path(root).expanduser().resolve()
        self._quota = _check_quota(quota_bytes)
        self._lock = RLock()
        self._chunks = self.root / "chunks"
        self._chunks.mkdir(parents=True, exist_ok=True)
        if self._chunks.is_symlink() or self._chunks.resolve() != self._chunks:
            raise BackendError("chunk directory must not be a symlink")

    def capacity_bytes(self) -> int:
        return self._quota

    def _path(self, chunk_id: str) -> Path:
        chunk_id = _check_chunk_id(chunk_id)
        prefix = self._chunks / chunk_id[:2]
        if prefix.is_symlink():
            raise BackendError("chunk path crosses a symlink")
        target = prefix / chunk_id
        if target.is_symlink():
            raise BackendError("chunk path is a symlink")
        return target

    def used_bytes(self) -> int:
        total = 0
        for prefix in self._chunks.iterdir():
            if prefix.is_symlink():
                raise BackendError("chunk prefix is a symlink")
            if not prefix.is_dir():
                continue
            for entry in prefix.iterdir():
                if entry.is_symlink():
                    raise BackendError("chunk entry is a symlink")
                if entry.is_file() and _CHUNK_ID.fullmatch(entry.name):
                    total += entry.stat().st_size
        return total

    def put_chunk(self, chunk_id: str, data: bytes) -> None:
        if not isinstance(data, bytes):
            raise TypeError("chunk payload must be bytes")
        path = self._path(chunk_id)
        with self._lock, _process_lock(self.root / ".heaven.lock"):
            path = self._path(chunk_id)
            if path.exists():
                if path.read_bytes() != data:
                    raise ChunkConflictError("chunk ID already stores different data")
                return
            if self.used_bytes() + len(data) > self._quota:
                raise InsufficientCapacityError(f"backend {self.name} is full")
            path.parent.mkdir(exist_ok=True)
            if path.parent.is_symlink():
                raise BackendError("chunk path crosses a symlink")
            tmp_path: Path | None = None
            try:
                with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".upload-", delete=False) as out:
                    tmp_path = Path(out.name)
                    out.write(data)
                    out.flush()
                    os.fsync(out.fileno())
                try:
                    os.link(tmp_path, path)
                    _fsync_directory(path.parent)
                except FileExistsError:
                    if path.is_symlink() or path.read_bytes() != data:
                        raise ChunkConflictError("chunk ID collision")
            finally:
                if tmp_path is not None:
                    tmp_path.unlink(missing_ok=True)

    def get_chunk(self, chunk_id: str) -> bytes:
        path = self._path(chunk_id)
        try:
            return path.read_bytes()
        except FileNotFoundError:
            raise FileNotFoundError(f"chunk {chunk_id} unavailable on {self.name}") from None

    def delete_chunk(self, chunk_id: str) -> None:
        with self._lock, _process_lock(self.root / ".heaven.lock"):
            self._path(chunk_id).unlink(missing_ok=True)

    def list_chunk_ids(self) -> list[str]:
        ids: list[str] = []
        for prefix in self._chunks.iterdir():
            if prefix.is_symlink():
                raise BackendError("chunk prefix is a symlink")
            if not prefix.is_dir():
                continue
            for entry in prefix.iterdir():
                if entry.is_symlink():
                    raise BackendError("chunk entry is a symlink")
                if entry.is_file() and _CHUNK_ID.fullmatch(entry.name):
                    ids.append(entry.name)
        return sorted(ids)


class RcloneBackend:
    """Chunk storage via an already-configured rclone remote.

    Every operation is a separate rclone invocation. There is no interactive
    login, new account creation, or server start. The rclone binary may load
    credentials from its existing configuration and call a billable provider;
    callers must independently authorize and verify the remote's $0 cost.
    Quotas count Heaven chunks only; other provider usage can still exhaust the
    real account before this allocation is reached. Remote rcat writes are NOT
    atomic: failures may leave partial/orphan chunks, and multiwriter quota
    checks can race. Neither this adapter nor rclone provides cloud persistence
    guarantees, provider ToS permission, erasure coding, or account lifecycle.
    """

    def __init__(self, name: str, remote: str, quota_bytes: int, *, executable: str = "rclone"):
        if not name or any(x in name for x in ("/", "\\", "\x00")):
            raise ValueError("invalid backend name")
        if not isinstance(remote, str) or not _REMOTE.fullmatch(remote):
            raise ValueError("remote must be an existing rclone remote and optional subpath")
        subpath = remote.partition(":")[2]
        if "\\" in subpath or any(segment in (".", "..") for segment in subpath.split("/")):
            raise ValueError("remote subpath must not contain traversal segments")
        if not isinstance(executable, str) or not executable:
            raise ValueError("rclone executable must be configured")
        self.name = name
        self.remote = remote.rstrip("/")
        self._quota = _check_quota(quota_bytes)
        self.executable = executable

    def capacity_bytes(self) -> int:
        return self._quota

    def _prefix(self) -> str:
        return self.remote + "/chunks"

    def _path(self, chunk_id: str) -> str:
        chunk_id = _check_chunk_id(chunk_id)
        return self._prefix() + "/" + chunk_id[:2] + "/" + chunk_id

    def _run(self, operation: str, *args: str, data: bytes | None = None) -> bytes:
        try:
            completed = subprocess.run(
                [self.executable, operation, *args],
                input=data,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=180,
                check=False,
            )
        except (FileNotFoundError, OSError, subprocess.TimeoutExpired) as exc:
            raise BackendError(f"rclone operation {operation} unavailable") from exc
        if completed.returncode:
            # Never surface raw stderr: providers may include private remote paths.
            raise BackendError(f"rclone operation {operation} failed (exit {completed.returncode})")
        return completed.stdout

    def _prefix_exists(self) -> bool:
        """Determine whether the empty chunk namespace is simply not created.

        Inspect one path component at a time, avoiding a false 'empty' result if
        the remote is unreachable or an existing chunk directory is failing.
        """
        alias, _, path = self._prefix().partition(":")
        parent = alias + ":"
        for component in (part for part in path.split("/") if part):
            entries = self._run("lsf", parent, "--max-depth", "1")
            found = any(
                entry.rstrip("/") == component
                for entry in entries.decode("utf-8").splitlines()
            )
            if not found:
                return False
            parent = parent.rstrip("/") + "/" + component
        return True

    def used_bytes(self) -> int:
        try:
            result = self._run("size", self._prefix(), "--json")
        except BackendError:
            # An uninitialized remote namespace has zero Heaven chunks, but a
            # provider outage must never masquerade as empty free capacity.
            if not self._prefix_exists():
                return 0
            raise
        try:
            count = json.loads(result)["bytes"]
            if type(count) is not int or count < 0:
                raise ValueError
            return count
        except (KeyError, ValueError, TypeError) as exc:
            raise BackendError("rclone size returned invalid JSON") from exc

    def put_chunk(self, chunk_id: str, data: bytes) -> None:
        if not isinstance(data, bytes):
            raise TypeError("chunk payload must be bytes")
        # This is not a remotely atomic quota check. A shared, authoritative
        # allocator is required before enabling concurrent multi-host writes.
        if self.used_bytes() + len(data) > self._quota:
            raise InsufficientCapacityError(f"backend {self.name} is full")
        self._run("mkdir", self._prefix())
        self._run("rcat", self._path(chunk_id), data=data)

    def get_chunk(self, chunk_id: str) -> bytes:
        return self._run("cat", self._path(chunk_id))

    def delete_chunk(self, chunk_id: str) -> None:
        self._run("deletefile", self._path(chunk_id))

    def list_chunk_ids(self) -> list[str]:
        listed = self._run("lsf", self._prefix(), "--recursive", "--files-only")
        names = []
        for raw in listed.decode("utf-8").splitlines():
            candidate = raw.rsplit("/", 1)[-1]
            if _CHUNK_ID.fullmatch(candidate):
                names.append(candidate)
        return sorted(names)
