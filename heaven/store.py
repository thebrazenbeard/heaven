"""Durable content-address-verified virtual files over independent chunk backends.

Only the metadata/index resides in SQLite. Chunk payloads remain in separately
configured storage backends. A successful SQLite commit is the visibility point
for an upload, including replacement uploads.

This package is a local/prototype storage engine, not a proof that a collection
of free Google accounts is a reliable or permitted permanent storage service.
"""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator, Sequence

from .backends import BackendError, InsufficientCapacityError, StorageBackend


DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024


class IntegrityError(IOError):
    """A stored chunk or reconstructed file differs from its recorded digest."""


class MissingBackendError(IOError):
    """The metadata references an unconfigured backend."""


class BackendIdentityMismatchError(BackendError):
    """A configured backend name now resolves to a different declared location."""


class BackendIdentityCollisionError(BackendError):
    """Two backend names resolve to the same declared physical location."""


class LegacyBackendBindingRequiredError(BackendError):
    """Existing chunks predate locator pinning and require explicit migration."""


@dataclass(frozen=True)
class FileInfo:
    path: str
    size: int
    sha256: str
    mtime: float


@dataclass(frozen=True)
class _Chunk:
    seq: int
    backend: str
    chunk_id: str
    size: int
    sha256: str


def _canonical_path(path: str) -> str:
    """Keep one unambiguous key per file and refuse traversal-like input."""
    if not isinstance(path, str) or not path or "\\" in path or "\x00" in path:
        raise ValueError("invalid virtual file path")
    if not path.startswith("/") or path == "/" or path.endswith("/"):
        raise ValueError("file paths must be absolute and name a file")
    segments = path[1:].split("/")
    if any(part in ("", ".", "..") for part in segments):
        raise ValueError("empty or traversal path segment")
    if any(any(ord(ch) < 32 or ord(ch) == 127 for ch in part) for part in segments):
        raise ValueError("control characters forbidden in paths")
    return path


def _prefix(prefix: str) -> str:
    if not isinstance(prefix, str) or not prefix.startswith("/"):
        raise ValueError("prefix must start with /")
    if prefix == "/":
        return "/"
    # Directory queries conventionally use '/dir/' but a file-prefix query is
    # also useful. Explicitly reject traversal and ambiguous empty components.
    _canonical_path(prefix.rstrip("/"))
    return prefix


class HeavenStore:
    """Expose many chunk stores as one versioned virtual file namespace.

    Parameters
    ----------
    db_path:
        Host-owned durable SQLite manifest path. Keep this and independent
        recovery copies safe: without the manifest, chunks are not discoverable.
    backends:
        Distinct named providers, each reporting finite quota/usage and able to
        put/get/delete random opaque chunk identifiers.
    chunk_size:
        Upper bound on each chunk. Chunks shrink to fit residual provider space.

    A process-local lock serializes mutations in this instance. Multiple
    independent instances/processes should be externally coordinated for writes
    and garbage collection; SQLite protects metadata, not provider operations.
    """

    def __init__(
        self,
        db_path: str | Path,
        backends: Sequence[StorageBackend],
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer")
        if not backends:
            raise ValueError("at least one storage backend is required")
        self.backends = {backend.name: backend for backend in backends}
        if len(self.backends) != len(backends) or any(not name for name in self.backends):
            raise ValueError("backend names must be unique and nonempty")
        self.db_path = str(db_path)
        self.chunk_size = chunk_size
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as db:
            db.execute("PRAGMA journal_mode = WAL")
            db.execute("PRAGMA synchronous = FULL")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS files (
                    path TEXT PRIMARY KEY,
                    size INTEGER NOT NULL CHECK(size >= 0),
                    sha256 TEXT NOT NULL,
                    mtime REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chunks (
                    path TEXT NOT NULL REFERENCES files(path) ON DELETE CASCADE,
                    seq INTEGER NOT NULL CHECK(seq >= 0),
                    backend TEXT NOT NULL,
                    chunk_id TEXT NOT NULL,
                    size INTEGER NOT NULL CHECK(size > 0),
                    sha256 TEXT NOT NULL,
                    PRIMARY KEY(path, seq),
                    UNIQUE(backend, chunk_id)
                );
                CREATE INDEX IF NOT EXISTS chunks_by_backend ON chunks(backend);
                CREATE TABLE IF NOT EXISTS backend_bindings (
                    name TEXT PRIMARY KEY,
                    locator_sha256 TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                """
            )
            # Pin declared routes in a SQLite write transaction. This protects
            # against accidentally remapping account 'A' onto another root when
            # a config is edited. It is NOT attestation of a Google account:
            # existing rclone aliases may be rebound outside Heaven's control.
            db.execute("BEGIN IMMEDIATE")
            known = {
                str(row["name"]): str(row["locator_sha256"])
                for row in db.execute("SELECT name, locator_sha256 FROM backend_bindings")
            }
            referenced = {
                str(row["backend"])
                for row in db.execute("SELECT DISTINCT backend FROM chunks")
            }
            pending: list[tuple[str, str, float]] = []
            locations: dict[str, str] = {}
            for old_name, old_digest in known.items():
                previously_assigned = locations.setdefault(old_digest, old_name)
                if previously_assigned != old_name:
                    raise BackendIdentityCollisionError(
                        "backend identity table contains duplicate physical locations"
                    )
            for name, backend in self.backends.items():
                locator = backend.identity_locator()
                if not isinstance(locator, str) or not locator:
                    raise BackendIdentityMismatchError(f"backend {name!r} has no stable identity locator")
                fingerprint = hashlib.sha256(locator.encode("utf-8")).hexdigest()
                previously_assigned = locations.get(fingerprint)
                if previously_assigned is not None and previously_assigned != name:
                    raise BackendIdentityCollisionError(
                        f"backend {name!r} duplicates storage location assigned to {previously_assigned!r}"
                    )
                locations[fingerprint] = name
                previous = known.get(name)
                if previous is None:
                    if name in referenced:
                        # An 0.1 manifest has no trustworthy record of which
                        # physical root originally held these chunks. Reusing
                        # a same-named config without an explicit verified
                        # adoption could silently point to unrelated data.
                        raise LegacyBackendBindingRequiredError(
                            f"backend {name!r} has unbound legacy chunks; manual migration required"
                        )
                    pending.append((name, fingerprint, time.time()))
                elif previous != fingerprint:
                    raise BackendIdentityMismatchError(
                        f"backend {name!r} physical locator changed; refusing to remap existing data"
                    )
            if pending:
                db.executemany(
                    "INSERT INTO backend_bindings(name, locator_sha256, created_at) VALUES(?,?,?)",
                    pending,
                )

    def close(self) -> None:
        """Compatibility with context-managed callers; connections are per operation."""
        return None

    def _available_snapshot(self) -> dict[str, int]:
        """Read quotas once per upload rather than querying N remotes per chunk."""
        capacities: dict[str, int] = {}
        for name, backend in self.backends.items():
            available = max(0, backend.capacity_bytes() - backend.used_bytes())
            capacities[name] = available
        return capacities

    def _select_backend(self, capacities: dict[str, int]) -> tuple[StorageBackend, int]:
        """Best remaining capacity, with provider name as stable tie-breaker."""
        candidates: list[tuple[int, str]] = [
            (available, name) for name, available in capacities.items() if available > 0
        ]
        if not candidates:
            raise InsufficientCapacityError("the federated pool is full")
        candidates.sort(key=lambda item: (-item[0], item[1]))
        return self.backends[candidates[0][1]], candidates[0][0]

    def put_file(self, path: str, stream: BinaryIO) -> FileInfo:
        """Stream one file into the pool, then atomically publish its manifest.

        On failed uploads, the prior version remains readable. Prepared chunks
        are best-effort removed. Upload retries should retain their input source.
        """
        path = _canonical_path(path)
        if not hasattr(stream, "read"):
            raise TypeError("source must have a binary read method")
        with self._lock:
            staged: list[_Chunk] = []
            total_size = 0
            full_digest = hashlib.sha256()
            try:
                available = self._available_snapshot()
                while True:
                    data = stream.read(self.chunk_size)
                    if not data:
                        break
                    if not isinstance(data, bytes):
                        raise TypeError("source must yield bytes")
                    full_digest.update(data)
                    total_size += len(data)
                    offset = 0
                    while offset < len(data):
                        backend, capacity = self._select_backend(available)
                        payload = data[offset : offset + min(capacity, len(data) - offset)]
                        key = secrets.token_hex(16)
                        digest = hashlib.sha256(payload).hexdigest()
                        try:
                            backend.put_chunk(key, payload)
                        except InsufficientCapacityError:
                            # Another process may have consumed the observed
                            # quota. Try a different account, not the stale
                            # available capacity on this one.
                            available[backend.name] = 0
                            continue
                        staged.append(_Chunk(len(staged), backend.name, key, len(payload), digest))
                        available[backend.name] -= len(payload)
                        offset += len(payload)

                info = FileInfo(path, total_size, full_digest.hexdigest(), time.time())
                old_chunks: list[_Chunk] = []
                with self._connect() as db:
                    db.execute("BEGIN IMMEDIATE")
                    old_chunks = self._chunks_for(db, path)
                    db.execute("DELETE FROM files WHERE path = ?", (path,))
                    db.execute(
                        "INSERT INTO files(path, size, sha256, mtime) VALUES(?,?,?,?)",
                        (info.path, info.size, info.sha256, info.mtime),
                    )
                    db.executemany(
                        "INSERT INTO chunks(path, seq, backend, chunk_id, size, sha256) VALUES(?,?,?,?,?,?)",
                        [(path, c.seq, c.backend, c.chunk_id, c.size, c.sha256) for c in staged],
                    )
            except BaseException:
                self._delete_payloads(staged)
                raise
            # This is now a committed version. A cleanup outage can leak old
            # chunk objects, but must never roll back visibility or delete the
            # newly committed chunks.
            self._delete_payloads(old_chunks)
            return info

    def _chunks_for(self, db: sqlite3.Connection, path: str) -> list[_Chunk]:
        rows = db.execute(
            "SELECT seq, backend, chunk_id, size, sha256 FROM chunks WHERE path = ? ORDER BY seq",
            (path,),
        ).fetchall()
        return [_Chunk(int(r["seq"]), str(r["backend"]), str(r["chunk_id"]), int(r["size"]), str(r["sha256"])) for r in rows]

    def _snapshot(self, path: str) -> tuple[FileInfo, list[_Chunk]]:
        path = _canonical_path(path)
        with self._connect() as db:
            row = db.execute(
                "SELECT path,size,sha256,mtime FROM files WHERE path = ?", (path,)
            ).fetchone()
            if row is None:
                raise FileNotFoundError(path)
            info = FileInfo(str(row["path"]), int(row["size"]), str(row["sha256"]), float(row["mtime"]))
            return info, self._chunks_for(db, path)

    def _read_chunk(self, chunk: _Chunk) -> bytes:
        backend = self.backends.get(chunk.backend)
        if backend is None:
            raise MissingBackendError(f"backend {chunk.backend!r} is not configured")
        try:
            data = backend.get_chunk(chunk.chunk_id)
        except (FileNotFoundError, KeyError, BackendError) as exc:
            raise IntegrityError(f"missing chunk {chunk.chunk_id} on {chunk.backend}") from exc
        if len(data) != chunk.size or hashlib.sha256(data).hexdigest() != chunk.sha256:
            raise IntegrityError(f"corrupt chunk {chunk.chunk_id} on {chunk.backend}")
        return data

    def iter_file(self, path: str) -> Iterator[bytes]:
        """Yield verified chunks and verify the reconstructed file on completion."""
        # The lock protects same-instance replacement/deletion for the full
        # lifespan of this iterator. Callers should exhaust/close the iterator.
        with self._lock:
            info, chunks = self._snapshot(path)
            digest = hashlib.sha256()
            total = 0
            for chunk in chunks:
                data = self._read_chunk(chunk)
                total += len(data)
                digest.update(data)
                yield data
            if total != info.size or digest.hexdigest() != info.sha256:
                raise IntegrityError(f"file digest mismatch: {info.path}")

    def get_file(self, path: str, sink: BinaryIO) -> FileInfo:
        """Write reconstructed bytes to a binary stream, returning metadata."""
        if not hasattr(sink, "write"):
            raise TypeError("sink must have a binary write method")
        # Fetch immutable metadata before streaming; cross-process writers need
        # coordination to preserve snapshot consistency.
        with self._lock:
            info = self.stat_file(path)
            for data in self.iter_file(path):
                sink.write(data)
            return info

    def iter_range(self, path: str, start: int, length: int) -> Iterator[bytes]:
        """Stream only intersecting, independently verified chunks.

        `length` is a byte count, not an inclusive end offset. Range reads do
        not verify the whole-file SHA256, because they need not fetch every chunk.
        """
        if (type(start) is not int or type(length) is not int or start < 0 or length < 0):
            raise ValueError("range start and length must be nonnegative integers")
        with self._lock:
            info, chunks = self._snapshot(path)
            if start > info.size:
                raise ValueError("range starts beyond file size")
            if length == 0 or start == info.size:
                return
            end = min(info.size, start + length)
            pos = 0
            returned = 0
            for chunk in chunks:
                chunk_start, chunk_end = pos, pos + chunk.size
                pos = chunk_end
                if chunk_end <= start:
                    continue
                if chunk_start >= end:
                    break
                payload = self._read_chunk(chunk)
                section = payload[max(0, start - chunk_start) : min(chunk.size, end - chunk_start)]
                returned += len(section)
                if section:
                    yield section
            if returned != end - start:
                raise IntegrityError("range coverage does not match metadata")

    def read_range(self, path: str, start: int, length: int) -> bytes:
        """Return a byte range in memory; prefer iter_range for large requests."""
        return b"".join(self.iter_range(path, start, length))

    def stat_file(self, path: str) -> FileInfo:
        return self._snapshot(path)[0]

    stat = stat_file

    def list_files(self, prefix: str = "/") -> list[FileInfo]:
        prefix = _prefix(prefix)
        with self._connect() as db:
            # Prefix is filtered in Python to avoid SQL LIKE's wildcard and
            # escaping semantics; a filename may legitimately contain '%'.
            rows = db.execute("SELECT path,size,sha256,mtime FROM files ORDER BY path").fetchall()
            return [
                FileInfo(str(r["path"]), int(r["size"]), str(r["sha256"]), float(r["mtime"]))
                for r in rows if str(r["path"]).startswith(prefix)
            ]

    def delete_file(self, path: str) -> None:
        path = _canonical_path(path)
        with self._lock:
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                chunks = self._chunks_for(db, path)
                result = db.execute("DELETE FROM files WHERE path = ?", (path,))
                if result.rowcount == 0:
                    raise FileNotFoundError(path)
            self._delete_payloads(chunks)

    def verify_file(self, path: str) -> bool:
        """Fully read and validate all chunks; false on missing/corrupt chunks."""
        try:
            for _ in self.iter_file(path):
                pass
            return True
        except (IntegrityError, MissingBackendError, FileNotFoundError, OSError, BackendError):
            return False

    def _delete_payloads(self, chunks: Sequence[_Chunk]) -> None:
        # Best effort: metadata is authoritative; outages may create orphans.
        # Do not delete unknown providers' data by name coincidence.
        for chunk in chunks:
            backend = self.backends.get(chunk.backend)
            if backend is None:
                continue
            try:
                backend.delete_chunk(chunk.chunk_id)
            except (OSError, KeyError, BackendError):
                pass


__all__ = [
    "HeavenStore", "FileInfo", "IntegrityError", "MissingBackendError",
    "BackendIdentityMismatchError", "BackendIdentityCollisionError",
    "LegacyBackendBindingRequiredError", "InsufficientCapacityError",
]
