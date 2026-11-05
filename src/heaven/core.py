"""Small, dependency-free, chunked storage pool with a durable SQLite index.

Objects are written under opaque UUID names, never under user-supplied paths.
The index is committed only after every data chunk has been written, renamed,
and fsynced. A power loss can leave *unreferenced* chunks, but should never
expose a manifest that points to partially written chunks on filesystems that
honor fsync and atomic same-directory rename.

No encryption, replication, or cloud-provider adapter is implemented yet.
"""

from __future__ import annotations

import errno
import hashlib
import os
import re
import sqlite3
import uuid
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO


MAX_CHUNK_BYTES = 64 * 1024 * 1024
_STORE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_OBJECT_ID = re.compile(r"[0-9a-f]{32}\Z")


class CapacityError(OSError):
    """No configured storage root has space for the next byte."""


class IntegrityError(OSError):
    """A stored chunk or file failed its integrity check."""


@dataclass(frozen=True)
class Store:
    name: str
    root: Path
    capacity_bytes: int

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _STORE_NAME.fullmatch(self.name):
            raise ValueError("store names must contain 1–64 simple ASCII characters")
        if not isinstance(self.capacity_bytes, int) or self.capacity_bytes < 1:
            raise ValueError("store capacity_bytes must be a positive integer")
        object.__setattr__(self, "root", Path(self.root).resolve())


@dataclass(frozen=True)
class FileInfo:
    name: str
    size: int
    sha256: str
    chunk_count: int


@dataclass(frozen=True)
class Verification:
    ok: bool
    problems: list[str]


def _check_name(name: str) -> None:
    if (
        not isinstance(name, str)
        or not name
        or name.startswith("/")
        or "\\" in name
        or any(ord(c) < 32 or ord(c) == 127 for c in name)
        or any(part in ("", ".", "..") for part in name.split("/"))
    ):
        raise ValueError("file name must be a clean, relative POSIX path")


def _sync_directory(directory: Path) -> None:
    """Flush directory entry changes where the host supports it."""
    if os.name == "nt":
        # Windows does not expose a portable directory fsync operation.
        return
    try:
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        if exc.errno not in (errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP):
            # Do not publish a manifest when the durable directory flush
            # genuinely failed (e.g. EIO or ENOSPC).
            raise


class Pool:
    """Aggregate several local storage roots into a single path namespace.

    Configured capacities are logical quotas for Heaven's stored chunks, not
    a measurement of actual disk free space. Concurrent writes to the same
    index are serialized with SQLite BEGIN IMMEDIATE.
    """

    def __init__(
        self,
        db_path: Path,
        stores: list[Store],
        chunk_size: int = 4 * 1024 * 1024,
    ) -> None:
        if not isinstance(chunk_size, int) or not 1 <= chunk_size <= MAX_CHUNK_BYTES:
            raise ValueError(f"chunk_size must be between 1 and {MAX_CHUNK_BYTES}")
        if not stores:
            raise ValueError("at least one store is required")
        names = [store.name for store in stores]
        if len(names) != len(set(names)):
            raise ValueError("store names must be unique")
        roots = [store.root for store in stores]
        if len(roots) != len(set(roots)):
            raise ValueError("store roots must be unique")
        self.db_path = Path(db_path).resolve()
        self.stores = {store.name: store for store in stores}
        self.chunk_size = chunk_size
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        for store in stores:
            objects = store.root / ".heaven_objects"
            if objects.is_symlink():
                raise ValueError("storage object directory cannot be a symlink")
            objects.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS files (
                    name TEXT PRIMARY KEY,
                    size INTEGER NOT NULL CHECK (size >= 0),
                    sha256 TEXT NOT NULL,
                    chunk_count INTEGER NOT NULL CHECK (chunk_count >= 0)
                );
                CREATE TABLE IF NOT EXISTS chunks (
                    file_name TEXT NOT NULL REFERENCES files(name) ON DELETE CASCADE,
                    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
                    store_name TEXT NOT NULL,
                    object_id TEXT NOT NULL UNIQUE,
                    size INTEGER NOT NULL CHECK (size > 0),
                    sha256 TEXT NOT NULL,
                    PRIMARY KEY (file_name, chunk_index)
                );
                CREATE INDEX IF NOT EXISTS chunks_store ON chunks(store_name);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def _object_path(self, store_name: str, object_id: str) -> Path:
        if store_name not in self.stores or not _OBJECT_ID.fullmatch(object_id):
            raise IntegrityError("manifest references an invalid store or object")
        return self.stores[store_name].root / ".heaven_objects" / f"{object_id}.chunk"

    @staticmethod
    def _lookup(db: sqlite3.Connection, name: str) -> FileInfo:
        row = db.execute(
            "SELECT name, size, sha256, chunk_count FROM files WHERE name = ?", (name,)
        ).fetchone()
        if row is None:
            raise FileNotFoundError(name)
        return FileInfo(*row)

    def list_files(self) -> list[FileInfo]:
        with closing(self._connect()) as db:
            rows = db.execute(
                "SELECT name, size, sha256, chunk_count FROM files ORDER BY name"
            ).fetchall()
        return [FileInfo(*row) for row in rows]

    def put(self, name: str, source: BinaryIO) -> FileInfo:
        """Create a file from a binary stream; existing names never overwrite.

        A failed upload rolls back the index and removes its new chunk objects.
        A sudden process/power failure can leave orphaned objects; these are not
        automatically purged to avoid destroying recoverable evidence.
        """
        _check_name(name)
        written: list[Path] = []
        temporaries: list[Path] = []
        committed = False
        total = 0
        file_hash = hashlib.sha256()
        records: list[tuple[str, int, str, str, int, str]] = []
        with closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                if db.execute("SELECT 1 FROM files WHERE name = ?", (name,)).fetchone():
                    raise FileExistsError(name)
                usage = dict(
                    db.execute(
                        "SELECT store_name, COALESCE(SUM(size), 0) FROM chunks GROUP BY store_name"
                    ).fetchall()
                )
                free = {
                    store.name: max(store.capacity_bytes - usage.get(store.name, 0), 0)
                    for store in self.stores.values()
                }
                while True:
                    chosen = max(free, key=free.get)
                    available = min(free[chosen], self.chunk_size)
                    if available <= 0:
                        if source.read(1):
                            raise CapacityError("all storage roots reached their configured quotas")
                        break
                    data = source.read(available)
                    if not isinstance(data, bytes):
                        raise TypeError("source must be a binary stream returning bytes")
                    if len(data) > available:
                        raise ValueError("source returned more bytes than requested")
                    if not data:
                        break
                    digest = hashlib.sha256(data).hexdigest()
                    object_id = uuid.uuid4().hex
                    final_path = self._object_path(chosen, object_id)
                    temp_path = final_path.with_suffix(".tmp")
                    temporaries.append(temp_path)
                    with open(temp_path, "xb") as handle:
                        handle.write(data)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(temp_path, final_path)
                    written.append(final_path)
                    _sync_directory(final_path.parent)
                    with open(final_path, "rb") as persisted:
                        saved_data = persisted.read(len(data) + 1)
                    if hashlib.sha256(saved_data).hexdigest() != digest:
                        raise IntegrityError("newly written chunk failed readback verification")
                    index = len(records)
                    records.append((name, index, chosen, object_id, len(data), digest))
                    total += len(data)
                    free[chosen] -= len(data)
                    file_hash.update(data)
                info = FileInfo(name, total, file_hash.hexdigest(), len(records))
                db.execute(
                    "INSERT INTO files(name, size, sha256, chunk_count) VALUES (?, ?, ?, ?)",
                    (info.name, info.size, info.sha256, info.chunk_count),
                )
                db.executemany(
                    "INSERT INTO chunks(file_name, chunk_index, store_name, object_id, size, sha256) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    records,
                )
                db.commit()
                committed = True
                return info
            except BaseException:
                # On uncertain commit outcome, preserve chunks if the file
                # became visible. Never delete objects referenced by an index.
                try:
                    db.rollback()
                    visible = db.execute(
                        "SELECT 1 FROM files WHERE name = ?", (name,)
                    ).fetchone()
                except sqlite3.Error:
                    visible = True  # Fail toward preservation, not deletion.
                if not committed and not visible:
                    for path in (*temporaries, *written):
                        try:
                            path.unlink(missing_ok=True)
                        except OSError:
                            pass  # Leaves an unreferenced object, not lost data.
                raise

    def _chunk_rows(self, db: sqlite3.Connection, name: str):
        return db.execute(
            "SELECT chunk_index, store_name, object_id, size, sha256 FROM chunks "
            "WHERE file_name = ? ORDER BY chunk_index",
            (name,),
        ).fetchall()

    def _read_chunk(self, row: tuple) -> bytes:
        index, store_name, object_id, expected_size, expected_sha = row
        if not isinstance(expected_size, int) or not 0 < expected_size <= MAX_CHUNK_BYTES:
            raise IntegrityError(f"chunk {index}: invalid length in manifest")
        path = self._object_path(store_name, object_id)
        try:
            with open(path, "rb") as handle:
                data = handle.read(expected_size + 1)
        except FileNotFoundError as exc:
            raise IntegrityError(f"chunk {index}: missing object on {store_name}") from exc
        if len(data) != expected_size or hashlib.sha256(data).hexdigest() != expected_sha:
            raise IntegrityError(f"chunk {index}: length/hash mismatch on {store_name}")
        return data

    def get(self, name: str, sink: BinaryIO) -> None:
        """Read and validate each chunk before writing it to ``sink``.

        If a *later* chunk is missing or corrupt, the caller's sink may already
        contain an earlier prefix. Call ``verify`` first when that is unsafe.
        """
        _check_name(name)
        with closing(self._connect()) as db:
            db.execute("BEGIN")
            info = self._lookup(db, name)
            rows = self._chunk_rows(db, name)
            if len(rows) != info.chunk_count:
                raise IntegrityError("manifest chunk count mismatch")
            cumulative = 0
            hash_file = hashlib.sha256()
            for expected_index, row in enumerate(rows):
                if row[0] != expected_index:
                    raise IntegrityError("manifest chunk indices are discontinuous")
                data = self._read_chunk(row)
                remainder = memoryview(data)
                while remainder:
                    written = sink.write(remainder)
                    if not isinstance(written, int) or written <= 0 or written > len(remainder):
                        raise OSError("sink made no valid progress writing the restored file")
                    remainder = remainder[written:]
                cumulative += len(data)
                hash_file.update(data)
            if cumulative != info.size or hash_file.hexdigest() != info.sha256:
                raise IntegrityError("whole-file size/hash mismatch")

    def verify(self, name: str) -> Verification:
        """Inspect every referenced chunk and whole-file checksum."""
        _check_name(name)
        with closing(self._connect()) as db:
            db.execute("BEGIN")
            info = self._lookup(db, name)
            rows = self._chunk_rows(db, name)
            problems: list[str] = []
            if len(rows) != info.chunk_count:
                problems.append("manifest chunk count mismatch")
            cumulative = 0
            file_hash = hashlib.sha256()
            for expected_index, row in enumerate(rows):
                if row[0] != expected_index:
                    problems.append(f"manifest index {row[0]} is not {expected_index}")
                try:
                    data = self._read_chunk(row)
                except (IntegrityError, OSError) as exc:
                    problems.append(str(exc))
                    continue
                cumulative += len(data)
                file_hash.update(data)
            if not problems and (
                cumulative != info.size or file_hash.hexdigest() != info.sha256
            ):
                problems.append("whole-file size/hash mismatch")
        return Verification(not problems, problems)

    def delete(self, name: str) -> None:
        """Remove namespace entry first, then best-effort unlink its chunks.

        A crash after index commit may leave orphaned chunks on disk, but cannot
        leave a manifest pointing to chunks removed by this operation.
        """
        _check_name(name)
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            self._lookup(db, name)
            rows = self._chunk_rows(db, name)
            db.execute("DELETE FROM files WHERE name = ?", (name,))
            db.commit()
        for row in rows:
            try:
                self._object_path(row[1], row[2]).unlink(missing_ok=True)
            except OSError:
                pass  # Recoverable orphan; namespace deletion already committed.

