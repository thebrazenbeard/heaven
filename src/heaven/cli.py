"""Command-line interface for the local, provider-neutral Heaven storage pool.

This first implementation uses explicitly configured filesystem storage roots.
It does not create cloud accounts, configure cloud credentials, or claim to be a
backup system.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

from heaven.core import Pool, Store


def _pool_from_config(path: Path) -> Pool:
    config_file = path.resolve()
    config = json.loads(config_file.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a JSON object")

    def relative_path(value: str) -> Path:
        if not isinstance(value, str) or not value:
            raise ValueError("Storage paths must be non-empty strings")
        p = Path(value).expanduser()
        return (p if p.is_absolute() else config_file.parent / p).resolve()

    stores = []
    for entry in config.get("stores", []):
        if not isinstance(entry, dict):
            raise ValueError("Each store must be a JSON object")
        stores.append(
            Store(
                name=entry["name"],
                root=relative_path(entry["root"]),
                capacity_bytes=entry["capacity_bytes"],
            )
        )
    if not stores:
        raise ValueError("Configure at least one store")

    return Pool(
        db_path=relative_path(config["database"]),
        stores=stores,
        chunk_size=config.get("chunk_size", 4 * 1024 * 1024),
    )


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Heaven: one namespace over multiple storage roots")
    p.add_argument("--config", type=Path, required=True, help="JSON pool configuration")
    sub = p.add_subparsers(dest="action", required=True)
    put = sub.add_parser("put", help="Store a local file")
    put.add_argument("source", type=Path)
    put.add_argument("name", help="Name in the Heaven pool")
    get = sub.add_parser("get", help="Restore a pool file to a local path")
    get.add_argument("name")
    get.add_argument("destination", type=Path)
    get.add_argument("--force", action="store_true", help="Explicitly allow overwriting the destination")
    sub.add_parser("list", help="List stored files")
    verify = sub.add_parser("verify", help="Check checksums and availability")
    verify.add_argument("name")
    delete = sub.add_parser("delete", help="Delete one named pool file")
    delete.add_argument("name")
    serve = sub.add_parser("serve", help="Run experimental loopback-only WebDAV gateway")
    serve.add_argument("--port", type=int, default=8768, help="127.0.0.1 port (default: 8768)")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        pool = _pool_from_config(args.config)
        if args.action == "put":
            with args.source.open("rb") as source:
                info = pool.put(args.name, source)
            print(f"stored {info.name} ({info.size} bytes, {info.chunk_count} chunks; sha256 {info.sha256})")
        elif args.action == "get":
            destination = args.destination.resolve()
            if destination.exists() and not args.force:
                raise FileExistsError(f"{destination} already exists (use --force to replace)")
            destination.parent.mkdir(parents=True, exist_ok=True)
            # Don't expose partially reconstructed output if a stored part is bad.
            fd, temp_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
            try:
                with os.fdopen(fd, "wb") as sink:
                    pool.get(args.name, sink)
                    sink.flush()
                    os.fsync(sink.fileno())
                os.replace(temp_name, destination)
            finally:
                if os.path.exists(temp_name):
                    os.unlink(temp_name)
            print(f"restored {args.name} to {destination}")
        elif args.action == "list":
            for item in pool.list_files():
                print(f"{item.size:>12} {item.sha256} {item.name}")
        elif args.action == "verify":
            verdict = pool.verify(args.name)
            if not verdict.ok:
                for problem in verdict.problems:
                    print(f"FAILED: {problem}", file=sys.stderr)
                return 2
            print(f"verified {args.name}")
        elif args.action == "delete":
            pool.delete(args.name)
            print(f"deleted {args.name}")
        elif args.action == "serve":
            from heaven.webdav import make_server

            with make_server(pool, port=args.port) as server:
                print(f"Heaven experimental WebDAV on http://127.0.0.1:{server.server_port}/", flush=True)
                print("Local connections only; no authentication, TLS, locks, or overwrite support", flush=True)
                server.serve_forever()
        return 0
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"heaven: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
