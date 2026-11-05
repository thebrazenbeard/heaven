"""Heaven command-line interface.

The state directory stores only metadata and non-secret backend references.
Cloud authentication stays in rclone's separately managed configuration.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any

from .backends import LocalDirectoryBackend, RcloneBackend
from .store import HeavenStore


CONFIG_NAME = "config.json"
DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024
DEFAULT_STATE = ".heaven"
_SIZE_UNITS = {
    "B": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "TIB": 1024**4,
}


class ConfigurationError(ValueError):
    """Invalid, absent, or unsupported Heaven configuration."""


def parse_size(value: str) -> int:
    """Parse a byte count or human-readable quota (e.g. 15GB, 512MiB)."""
    match = re.fullmatch(r"\s*([0-9]+)\s*([a-zA-Z]*)\s*", value)
    if not match:
        raise argparse.ArgumentTypeError("size must be an integer such as 15GB or 4MiB")
    amount = int(match.group(1))
    unit = match.group(2).upper() or "B"
    if amount <= 0 or unit not in _SIZE_UNITS:
        raise argparse.ArgumentTypeError("size must be positive with a supported unit")
    return amount * _SIZE_UNITS[unit]


def _config_file(state: Path) -> Path:
    return state / CONFIG_NAME


def _load_config(state: Path) -> dict[str, Any]:
    location = _config_file(state)
    if not location.is_file():
        raise ConfigurationError(f"not initialized: {state}; run `heaven --state {state} init`")
    try:
        config = json.loads(location.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigurationError(f"cannot read {location}: {exc}") from exc
    if not isinstance(config, dict) or config.get("version") != 1:
        raise ConfigurationError("unsupported config schema (expected version 1)")
    if not isinstance(config.get("backends"), list):
        raise ConfigurationError("config.backends must be a list")
    chunk_size = config.get("chunk_size")
    if type(chunk_size) is not int or chunk_size <= 0 or chunk_size > 128 * 1024 * 1024:
        raise ConfigurationError("chunk_size must be between 1 byte and 128MiB")
    return config


def _save_config(state: Path, config: dict[str, Any]) -> None:
    """Atomic replacement. Never serializes OAuth credentials or passwords."""
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=".config-", suffix=".json", dir=state)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(config, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temp_path, 0o600)
        os.replace(temp_path, _config_file(state))
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def _build_store(state: Path) -> HeavenStore:
    config = _load_config(state)
    backends = []
    for item in config["backends"]:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise ConfigurationError("invalid backend entry")
        if type(item.get("quota_bytes")) is not int or item["quota_bytes"] <= 0:
            raise ConfigurationError("invalid backend quota")
        kind = item.get("type")
        if kind == "local":
            backends.append(
                LocalDirectoryBackend(item["name"], item["root"], item["quota_bytes"])
            )
        elif kind == "rclone":
            backends.append(RcloneBackend(item["name"], item["remote"], item["quota_bytes"]))
        else:
            raise ConfigurationError(f"unsupported backend type: {kind!r}")
    return HeavenStore(state / "index.sqlite3", backends, chunk_size=config["chunk_size"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="heaven", description="Federated chunked object storage with one logical namespace"
    )
    parser.add_argument(
        "--state", type=Path, default=Path(os.environ.get("HEAVEN_STATE", DEFAULT_STATE)),
        help="explicit Heaven state directory (default: $HEAVEN_STATE or .heaven)",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="create a new local metadata store")
    init.add_argument("--chunk-size", type=parse_size, default=DEFAULT_CHUNK_SIZE)

    backend = commands.add_parser("backend", help="manage pre-authorized storage locations")
    backend_commands = backend.add_subparsers(dest="backend_command", required=True)
    backend_commands.add_parser("list", help="show configured backends")
    for kind in ("local", "rclone"):
        add = backend_commands.add_parser(f"add-{kind}", help=f"register a {kind} backend")
        add.add_argument("--name", required=True, help="stable backend identifier")
        add.add_argument("--quota", required=True, type=parse_size, help="hard capacity budget, e.g. 15GB")
        if kind == "local":
            add.add_argument("--root", required=True, type=Path, help="local directory holding chunks")
        else:
            add.add_argument("--remote", required=True, help="existing rclone remote path, e.g. drive1:heaven")

    put = commands.add_parser("put", help="stream a local file into the virtual pool")
    put.add_argument("source", type=Path)
    put.add_argument("path", help="logical destination path, e.g. /backups/data.bin")
    get = commands.add_parser("get", help="restore a logical file to disk")
    get.add_argument("path")
    get.add_argument("destination", type=Path)
    get.add_argument("--overwrite", action="store_true", help="replace an existing local file")
    listed = commands.add_parser("ls", help="list logical files")
    listed.add_argument("prefix", nargs="?", default="/")
    backup = commands.add_parser("backup", help="create a consistent, verified local manifest backup")
    backup.add_argument("destination", type=Path, help="new SQLite backup path (existing files are refused)")
    inspect = commands.add_parser("inspect-backup", help="verify a backup and its integrity receipt without account access")
    inspect.add_argument("path", type=Path, help="previously created SQLite backup path")
    for command, help_text in (
        ("stat", "print file size and checksum"),
        ("rm", "delete a logical file and its stored chunks"),
        ("verify", "verify every chunk against the recorded checksum"),
    ):
        target = commands.add_parser(command, help=help_text)
        target.add_argument("path")

    serve = commands.add_parser("serve", help="serve local authenticated WebDAV")
    serve.add_argument("--host", default="127.0.0.1", choices=("127.0.0.1", "::1", "localhost"))
    serve.add_argument("--port", type=int, default=8767)
    serve.add_argument("--username", default="heaven")
    serve.add_argument(
        "--password-env", default="HEAVEN_DAV_PASSWORD",
        help="environment variable containing password (never stored in config)",
    )
    return parser


def _file_json(info: Any) -> str:
    if hasattr(info, "__dataclass_fields__"):
        return json.dumps(asdict(info), sort_keys=True)
    return json.dumps(info, sort_keys=True)


def run(args: argparse.Namespace) -> int:
    state = args.state.expanduser().resolve()
    if args.command == "init":
        if _config_file(state).exists():
            raise ConfigurationError(f"already initialized: {state}")
        if args.chunk_size > 128 * 1024 * 1024:
            raise ConfigurationError("chunk size cannot exceed 128MiB")
        _save_config(state, {"version": 1, "chunk_size": args.chunk_size, "backends": []})
        print(f"initialized {state}")
        return 0

    if args.command == "backend":
        config = _load_config(state)
        if args.backend_command == "list":
            for item in config["backends"]:
                print(json.dumps(item, sort_keys=True))
            return 0
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}", args.name):
            raise ConfigurationError("backend name must be 1-64 simple ASCII characters")
        if any(item["name"] == args.name for item in config["backends"]):
            raise ConfigurationError(f"backend already exists: {args.name}")
        item: dict[str, Any] = {
            "type": args.backend_command.removeprefix("add-"),
            "name": args.name,
            "quota_bytes": args.quota,
        }
        if item["type"] == "local":
            item["root"] = str(args.root.expanduser().resolve())
        else:
            if not re.fullmatch(r"[A-Za-z0-9_.-]+:.+", args.remote):
                raise ConfigurationError("rclone remote must have an existing remote-name:path format")
            item["remote"] = args.remote
        config["backends"].append(item)
        _save_config(state, config)
        print(f"added {item['type']} backend {args.name}")
        return 0

    # Metadata custody does not require opening any provider/account backend.
    # Backup is deliberately local-only and never modifies account data.
    if args.command == "backup":
        _load_config(state)
        from .recovery import create_backup

        print(_file_json(create_backup(
            state / "index.sqlite3", args.destination.expanduser().resolve(),
            config_path=_config_file(state),
        )))
        return 0
    if args.command == "inspect-backup":
        from .recovery import inspect_backup

        print(_file_json(inspect_backup(args.path.expanduser().resolve())))
        return 0

    store = _build_store(state)
    try:
        if args.command == "put":
            with args.source.open("rb") as source:
                print(_file_json(store.put_file(args.path, source)))
        elif args.command == "get":
            # Download to a sibling temporary file so an interrupted retrieval
            # never damages a pre-existing destination.
            destination = args.destination.expanduser().resolve()
            if destination.exists() and not args.overwrite:
                raise ConfigurationError("destination exists; supply --overwrite to replace it")
            destination.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(prefix=".heaven-download-", dir=destination.parent)
            try:
                with os.fdopen(fd, "wb") as output:
                    info = store.get_file(args.path, output)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(temp_path, destination)
            finally:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
            print(_file_json(info))
        elif args.command == "ls":
            for info in store.list_files(args.prefix):
                print(_file_json(info))
        elif args.command == "stat":
            print(_file_json(store.stat_file(args.path)))
        elif args.command == "rm":
            store.delete_file(args.path)
            print(f"deleted {args.path}")
        elif args.command == "verify":
            verified = store.verify_file(args.path)
            print("OK" if verified else "FAILED")
            return 0 if verified else 2
        elif args.command == "serve":
            if not 1 <= args.port <= 65535:
                raise ConfigurationError("port must be between 1 and 65535")
            secret = os.environ.get(args.password_env)
            if not secret:
                raise ConfigurationError(
                    f"set {args.password_env} to a nonempty WebDAV password before serving"
                )
            from .webdav import serve as serve_webdav

            serve_webdav(store, host=args.host, port=args.port, username=args.username, password=secret)
        else:
            raise ConfigurationError(f"unsupported command: {args.command}")
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            close()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except (ConfigurationError, OSError, ValueError, KeyError, RuntimeError) as exc:
        print(f"heaven: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
