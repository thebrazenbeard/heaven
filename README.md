# Heaven

Heaven combines several independently authorized storage locations behind one logical namespace. It splits large files into bounded chunks, distributes chunks according to available backend capacity, records their order and SHA-256 checksums in SQLite, and reconstructs or streams files on demand. A local, password-protected WebDAV endpoint exposes the namespace to compatible clients.

**Status: experimental prototype. Do not store your only copy of anything important.** This repository does not supply 1,000 accounts, create Google accounts, bypass quotas or policies, or claim that free cloud quotas are permanent. With 1,000 *independently available* 15 GB accounts, the arithmetic is approximately **15 TB gross**, less overhead and redundancy. Actual usable capacity and reliability depend on provider rules, quotas, outages and account retention.

## Quick start: two local accounts

Requires Python 3.10+; no Python runtime dependencies for local operation. From the repository root:

```sh
python -m heaven.cli --state .heaven init --chunk-size 1MiB
python -m heaven.cli --state .heaven backend add-local --name account1 --root ./data/account1 --quota 8MiB
python -m heaven.cli --state .heaven backend add-local --name account2 --root ./data/account2 --quota 8MiB
python -m heaven.cli --state .heaven put ./some-file.bin /archive/some-file.bin
python -m heaven.cli --state .heaven ls /
python -m heaven.cli --state .heaven verify /archive/some-file.bin
python -m heaven.cli --state .heaven get /archive/some-file.bin ./recovered.bin
```

To snapshot the SQLite file/chunk index (including committed WAL data) to a **new**, privately stored backup bundle, then inspect it without touching a storage provider:

```sh
python -m heaven.cli --state .heaven backup ./catalog-2026-11-05.sqlite3
python -m heaven.cli --state .heaven inspect-backup ./catalog-2026-11-05.sqlite3
```

The backup produces a SQLite snapshot, an integrity receipt and a whitelisted configuration sidecar; it refuses overwrites. **It does not contain chunk payloads**, perform a restore, or establish authenticity against a malicious actor able to alter both files and receipt. Keep the backup away from the state volume and preserve your separately owned account data.

A file larger than either local backend's free space may be admitted if there is enough **combined** capacity and the configured chunk size permits placement. Files are served as one logical object; callers do not need to know which backend stores which chunk.

To start a local WebDAV endpoint, provide a password **outside the repository and config file**:

```sh
export HEAVEN_DAV_PASSWORD='choose-a-strong-local-password'
python -m heaven.cli --state .heaven serve --host 127.0.0.1 --port 8767
```

Use a WebDAV client with user `heaven`, the supplied password, and `http://127.0.0.1:8767/`. The HTTP endpoint is intended solely for loopback development; never expose it publicly without a separately tested secure transport/authentication deployment.

## Existing authorized cloud remotes (experimental)

If `rclone` is separately installed and configured for an account you control, it may be registered by its existing remote name. Heaven itself does not handle OAuth secrets:

```sh
python -m heaven.cli --state .heaven backend add-rclone --name drive1 --remote drive1:heaven --quota 15GB
```

This is a **declared placement budget**, not a verified account quota. Provider integration, rate limits, retries, repair after account loss, and automatic free-space reconciliation require their own end-to-end qualification. No credentials, tokens or service account key files should be committed.

## Architecture

- `heaven/backends.py`: common chunk storage backend API, local directory implementation, optional preconfigured rclone remote adapter.
- `heaven/store.py`: capacity-aware placement, virtual file index, integrity checks, streaming reads, deletion and transactional catalog updates.
- `heaven/cli.py`: explicit local state initialization and file/backend operations.
- `heaven/webdav.py`: local HTTP/WebDAV file access for development.
- `heaven/recovery.py`: no-overwrite online SQLite catalog backup, limited nonsecret configuration copy and integrity inspection.
- `tests/`: synthetic roundtrip and hostile regression cases using temporary directories.

The authoritative metadata catalog resides in the configured state directory and must be preserved separately from the backends. Deleting it can destroy the mapping needed to reconstruct files even when the chunks survive. Checksum validation detects corruption but does not repair missing chunks. The long-term path for genuine durability is redundant metadata, replicated or erasure-coded chunks, recovery receipts, and provider-level fault-injection tests.

Backend names are pinned to hashed location identifiers in the catalog. Redirecting a previously used name to a different local directory or rclone remote is rejected; adding a new backend remains allowed. A legacy catalog with unbound existing chunks is deliberately held for verified manual adoption, which is not yet implemented. A pinned rclone remote *alias* is not proof that its external OAuth configuration still targets the same underlying provider account.

## Tests

```sh
python -m unittest discover -s tests -v
```

Read [docs/SECURITY.md](docs/SECURITY.md) before connecting real storage. Local source tests are not proof of service durability or security when placed on the Internet.
