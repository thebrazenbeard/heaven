# Heaven operations (local MVP)

This guide covers the reproducible local-storage milestone. It deliberately avoids requiring cloud credentials, paid compute, a Synology backup destination, or account creation. The NAS and other host storage must not be altered merely to test Heaven.

## Safety boundaries

- Run the first tests using fresh temporary directories, **not** a real media library or a shared backup volume.
- Treat all stored data as disposable until byte-for-byte readback, reopen, and simulated-failure tests succeed.
- No cross-account redundancy or client-side encryption is implied by distributed chunking alone.
- Do not depend on stable concurrent read/delete semantics. A raw `Pool.get` sink can receive partial bytes if another client deletes the file concurrently; no reader lease/object pinning exists in this milestone.
- Never embed OAuth refresh tokens, API keys, personal data, or encryption keys in the repository or manifests submitted as test artifacts.
- Do not enable an externally reachable WebDAV listener unless authentication, TLS/private-network isolation, access control and path sanitation are verified.
- Respect the `$0` operating rule. Cloud-provider integrations, once available, must not incur charges or implicitly provision paid resources.
- Do not automate fabricated-account signups or quota evasion. The storage adapter must operate only with accounts and resources that the operator is authorized to use.
- Before any remote-provider test, verify current pricing and operational quotas. Stop if there is any unapproved charge exposure; a free account does not guarantee every related API or network operation is free.

## Local validation checklist

1. Create at least two empty backing directories with capacities individually smaller than the test input.
2. Upload a deterministic file whose total size exceeds either single store but fits their combined available space.
3. Read it back, compare its full digest and bytes, close the process, reopen the pool and repeat.
4. Corrupt a stored chunk; retrieval or verification must fail explicitly instead of returning corrupted output.
5. Remove one stored chunk; expect explicit unavailability. **Automatic healing requires redundancy, which is not yet part of this milestone.**
6. Attempt an input exceeding aggregate capacity; verify no incomplete file appears committed and no capacity accounting is permanently leaked.
7. Probe `../` and absolute-path filenames; they must not escape the pool namespace.
8. If WebDAV is included, test access on loopback only before introducing a private network frontend.

## Operational states

| Observed condition | Operator response |
| --- | --- |
| Checksum mismatch | Stop serving affected data; locate the chunk and inspect without modifying originals |
| Missing store/chunk | Mark file unavailable; do not claim reconstruction without another copy/parity |
| Concurrent delete while reading | Raw library sink may contain a prefix before failure; retry only against a stable logical file and avoid concurrent mutating clients |
| Insufficient capacity | Reject write clearly; ensure previously committed files remain intact |
| Manifest unavailable | Stop writes; recover the exact manifest from a separately qualified recovery mechanism |
| Interrupted upload | Inspect pending/orphan chunks; do not manually delete unknown objects before identifying ownership |
| Physical disk fills despite logical free quota | Inspect filesystem free space and unreferenced objects; configured quotas track committed logical chunks, not all physical use |
| Credential/API failure (future) | Classify retryable vs permanent failures and back off; do not silently declare data deleted |

## Recovery priorities

1. Preserve the manifest, storage roots, and relevant logs before experimenting with repair.
2. Record which logical files, ordered chunks, and stores are affected.
3. Run integrity verification without mutation.
4. Restore from known-good independent copies if present; **do not infer** that another chunk contains the missing bytes.
5. After recovery, verify the complete logical file digest from a trusted baseline.

## Developer quickstart

Requires Python 3.11+. Commands from the Heaven checkout:

```bash
python -m pip install -e .
python -m unittest discover -s tests -v
```

The included `examples/local-pool.json` defines two disposable local stores of 16 MiB each, a 1 MiB chunk size, and a SQLite manifest under `demo-state/` in the checkout. It contains no cloud API tokens. Run the following with a **test file**; every `put` stores under the supplied logical name, not the source path:

```bash
heaven --config examples/local-pool.json put ./test-data.bin documents/test-data.bin
heaven --config examples/local-pool.json list
heaven --config examples/local-pool.json verify documents/test-data.bin
heaven --config examples/local-pool.json get documents/test-data.bin ./restored.bin
```

Compare `test-data.bin` and `restored.bin` byte for byte. For the explicit deletion path, `heaven --config examples/local-pool.json delete documents/test-data.bin` removes the **logical pool file**, so do not use that command on irreplaceable contents. The CLI's `get` writes a temporary destination first and renames into place after reconstruction, preventing a failed read from exposing a partial output at the requested path. It refuses to replace an existing destination by default. Only use `--force` when replacement is intentional.

For a meaningful cross-store test, use a file larger than 16 MiB but smaller than combined available capacity, verify its chunks exist under both `demo-state/disk-a` and `demo-state/disk-b`, and then restore it. Do not advertise Heaven as production storage before the checks above are measured.

### Experimental local WebDAV

```bash
heaven --config examples/local-pool.json serve --port 8768
```

This binds **only** to `127.0.0.1:8768`. It is an unauthenticated, restricted Class 1-style gateway for local test clients, not a production network share. It exposes GET, HEAD, PUT (create-only), DELETE, PROPFIND, and OPTIONS; it does not support locking, replacing existing files, MOVE/COPY, or arbitrary WebDAV client behavior. Large GET responses are fully spooled locally before they are served, which can exhaust temporary disk space. Stop the process when testing ends. Do not make it publicly reachable through port forwarding or a proxy without an independently qualified authentication and authorization layer.

## Status reporting

For each qualification report, record exact Git commit/branch, Python/tool versions, number and capacities of test stores, test input sizes/digests, failure injections, test command and result. Keep `SOURCE`, `TEST`, `INSTALL`, `REAL_SYSTEM_PROBE` and `FORMAL_VERIFY` evidence separate.
