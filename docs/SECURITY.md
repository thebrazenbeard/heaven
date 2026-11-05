# Security and recovery boundaries

Heaven is a virtual storage manager, **not** a way to make provider-owned storage irrevocable. Accounts and quotas belong to their providers. Account suspension, rate limits, credential revocation and changes in provider terms can remove capacity without notice. Do not assume that large numbers of free accounts are authorized or durable.

## Prototype boundaries

- `LocalDirectoryBackend` is the deterministic, testable first backend. Directories simulate separate accounts; they do **not** constitute a Google Drive integration test.
- An optional rclone backend uses independently configured and authorized remotes. Heaven must never create accounts, scrape passwords, disable rate limiting, or store OAuth tokens in its SQLite catalog or repository.
- The local WebDAV listener is for loopback testing. Put authentication in front of it before accepting writes; no public interface, autostart, or Internet exposure is justified by source tests.
- A complete file is only advertised after its chunks and committed manifest are present. Use SHA-256 to detect damage, not to repair it. Redundancy/erasure coding remains a separate, unimplemented recovery feature until tested.
- The SQLite catalog is operationally critical. Copies of data chunks alone cannot reconstruct filenames, chunk order, or ownership. Back it up independently, test restore, and consider an encrypted replicated catalog before storing irreplaceable data.
- No storage provider is a substitute for a separately controlled, recoverable backup. If a backend disappears, files may become unreadable unless recoverable copies or erasure coding were actually written.

## Threats to challenge

1. Path traversal through virtual names and physical chunk identifiers.
2. Malicious clients overwriting a file while readers are retrieving chunks.
3. Account full/connection interrupted midway through a file write.
4. A backend returning corrupt data; every retrieved chunk must be checked against its catalog digest.
5. The metadata catalog being corrupted, lost, rolled back or tampered with.
6. Concurrent writers selecting the same backend capacity or superseding each other.
7. External backend quotas, retries, versioning semantics and provider throttling.
8. Secrets leaking through process command lines, logs or committed configuration.

Release status is **development prototype** until fault-injection, restore and independently authorized provider integration tests pass. Do not use for sole-copy data.
