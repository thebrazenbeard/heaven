# Heaven architecture and qualification boundaries

Heaven is intended to make multiple independent storage targets appear as one logical filesystem. The original [project description](../README.md) calls for automatic placement, splitting, reconstruction, a global index, redundancy, and a WebDAV-facing namespace. These are goals, not evidence that all mechanisms are already implemented.

## Model

```
       Client filesystem / optional WebDAV gateway
                         |
                    Heaven pool
                  /      |       \
           file index  chunker  placement policy
                         |       |
                  integrity checks
                         |
              provider-neutral stores
                    /         \
             local store    remote adapters (future)
```

The unit of placement is a **chunk**, not an entire file. This is what allows a logical file to be larger than the capacity of any single backing account. The manifest is the authority for mapping each logical name to its ordered sequence of chunks and backing stores. A chunk's digest is an integrity check, not a guarantee of recoverability. Two chunks on different storage targets are not redundant unless additional equivalent/parity data exists.

## First milestone: local-provider MVP

The first deliverable uses ordinary filesystem directories as independent stores, each with an administratively configured **logical** capacity; the configuration does not measure physical disk free space. This isolates the correctness of splitting, placement, indexing and readback from OAuth flows, provider quotas and remote API behavior. Tests should confirm cross-store reconstruction, persistent reopen, corrupt/missing chunks, and full-capacity failures before any real cloud adapter is enabled.

The current Python implementation (`src/heaven/core.py`) provides `Store`, `Pool`, and the methods `put`, `get`, `list_files`, `verify`, and `delete`. `put` is create-only, uses SHA-256 on chunks and complete files, persists a SQLite manifest, and commits a visible file record only after its chunks are written. If an upload raises an error, it rolls back the manifest and attempts to remove uncommitted chunks. A sudden crash may leave orphaned chunk objects, which are not automatically purged. Writes to a single SQLite manifest are serialized. These are **source-inspection findings**, not a durability guarantee.

The optional `src/heaven/webdav.py` contains a restricted, loopback-only Class 1-style WebDAV gateway. It does **not** implement full WebDAV interoperability, locking, authentication, network TLS, MOVE/COPY, or existing-file replacement. To avoid sending a partially corrupted response, the current GET implementation spools the **entire** object locally before replying; that is not low-spool, on-demand streaming and is unsuitable for very large files until redesigned. Its scope is an experimental client bridge, not a public service.

**Concurrent deletion limitation:** A SQLite read transaction preserves the manifest but does not pin chunk files on disk. A concurrent `delete` can unlink a later chunk while `get` is reconstructing a file; the raw caller-provided sink can receive a valid prefix and then an `IntegrityError`. This race has been independently reproduced in local testing. The CLI's temporary destination and the WebDAV gateway's spool contain its effects, but they do **not** solve stable read snapshots for arbitrary API consumers. A production design needs object lifetimes/refcounts or read leases plus delayed garbage collection, validated with concurrent stress tests.

Implementation status is deliberately granular:

| Capability | MVP target | Qualification requirement |
| --- | --- | --- |
| Logical namespace | Source implemented | Exact name-to-byte reconstruction |
| Configurable local stores | Source implemented; logical quotas | End-to-end data crossing stores |
| Chunk integrity | SHA-256 per chunk implemented | Deliberate byte flip rejected |
| Durable metadata | SQLite WAL source implemented | Pool reopen and reconstruction |
| Transactional writes | Index published after chunks; source implemented | Disk-full/injected failure tests |
| Stable concurrent reads | **Not implemented**; delete/read race reproduced | Reader pinning and concurrent delete tests |
| Redundancy/erasure | **Not part of first slice** | Future missing-store recovery tests |
| Encryption | **Not part of first slice** | Future key custody and tamper tests |
| Cloud account adapters | **Not part of first slice** | Authorized-provider integration tests |
| WebDAV/Taildrive | Restricted local WebDAV source; **no Taildrive integration** | Protocol and authentication tests |
| Million-account scale | **Unqualified hypothesis** | Measured sharded metadata/scheduler tests |

Treat missing chunks as **data unavailable**. Do not silently return corrupted bytes, zeros, or a shortened file. Without redundancy, a missing chunk cannot be repaired automatically, only detected and reported.

## Failure domains and non-negotiable invariants

1. **Identity:** A logical file name identifies an ordered, immutable sequence of chunk references for one committed file version. Reject traversal and ambiguous paths.
2. **Integrity:** Every returned chunk must match its expected digest before being served as trusted data; the reassembled byte sequence must match original bytes.
3. **Atomicity:** A put that fails midway must not make a partly written file visible as complete. Pending chunks should be distinguishable from committed ones so interrupted operations can be swept safely.
4. **Capacity:** Placement uses current reserved/committed bytes, not merely nominal quotas. Concurrency needs reservation or locking to prevent over-allocation.
5. **Durability:** A metadata store is a separate failure domain; losing the only manifest may orphan all chunks. Its independent recovery mechanism must be defined before Heaven stores irreplaceable data.
6. **Provider revocation:** A storage credential can expire, an account can be suspended, quotas can change, and APIs can throttle requests. Plan for transient and permanent failures.
7. **No implicit redundancy:** Chunk distribution does not protect data from account loss. Recoverability requires replicas or erasure coding across independent targets with placement constraints.

## Proposed production evolution (not implemented)

**Provider adapters.** Expose `put_chunk`, `get_chunk`, `head_chunk`, and `delete_chunk` capabilities with explicit size, latency, throttling and failure classification. Support only accounts/storage to which the operator has permission, and honor provider terms. Do not assume 15 GB/account is an enduring entitlement or that bulk account creation is accepted. Heaven will not automate mass creation of false identities or rotate through accounts to evade a provider's restrictions. Before enabling each cloud adapter, check its current pricing and quotas; a nominally free account does not prove that every API or network operation is free.

**Manifest safety.** Use a transactional metadata journal, immutable content addresses, idempotent writes and garbage collection based on committed reachability. Maintain a recoverable index, not a volatile global dictionary. Avoid one giant SQLite writer for very large deployments; partition/shard metadata only after profiling.

**Redundancy.** Use explicit replicas or an erasure scheme such as *k* data + *m* parity shards. Place redundancy groups across independent failure domains, and regularly perform restore drills. Capacity estimates must subtract parity overhead and reserved headroom.

**Encryption.** Apply client-side authenticated encryption *before* upload to untrusted stores. Keep encryption keys separate from chunk manifests and provider tokens. Encryption is not equivalent to an integrity checksum, and losing the key defeats reconstruction.

**Unified access.** A WebDAV server can expose the logical namespace; Taildrive may make that server reachable over a private network. Taildrive is an access mechanism, not Heaven's storage pool, scheduling or fault-tolerance engine. Do not publish an unauthenticated writable gateway to the internet.

**Scale.** A million stores is a control-plane stress test, not a linear extension of a ten-store prototype. Scheduler complexity, token rotation, health checks, metadata cardinality, provider throttling, account lifecycle and background repair must be measured rather than inferred from a simple capacity multiplication. The arithmetic `1,000,000 × 15 GB = 15 PB` is *gross theoretical capacity*, not achieved reliable capacity. At a 4 MiB chunk size, 15 PB requires roughly 3.6 billion chunk records before redundancy; a single ordinary SQLite index and full-object WebDAV spooling are demonstrably the wrong production primitives at that scale.

## Hostile challenge

> **Critical:** One metadata database without independent recovery is a single point of total loss even when all chunks survive. Heaven must not become the only copy of irreplaceable data until manifest restore has been tested.

> **Critical:** Distribution without parity turns a large logical file into an all-chunks-required dependency. More accounts can increase the likelihood of a missing chunk; they do not automatically increase resilience.

> **High:** A successful write followed by metadata commit says little about long-term durability. Read-after-write verification, scheduled audits, and proof of restoration under deliberately failed providers are necessary.

> **High:** A million free accounts require provider cooperation. The software question is separable from the operator's permission to automate that many accounts.

## Evidence levels

This document is an architecture specification. Source inspection, unit tests, real provider probes, deployment, and formal recovery qualification are **distinct evidence levels**. A test of two local directories does not establish remote quota behavior, fault tolerance, or production readiness.
