# Heaven

Heaven is a provider-neutral storage management layer intended to present many storage locations as one logical filesystem.

Its long-term design:

- Presents every authorized storage account or device as part of one unified filesystem, accessible through an interface such as WebDAV.
- Distributes files among storage locations according to available capacity.
- Splits files larger than an individual location's free capacity into chunks across multiple locations.
- Maintains a global index mapping logical files and chunks to their physical storage locations.
- Transparently retrieves and reconstructs files, or streams chunks in order.
- Uses checksums and, in future versions, redundancy and erasure coding to recover from missing chunks.
- Tracks authentication, placement, balancing, and failures without manual per-account operation.

From the user's perspective, the goal is one enormous drive, independent of how many storage resources hold its parts.

## Current implementation status

The initial implementation is an **experimental local-filesystem prototype**, not a production cloud storage service. It provides an SQLite manifest, capacity-aware chunk placement across independently configured local roots, SHA-256 integrity checks, reconstruction, and a CLI. It does **not** currently provide encryption, erasure coding, redundant placement, Google Drive integration, or durability against loss of the only manifest. Do not place irreplaceable files in this prototype.

## Local demonstration

Requires Python 3.11 or newer. From the repository root:

```bash
python -m pip install -e .
heaven --config examples/local-pool.json put ./README.md documentation/README.md
heaven --config examples/local-pool.json list
heaven --config examples/local-pool.json verify documentation/README.md
heaven --config examples/local-pool.json get documentation/README.md ./restored-readme.md
heaven --config examples/local-pool.json serve --port 8768
```

The example creates disposable test storage beneath `demo-state/`. Its two configured 16 MiB stores have a combined **32 MiB capacity**, subject to chunk packing. To test a file larger than one store, use a 20 MiB input; to test files larger than the total, increase the configured roots and capacities.

The `serve` command exposes a small experimental subset of WebDAV on `127.0.0.1` only. It is unauthenticated, supports no WebDAV locks or overwrites, and fully spools GET responses before transfer. Do **not** expose it on the public Internet or treat it as a production-compatible WebDAV implementation.

Run the test suite:

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

Read [architecture](docs/ARCHITECTURE.md) and [operations](docs/OPERATIONS.md) for the actual state, failure boundaries, and safe operating assumptions.

## Design boundaries

- **$0 budget:** no deployment, paid cloud services, or paid compute is required for the local prototype.
- **Provider-neutral:** local stores are the first adapter. A Google Drive adapter is a separate, gated development step.
- **Authorized resources only:** Heaven does not create accounts or bypass provider restrictions. Broad or automated mass-account creation is not part of this implementation.
- **Integrity is not redundancy:** a SHA-256 check detects changed bytes; it does not restore missing bytes.
- **A unified namespace is not more capacity:** usable space depends on the real availability, quotas, and costs of underlying providers.

## Collaboration

Development should proceed via reviewable draft pull requests with tests that demonstrate byte-perfect reconstruction, restart recovery, rollback on capacity exhaustion, and explicit failure on corrupted or missing parts.
