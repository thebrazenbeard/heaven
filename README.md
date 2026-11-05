a storage management layer that:

Presents every account as part of one unified filesystem, accessible through something like WebDAV.

Automatically distributes files across accounts based on available capacity.

Splits files larger than an individual account's available storage into smaller chunks distributed across multiple accounts.

Maintains a global index mapping every file and chunk to its physical storage location.

Transparently retrieves and reconstructs files—or streams the required chunks in sequence—when accessed.

Uses checksums, redundancy, and potentially erasure coding to recover from missing or unavailable chunks.

Handles authentication, storage balancing, and account failures automatically.

From the user's perspective, it behaves like one enormous drive, regardless of how many accounts hold the underlying data.
