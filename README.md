# walrus-sui-archival

**Work in Progress** - A system for archiving Sui blockchain checkpoints to Walrus decentralized storage.

## What's Being Developed

This tool archives Sui checkpoints by:
1. Downloading checkpoints from Sui's S3 bucket
2. Bundling multiple checkpoints into blob files
3. Uploading these blobs to Walrus for permanent storage
4. Tracking all archived blobs in a local RocksDB database

## Current Features

- Continuous monitoring and downloading of Sui checkpoints
- Automatic bundling when size/time thresholds are reached
- Uploading to Walrus with retry on failure
- Database tracking of all archived checkpoint blobs
- Commands to inspect blobs (both local files and from Walrus)
- Commands to inspect the archival database

## Quick Start

```bash
# Build
cargo build --release

# Run archival (requires config file)
cargo run -- run --config config/testnet_config.yaml

# Inspect database
cargo run -- inspect-db --db-path archival_db list-blobs

# Inspect a blob file
cargo run -- inspect-blob --path checkpoint_blobs/some_blob.blob

# Inspect a blob from Walrus
cargo run -- inspect-blob --blob-id "BlobIDHere" --client-config ~/.walrus/client_config.yaml
```

## Status

This is an active development project. Core archival functionality is working, with ongoing improvements to reliability and monitoring.