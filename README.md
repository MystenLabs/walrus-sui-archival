# walrus-sui-archival

A system for archiving Sui blockchain checkpoints to Walrus decentralized storage.

## Overview

Archives Sui checkpoints by downloading them from S3, bundling into compressed blobs, and uploading to Walrus for permanent storage. Includes automatic blob expiration extension and REST API for data access.

## Quick Start

```bash
# Build
cargo build --release

# Run archival service
cargo run -- run --config config/testnet_config.yaml

# Query archived data via REST API (default port 8080)
curl http://localhost:8080/checkpoints/12345
curl http://localhost:8080/blobs
```

## Commands

```bash
# Inspect database
cargo run -- inspect-db --db-path archival_db list-blobs
cargo run -- inspect-db --db-path archival_db get-checkpoint-blob-info --checkpoint 12345

# Inspect blobs
cargo run -- inspect-blob --path checkpoint_blobs/some_blob.blob
cargo run -- inspect-blob --blob-id "BlobIDHere" --client-config ~/.walrus/client_config.yaml

# Manage blobs
cargo run -- list-blobs --client-config ~/.walrus/client_config.yaml
cargo run -- burn-blobs --client-config ~/.walrus/client_config.yaml  # Clean up test blobs
```

## Docker

```bash
cd docker/walrus-sui-archival
./build.sh
docker run -v /path/to/config:/config walrus-sui-archival:latest run --config /config/config.yaml
```

## REST API

- `GET /health` - Health check
- `GET /checkpoints/<number>` - Get checkpoint location
- `GET /checkpoints/<number>/content` - Download checkpoint
- `GET /blobs` - List all blobs
- `GET /blobs/<blob_id>` - Get blob metadata

## Configuration

See `config/testnet_config.yaml` for example configuration.