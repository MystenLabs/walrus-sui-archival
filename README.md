# walrus-sui-archival

A system for archiving Sui blockchain checkpoints to Walrus decentralized storage.

## Overview

This system archives Sui checkpoints by:
1. Downloading checkpoints from Walrus Archival
2. Bundling them into compressed blobs
3. Uploading to Walrus for permanent decentralized storage
4. Automatically extending blob expiration epochs
5. Maintaining on-chain metadata for disaster recovery
6. Providing REST API and web UI for data access

## Quick Start

```bash
# Build
cargo build --release

# Run archival service
cargo run --release -- run --config config/testnet_local_config.yaml

# Access web interface (default port 8080)
open http://localhost:8080
```

## Commands

### Main Service

```bash
# Run the archival service
cargo run --release -- run --config config/testnet_local_config.yaml
```

### Database Inspection

```bash
# List all archived blobs
cargo run --release -- inspect-db --db-path archival_db list-blobs

# Get blob info for a specific checkpoint
cargo run --release -- inspect-db --db-path archival_db get-checkpoint-blob-info --checkpoint 12345

# Get the latest archived checkpoint
cargo run --release -- inspect-db --db-path archival_db get-latest-checkpoint
```

### Blob Inspection

```bash
# Inspect a local blob file
cargo run --release -- inspect-blob --path /path/to/blob.bin

# Inspect a blob from Walrus by blob ID
cargo run --release -- inspect-blob \
  --blob-id "BlobIDHere" \
  --client-config config/local_testnet_client_config.yaml \
  --context testnet

# Inspect specific entry in blob by index
cargo run --release -- inspect-blob \
  --path /path/to/blob.bin \
  --index 5

# Inspect blob with full checkpoint data
cargo run --release -- inspect-blob \
  --blob-id "BlobIDHere" \
  --client-config config/local_testnet_client_config.yaml \
  --full
```

### Blob Management

```bash
# List all blobs owned by the wallet
cargo run --release -- list-owned-blobs \
  --client-config config/local_testnet_client_config.yaml \
  --context testnet
```

### Metadata Management

```bash
# Get current metadata blob ID from on-chain pointer
cargo run --release -- get-metadata-blob-id --config config/testnet_local_config.yaml

# Clear metadata blob ID from on-chain pointer
cargo run --release -- clear-metadata-blob-id --config config/testnet_local_config.yaml

# Dump metadata blob content
cargo run --release -- dump-metadata-blob --config config/testnet_local_config.yaml
```

## Docker

```bash
cd docker/walrus-sui-archival
./build.sh
docker run -v /path/to/config:/config \
  -v /path/to/db:/data \
  -p 8080:8080 \
  walrus-sui-archival:latest run --config /config/config.yaml
```

## REST API & Web Interface

The service provides both a web interface and REST API endpoints:

### Web Pages

- `GET /` - Home page with archival statistics, metadata tracking info, and navigation
- `GET /v1/blobs` - Browse all archived checkpoint blobs with detailed metadata
- `GET /v1/checkpoint` - Interactive checkpoint lookup with optional content display
- `GET /v1/health` - Health check endpoint

### API Endpoints

All API endpoints return JSON responses:

- `GET /v1/health` - Service health check (returns 200 OK)
- `GET /v1/checkpoint?checkpoint=<number>` - Get checkpoint blob information
  - Query parameters:
    - `checkpoint` (required): Checkpoint sequence number
    - `show_content` (optional): Set to `true` to include full checkpoint data
- `GET /v1/blobs` - List all archived blobs with metadata

### Web Interface Features

The web interface provides:

1. **Home Page**:
   - Real-time archival statistics (total blobs, checkpoints, size)
   - Current metadata blob ID from on-chain pointer
   - Metadata tracking information for disaster recovery
   - Quick navigation to other pages

2. **Blob List Page**:
   - Complete list of all archived checkpoint blobs
   - Blob metadata (blob ID, object ID, checkpoint ranges, size)
   - Epoch and expiration information

3. **Checkpoint Lookup**:
   - Search for specific checkpoints by number
   - View blob location and metadata
   - Optional: Display full checkpoint content (for testing only)

## Architecture

The system consists of several components that run concurrently:

1. **Checkpoint Downloader**: Downloads checkpoints from S3 using multiple workers
2. **Checkpoint Monitor**: Monitors downloaded checkpoints, handles out-of-order delivery, and triggers blob building
3. **Checkpoint Blob Publisher**: Bundles checkpoints into blobs and uploads to Walrus
4. **Checkpoint Blob Extender**: Extends blob expiration epochs automatically
5. **Archival State Snapshot Creator**: Creates and uploads metadata snapshots to Sui
6. **REST API Server**: Provides web interface and API endpoints for data access

### Backpressure Mechanism

The system implements automatic backpressure control:
- When the monitor has >500 pending checkpoints, it pauses the downloader
- When pending checkpoints drop to ≤100, downloading resumes
- This prevents memory issues and ensures smooth operation

## Configuration

See `config/testnet_local_config.yaml` for example configuration.

Key configuration sections:
- `checkpoint_downloader`: S3 download settings and worker count
- `checkpoint_monitor`: Blob building thresholds (size, time, end-of-epoch)
- `checkpoint_blob_publisher`: Walrus upload settings
- `checkpoint_blob_extender`: Automatic expiration extension settings
- `archival_state_snapshot`: Metadata snapshot and disaster recovery settings
- `rest_api_address`: Web interface bind address (default: 0.0.0.0:8080)