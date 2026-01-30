# walrus-sui-archival

A system for archiving Sui blockchain checkpoints to Walrus decentralized storage, with PostgreSQL persistence and a caching server for efficient data access.

## Overview

This system archives Sui checkpoints by:
1. Downloading checkpoints from Sui Archival
2. Bundling them into compressed blobs
3. Uploading to Walrus for permanent decentralized storage
4. Automatically extending blob expiration epochs
5. Maintaining on-chain metadata for disaster recovery
6. Persisting checkpoint metadata to PostgreSQL for fast queries
7. Providing REST API and web UI for data access
8. Offering a caching server for efficient frontend data delivery

## Project Structure

```
walrus-sui-archival/
├── crates/
│   ├── walrus-sui-archival/   # Main archival service
│   ├── caching-server/        # Reverse proxy with PostgreSQL/backend support
│   ├── postgres-store/        # PostgreSQL schema and migrations
│   ├── blob-bundle/           # Checkpoint blob bundling utilities
│   └── in-memory-checkpoint-holder/  # In-memory checkpoint management
├── website/                   # Next.js frontend (hosted on Walrus Sites)
└── docker/                    # Docker configurations
```

## Quick Start

```bash
# Build all crates
cargo build --release

# Run the main archival service
cargo run --release -p walrus-sui-archival -- run --config config/testnet_local_config.yaml

# Run the caching server (for frontend)
cargo run --release -p caching-server -- \
  --backend-url http://localhost:8080 \
  --database-url postgres://user:pass@localhost/walrus_archival

# Access web interface (default port 8080)
open http://localhost:8080
```

## Crates

### walrus-sui-archival

The main archival service that downloads, bundles, and uploads Sui checkpoints to Walrus.

```bash
# Run the archival service
cargo run --release -p walrus-sui-archival -- run --config config/testnet_local_config.yaml
```

### caching-server

A reverse proxy that serves frontend requests by querying PostgreSQL directly or falling back to the backend service. Features:
- Direct PostgreSQL queries for fast checkpoint and blob lookups
- Fallback to backend when PostgreSQL is unavailable
- In-memory caching with stale-while-revalidate
- Prometheus metrics with source tracking (postgres vs backend)
- `/metrics` endpoint for monitoring

```bash
# Run with PostgreSQL (recommended)
cargo run --release -p caching-server -- \
  --backend-url http://localhost:8080 \
  --database-url postgres://user:pass@localhost/walrus_archival \
  --listen-addr 0.0.0.0:3000

# Run without PostgreSQL (backend-only mode)
cargo run --release -p caching-server -- \
  --backend-url http://localhost:8080 \
  --listen-addr 0.0.0.0:3000
```

### postgres-store

Shared PostgreSQL schema and connection utilities used by both `walrus-sui-archival` and `caching-server`.

## Commands

### Main Service

```bash
# Run the archival service
cargo run --release -p walrus-sui-archival -- run --config config/testnet_local_config.yaml
```

### Database Inspection

```bash
# List all archived blobs
cargo run --release -p walrus-sui-archival -- inspect-db --db-path archival_db list-blobs

# Get blob info for a specific checkpoint
cargo run --release -p walrus-sui-archival -- inspect-db --db-path archival_db get-checkpoint-blob-info --checkpoint 12345

# Get the latest archived checkpoint
cargo run --release -p walrus-sui-archival -- inspect-db --db-path archival_db get-latest-checkpoint
```

### Blob Inspection

```bash
# Inspect a local blob file
cargo run --release -p walrus-sui-archival -- inspect-blob --path /path/to/blob.bin

# Inspect a blob from Walrus by blob ID
cargo run --release -p walrus-sui-archival -- inspect-blob \
  --blob-id "BlobIDHere" \
  --client-config config/local_testnet_client_config.yaml \
  --context testnet

# Inspect specific entry in blob by index
cargo run --release -p walrus-sui-archival -- inspect-blob \
  --path /path/to/blob.bin \
  --index 5

# Inspect blob with full checkpoint data
cargo run --release -p walrus-sui-archival -- inspect-blob \
  --blob-id "BlobIDHere" \
  --client-config config/local_testnet_client_config.yaml \
  --full
```

### Blob Management

```bash
# List all blobs owned by the wallet
cargo run --release -p walrus-sui-archival -- list-owned-blobs \
  --client-config config/local_testnet_client_config.yaml \
  --context testnet
```

### Metadata Management

```bash
# Get current metadata blob ID from on-chain pointer
cargo run --release -p walrus-sui-archival -- get-metadata-blob-id --config config/testnet_local_config.yaml

# Clear metadata blob ID from on-chain pointer
cargo run --release -p walrus-sui-archival -- clear-metadata-blob-id --config config/testnet_local_config.yaml

# Dump metadata blob content
cargo run --release -p walrus-sui-archival -- dump-metadata-blob --config config/testnet_local_config.yaml
```

## Docker

### Archival Service

```bash
cd docker/walrus-sui-archival
./build.sh
docker run -v /path/to/config:/config \
  -v /path/to/db:/data \
  -p 8080:8080 \
  walrus-sui-archival:latest run --config /config/config.yaml
```

### Caching Server

```bash
cd docker/caching-server
./build.sh
docker run \
  -e BACKEND_URL=http://backend:8080 \
  -e DATABASE_URL=postgres://user:pass@db/walrus_archival \
  -p 3000:3000 \
  caching-server:latest
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

The system consists of two main services and several internal components:

### Archival Service (walrus-sui-archival)

The main service that runs the following components concurrently:

1. **Checkpoint Downloader**: Downloads checkpoints from Sui Archival using multiple workers
2. **Checkpoint Monitor**: Monitors downloaded checkpoints, handles out-of-order delivery, and triggers blob building
3. **Checkpoint Blob Publisher**: Bundles checkpoints into blobs and uploads to Walrus
4. **Checkpoint Blob Extender**: Extends blob expiration epochs automatically
5. **Archival State Snapshot Creator**: Creates and uploads metadata snapshots to Sui
6. **REST API Server**: Provides web interface and API endpoints for data access
7. **PostgreSQL Writer**: Persists checkpoint and blob metadata to PostgreSQL (async, non-blocking)

### Caching Server (caching-server)

A lightweight reverse proxy for serving frontend requests:

1. **PostgreSQL Query Layer**: Direct database queries for checkpoint and blob lookups
2. **Backend Proxy**: Falls back to the archival service REST API when PostgreSQL is unavailable
3. **In-Memory Cache**: Caches responses with stale-while-revalidate strategy
4. **Metrics Endpoint**: Prometheus metrics with source tracking

### Data Flow

```
Sui Archival → Archival Service → RocksDB (local)
                    ↓
                PostgreSQL ← Caching Server → Frontend (Walrus Sites)
                    ↓
                 Walrus (blob storage)
```

### Backpressure Mechanism

The system implements automatic backpressure control:
- When the monitor has >500 pending checkpoints, it pauses the downloader
- When pending checkpoints drop to ≤100, downloading resumes
- This prevents memory issues and ensures smooth operation

### Metrics

Both services expose Prometheus metrics:

**Archival Service** (`/metrics`):
- `pg_inserts_total`, `pg_insert_failures`, `pg_insert_latency_seconds` - PostgreSQL insert metrics
- `pg_updates_total`, `pg_update_failures`, `pg_update_latency_seconds` - PostgreSQL update metrics
- `pg_deletes_total`, `pg_delete_failures` - PostgreSQL delete metrics
- `pg_queries_total`, `pg_query_failures`, `pg_query_latency_seconds` - PostgreSQL query metrics
- `rocksdb_inserts_total`, `rocksdb_insert_failures`, `rocksdb_insert_latency_seconds` - RocksDB insert metrics
- `rocksdb_updates_total`, `rocksdb_update_failures`, `rocksdb_update_latency_seconds` - RocksDB update metrics
- `rocksdb_deletes_total`, `rocksdb_delete_failures` - RocksDB delete metrics
- `rocksdb_queries_total`, `rocksdb_query_failures`, `rocksdb_query_latency_seconds` - RocksDB query metrics
- `rocksdb_consistency_gaps` - Consistency check failures

**Caching Server** (`/metrics`):
- `http_requests_total{endpoint, source, status}` - HTTP request counts by endpoint, data source, and status
- `http_request_latency_seconds{endpoint, source}` - HTTP request latency
- `cache_hits_total`, `cache_misses_total`, `cache_stale_serves_total` - Cache statistics
- `postgres_queries_total{operation}`, `postgres_query_failures{operation}` - PostgreSQL query metrics
- `postgres_query_latency_seconds{operation}` - PostgreSQL query latency
- `backend_requests_total`, `backend_request_failures` - Backend proxy metrics
- `backend_request_latency_seconds` - Backend request latency

## Configuration

### Archival Service

See `config/testnet_local_config.yaml` for example configuration.

Key configuration sections:
- `checkpoint_downloader`: Checkpoint download settings and worker count
- `checkpoint_monitor`: Blob building thresholds (size, time, end-of-epoch)
- `checkpoint_blob_publisher`: Walrus upload settings
- `checkpoint_blob_extender`: Automatic expiration extension settings
- `archival_state_snapshot`: Metadata snapshot and disaster recovery settings
- `rest_api_address`: Web interface bind address (default: 0.0.0.0:8080)
- `postgres_database_url`: PostgreSQL connection URL (optional)

### Caching Server

The caching server is configured via command-line arguments or environment variables:

| Argument | Environment Variable | Description | Default |
|----------|---------------------|-------------|---------|
| `--backend-url` | `BACKEND_URL` | Backend service URL | Required |
| `--database-url` | `DATABASE_URL` | PostgreSQL connection URL | Optional |
| `--listen-addr` | `LISTEN_ADDR` | Listen address | `0.0.0.0:3000` |
| `--cache-ttl` | `CACHE_TTL` | Cache TTL in seconds | `60` |

## Website

The frontend is a Next.js application hosted on Walrus Sites. See `website/README.md` for development instructions.

```bash
cd website
npm install
npm run dev
```

To deploy to Walrus Sites:
```bash
npm run build
site-builder publish ./out
```
AWS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
