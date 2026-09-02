# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Every change lands under the `[Unreleased]` section first. At release time, the
`[Unreleased]` section is renamed to the new version with the release date, and a
fresh empty `[Unreleased]` section is added on top. See `AGENTS.md` for the full
release process.

## [Unreleased]

### Changed

- Updated Walrus to testnet-v1.55.1 and Sui to testnet-v1.78.0, pinning
  `allocative` to 0.3.4 (Sui's locked version) and relinking diesel's
  `num-bigint` lock edge to 0.4.8 to keep the build green.

## [0.2.0] - 2026-08-22

### Changed

- Updated Walrus to testnet-v1.54.0 and Sui to testnet-v1.77.1, pinning
  `allocative` to 0.3.4 (Sui's locked version) and relinking diesel's
  `num-bigint` lock edge to 0.4.8 to keep the build green.
- Updated Walrus to testnet-v1.53.0 and Sui to testnet-v1.76.0 (automated weekly update).

### Added

- Weekly automated workflow that checks for a new Walrus testnet release and
  opens a PR updating Walrus and Sui (paired to the Sui version that Walrus
  release pins) (#24).

## [0.1.0] - 2026-07-29

Initial release.

### Added

- Checkpoint archival pipeline that downloads Sui checkpoints (bucket-based or
  ingestion-service-based downloaders, including a GCS proto checkpoint
  downloader), bundles them into blobs, and publishes them to Walrus.
- Checkpoint blob extender that monitors blob expiration epochs and extends
  blobs before they expire, including shared archival blobs funded via WAL
  tokens.
- Archival state tracking in RocksDB with optional PostgreSQL dual-write, and an
  on-chain metadata pointer with state snapshot creation.
- REST API server for serving archival data and a caching server with metadata
  blob ID collection.

### Changed

- Updated Walrus to testnet-v1.52.1 and Sui to testnet-v1.75.1 (#20).
- Made the ingestion service subscriber channel size configurable (#21).
- All Sui fullnode queries now use gRPC instead of JSON-RPC (#22).
