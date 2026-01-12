// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL database module for checkpoint blob metadata storage.
//!
//! This module re-exports from the `postgres-store` crate and provides
//! additional functionality specific to `walrus-sui-archival` (like backfill).

pub mod backfill;

pub use backfill::run_backfill;
// Re-export everything from postgres-store crate.
pub use postgres_store::{
    CheckpointBlobInfoRow,
    CheckpointIndexEntryRow,
    MIGRATIONS,
    NewCheckpointBlobInfo,
    NewCheckpointIndexEntry,
    PostgresPool,
    SharedPostgresPool,
    UpdateCheckpointBlobInfo,
    create_shared_pool,
    create_shared_pool_from_env,
};
