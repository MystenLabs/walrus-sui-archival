// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL database module for checkpoint blob metadata storage.
//!
//! This module provides a PostgreSQL-based storage backend as an alternative
//! to RocksDB for better scalability and reliability in distributed environments.

pub mod backfill;
pub mod connection;
pub mod models;
pub mod schema;

pub use backfill::run_backfill;
pub use connection::{
    MIGRATIONS,
    PostgresPool,
    SharedPostgresPool,
    create_shared_pool,
    create_shared_pool_from_env,
};
pub use models::{
    CheckpointBlobInfoRow,
    CheckpointIndexEntryRow,
    NewCheckpointBlobInfo,
    NewCheckpointIndexEntry,
    UpdateCheckpointBlobInfo,
};
