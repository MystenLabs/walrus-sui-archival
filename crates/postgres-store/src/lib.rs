// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL storage backend for checkpoint blob metadata.
//!
//! This crate provides a PostgreSQL-based storage backend as an alternative
//! to RocksDB for better scalability and reliability in distributed environments.

pub mod connection;
pub mod models;
pub mod schema;

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
