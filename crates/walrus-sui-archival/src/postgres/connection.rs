// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::{Context, Result};
use deadpool_diesel::postgres::{Manager, Pool, Runtime};
use diesel::prelude::*;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use super::{
    models::{
        CheckpointBlobInfoRow,
        CheckpointIndexEntryRow,
        NewCheckpointBlobInfo,
        NewCheckpointIndexEntry,
        UpdateCheckpointBlobInfo,
    },
    schema::{checkpoint_blob_info, checkpoint_index_entry},
};

/// Embedded migrations from the migrations directory.
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// PostgreSQL connection pool wrapper.
#[derive(Clone)]
pub struct PostgresPool {
    pool: Pool,
}

impl PostgresPool {
    /// Create a new PostgreSQL connection pool from DATABASE_URL environment variable.
    pub fn from_env() -> Result<Self> {
        let database_url =
            std::env::var("DATABASE_URL").context("DATABASE_URL environment variable not set")?;
        Self::new(&database_url)
    }

    /// Create a new PostgreSQL connection pool with the given database URL.
    pub fn new(database_url: &str) -> Result<Self> {
        let manager = Manager::new(database_url.to_string(), Runtime::Tokio1);
        let pool = Pool::builder(manager)
            .max_size(16)
            .build()
            .context("Failed to create database pool")?;

        Ok(Self { pool })
    }

    /// Run database migrations.
    pub async fn run_migrations(&self) -> Result<()> {
        let conn = self.pool.get().await.context("Failed to get connection")?;
        conn.interact(|conn| {
            conn.run_pending_migrations(MIGRATIONS)
                .map(|_| ())
                .map_err(|e| anyhow::anyhow!("Migration error: {}", e))
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))??;

        tracing::info!("Database migrations completed successfully");
        Ok(())
    }

    /// Insert a new checkpoint blob info record.
    pub async fn insert_checkpoint_blob_info(
        &self,
        blob_info: NewCheckpointBlobInfo,
        index_entries: Vec<NewCheckpointIndexEntry>,
    ) -> Result<()> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(move |conn| {
            conn.transaction::<_, diesel::result::Error, _>(|conn| {
                // Insert the blob info
                diesel::insert_into(checkpoint_blob_info::table)
                    .values(&blob_info)
                    .on_conflict(checkpoint_blob_info::start_checkpoint)
                    .do_update()
                    .set((
                        checkpoint_blob_info::end_checkpoint.eq(&blob_info.end_checkpoint),
                        checkpoint_blob_info::blob_id.eq(&blob_info.blob_id),
                        checkpoint_blob_info::object_id.eq(&blob_info.object_id),
                        checkpoint_blob_info::end_of_epoch.eq(&blob_info.end_of_epoch),
                        checkpoint_blob_info::blob_expiration_epoch
                            .eq(&blob_info.blob_expiration_epoch),
                        checkpoint_blob_info::is_shared_blob.eq(&blob_info.is_shared_blob),
                        checkpoint_blob_info::version.eq(&blob_info.version),
                    ))
                    .execute(conn)?;

                // Delete existing index entries for this blob (in case of update)
                diesel::delete(checkpoint_index_entry::table.filter(
                    checkpoint_index_entry::start_checkpoint.eq(blob_info.start_checkpoint),
                ))
                .execute(conn)?;

                // Insert index entries in batches
                if !index_entries.is_empty() {
                    for chunk in index_entries.chunks(1000) {
                        diesel::insert_into(checkpoint_index_entry::table)
                            .values(chunk)
                            .execute(conn)?;
                    }
                }

                Ok(())
            })
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))?;

        Ok(())
    }

    /// Get checkpoint blob info by start_checkpoint.
    pub async fn get_checkpoint_blob_info(
        &self,
        start_checkpoint: i64,
    ) -> Result<Option<CheckpointBlobInfoRow>> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(move |conn| {
            checkpoint_blob_info::table
                .find(start_checkpoint)
                .first(conn)
                .optional()
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// Find checkpoint blob info that contains a given checkpoint number.
    pub async fn find_blob_containing_checkpoint(
        &self,
        checkpoint: i64,
    ) -> Result<Option<CheckpointBlobInfoRow>> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(move |conn| {
            checkpoint_blob_info::table
                .filter(checkpoint_blob_info::start_checkpoint.le(checkpoint))
                .filter(checkpoint_blob_info::end_checkpoint.ge(checkpoint))
                .first(conn)
                .optional()
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// Get index entries for a checkpoint blob.
    pub async fn get_index_entries(
        &self,
        start_checkpoint: i64,
    ) -> Result<Vec<CheckpointIndexEntryRow>> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(move |conn| {
            checkpoint_index_entry::table
                .filter(checkpoint_index_entry::start_checkpoint.eq(start_checkpoint))
                .order(checkpoint_index_entry::checkpoint_number.asc())
                .load(conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// Get the latest stored checkpoint number.
    pub async fn get_latest_stored_checkpoint(&self) -> Result<Option<i64>> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(|conn| {
            checkpoint_blob_info::table
                .select(diesel::dsl::max(checkpoint_blob_info::end_checkpoint))
                .first::<Option<i64>>(conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// Update blob expiration epoch.
    pub async fn update_blob_expiration_epoch(
        &self,
        start_checkpoint: i64,
        new_expiration_epoch: i32,
    ) -> Result<()> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        let update = UpdateCheckpointBlobInfo {
            blob_expiration_epoch: Some(new_expiration_epoch),
        };

        conn.interact(move |conn| {
            diesel::update(checkpoint_blob_info::table.find(start_checkpoint))
                .set(&update)
                .execute(conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))?;

        Ok(())
    }

    /// Remove entries with start_checkpoint >= the given checkpoint.
    pub async fn remove_entries_from_checkpoint(&self, from_checkpoint: i64) -> Result<usize> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(move |conn| {
            // Index entries are deleted via CASCADE
            diesel::delete(
                checkpoint_blob_info::table
                    .filter(checkpoint_blob_info::start_checkpoint.ge(from_checkpoint)),
            )
            .execute(conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// List all checkpoint blob infos.
    pub async fn list_all_blobs(&self, reverse: bool) -> Result<Vec<CheckpointBlobInfoRow>> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(move |conn| {
            if reverse {
                checkpoint_blob_info::table
                    .order(checkpoint_blob_info::start_checkpoint.desc())
                    .load(conn)
            } else {
                checkpoint_blob_info::table
                    .order(checkpoint_blob_info::start_checkpoint.asc())
                    .load(conn)
            }
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// Count the number of checkpoint blob records.
    pub async fn count_checkpoint_blobs(&self) -> Result<i64> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(|conn| checkpoint_blob_info::table.count().get_result::<i64>(conn))
            .await
            .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
            .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// Check if a blob with the given start_checkpoint exists.
    pub async fn blob_exists(&self, start_checkpoint: i64) -> Result<bool> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(move |conn| {
            diesel::select(diesel::dsl::exists(
                checkpoint_blob_info::table.find(start_checkpoint),
            ))
            .get_result::<bool>(conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// Get the inner pool for advanced usage.
    pub fn get_pool(&self) -> &Pool {
        &self.pool
    }
}

/// Wrapper for thread-safe sharing of PostgresPool.
pub type SharedPostgresPool = Arc<PostgresPool>;

/// Create a shared PostgresPool from environment.
pub fn create_shared_pool_from_env() -> Result<SharedPostgresPool> {
    Ok(Arc::new(PostgresPool::from_env()?))
}

/// Create a shared PostgresPool with a specific URL.
pub fn create_shared_pool(database_url: &str) -> Result<SharedPostgresPool> {
    Ok(Arc::new(PostgresPool::new(database_url)?))
}
