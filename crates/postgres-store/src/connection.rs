// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::{Context, Result};
use deadpool_diesel::postgres::{Manager, Pool, Runtime};
use diesel::prelude::*;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::{
    models::{
        CheckpointBlobInfoRow,
        CheckpointIndexEntryRow,
        NewCheckpointBlobInfo,
        NewCheckpointIndexEntry,
        UpdateCheckpointBlobInfo,
    },
    schema::{checkpoint_blob_info, checkpoint_index_entry},
};

/// Helper struct for raw SQL query that returns blob size.
#[derive(QueryableByName)]
struct BlobSizeResult {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    total: i64,
}

/// Aggregate stats for checkpoint blobs.
#[derive(Debug, Clone, QueryableByName)]
pub struct BlobStats {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub blob_count: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub earliest_checkpoint: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub latest_checkpoint: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub total_size: i64,
}

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
                        checkpoint_blob_info::blob_size.eq(&blob_info.blob_size),
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

    /// List checkpoint blob infos with pagination.
    ///
    /// # Arguments
    /// * `reverse` - If true, returns blobs in descending order (newest first)
    /// * `limit` - Maximum number of blobs to return (default 50)
    /// * `cursor` - Optional cursor (start_checkpoint) for pagination.
    ///   - If reverse=true: returns blobs with start_checkpoint < cursor
    ///   - If reverse=false: returns blobs with start_checkpoint > cursor
    pub async fn list_all_blobs(
        &self,
        reverse: bool,
        limit: Option<i64>,
        cursor: Option<i64>,
    ) -> Result<Vec<CheckpointBlobInfoRow>> {
        let conn = self.pool.get().await.context("Failed to get connection")?;
        let limit = limit.unwrap_or(50);

        conn.interact(move |conn| {
            if reverse {
                let mut query = checkpoint_blob_info::table
                    .order(checkpoint_blob_info::start_checkpoint.desc())
                    .limit(limit)
                    .into_boxed();

                if let Some(c) = cursor {
                    query = query.filter(checkpoint_blob_info::start_checkpoint.lt(c));
                }

                query.load(conn)
            } else {
                let mut query = checkpoint_blob_info::table
                    .order(checkpoint_blob_info::start_checkpoint.asc())
                    .limit(limit)
                    .into_boxed();

                if let Some(c) = cursor {
                    query = query.filter(checkpoint_blob_info::start_checkpoint.gt(c));
                }

                query.load(conn)
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

    /// Get aggregate stats for homepage: blob_count, earliest_checkpoint, latest_checkpoint, total_size.
    pub async fn get_blob_stats(&self) -> Result<BlobStats> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(|conn| {
            // Use raw SQL for aggregate query
            diesel::sql_query(
                "SELECT
                    COUNT(*) as blob_count,
                    COALESCE(MIN(start_checkpoint), 0) as earliest_checkpoint,
                    COALESCE(MAX(end_checkpoint), 0) as latest_checkpoint,
                    COALESCE(SUM(blob_size), 0) as total_size
                FROM checkpoint_blob_info",
            )
            .get_result::<BlobStats>(conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// List blobs that expire before a given epoch.
    /// Uses the blob_expiration_epoch index for efficient querying.
    pub async fn list_blobs_expiring_before(
        &self,
        epoch: i32,
    ) -> Result<Vec<CheckpointBlobInfoRow>> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(move |conn| {
            checkpoint_blob_info::table
                .filter(checkpoint_blob_info::blob_expiration_epoch.lt(epoch))
                .order(checkpoint_blob_info::blob_expiration_epoch.asc())
                .load(conn)
        })
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

    /// Get all start_checkpoint values from the database.
    /// Useful for finding missing entries during backfill.
    pub async fn get_all_start_checkpoints(&self) -> Result<Vec<i64>> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(|conn| {
            checkpoint_blob_info::table
                .select(checkpoint_blob_info::start_checkpoint)
                .order(checkpoint_blob_info::start_checkpoint.asc())
                .load::<i64>(conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// Batch insert multiple checkpoint blob info records.
    /// This is more efficient than inserting one at a time.
    pub async fn batch_insert_checkpoint_blob_info(
        &self,
        records: Vec<(NewCheckpointBlobInfo, Vec<NewCheckpointIndexEntry>)>,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(move |conn| {
            conn.transaction::<_, diesel::result::Error, _>(|conn| {
                // Collect all blob infos for batch insert
                let blob_infos: Vec<&NewCheckpointBlobInfo> =
                    records.iter().map(|(info, _)| info).collect();

                // Batch insert blob infos with upsert
                for chunk in blob_infos.chunks(100) {
                    for blob_info in chunk {
                        diesel::insert_into(checkpoint_blob_info::table)
                            .values(*blob_info)
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
                                checkpoint_blob_info::blob_size.eq(&blob_info.blob_size),
                            ))
                            .execute(conn)?;
                    }
                }

                // Collect all start_checkpoints for deleting old index entries
                let start_checkpoints: Vec<i64> = records
                    .iter()
                    .map(|(info, _)| info.start_checkpoint)
                    .collect();

                // Delete existing index entries for all blobs in batch
                diesel::delete(
                    checkpoint_index_entry::table.filter(
                        checkpoint_index_entry::start_checkpoint.eq_any(&start_checkpoints),
                    ),
                )
                .execute(conn)?;

                // Collect all index entries and batch insert
                let all_index_entries: Vec<&NewCheckpointIndexEntry> = records
                    .iter()
                    .flat_map(|(_, entries)| entries.iter())
                    .collect();

                if !all_index_entries.is_empty() {
                    for chunk in all_index_entries.chunks(1000) {
                        let owned_chunk: Vec<NewCheckpointIndexEntry> =
                            chunk.iter().map(|&e| e.clone()).collect();
                        diesel::insert_into(checkpoint_index_entry::table)
                            .values(&owned_chunk)
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

    /// Get the inner pool for advanced usage.
    pub fn get_pool(&self) -> &Pool {
        &self.pool
    }

    /// Backfill blob_size for all checkpoint_blob_info records.
    /// blob_size = sum of length_bytes from all checkpoint_index_entry records
    /// with matching start_checkpoint.
    pub async fn backfill_blob_sizes(&self) -> Result<usize> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(|conn| {
            // Use a single UPDATE query with a subquery to compute all blob_sizes at once
            diesel::sql_query(
                "UPDATE checkpoint_blob_info SET blob_size = subquery.total_size
                 FROM (
                     SELECT start_checkpoint, COALESCE(SUM(length_bytes), 0) as total_size
                     FROM checkpoint_index_entry
                     GROUP BY start_checkpoint
                 ) AS subquery
                 WHERE checkpoint_blob_info.start_checkpoint = subquery.start_checkpoint",
            )
            .execute(conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// Get blobs that have NULL blob_size (for incremental backfill).
    pub async fn get_blobs_without_size(&self) -> Result<Vec<i64>> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(|conn| {
            checkpoint_blob_info::table
                .filter(checkpoint_blob_info::blob_size.is_null())
                .select(checkpoint_blob_info::start_checkpoint)
                .order(checkpoint_blob_info::start_checkpoint.asc())
                .load::<i64>(conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
    }

    /// Update blob_size for a single blob.
    pub async fn update_blob_size(&self, start_checkpoint: i64, blob_size: i64) -> Result<()> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(move |conn| {
            diesel::update(checkpoint_blob_info::table.find(start_checkpoint))
                .set(checkpoint_blob_info::blob_size.eq(blob_size))
                .execute(conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))?;

        Ok(())
    }

    /// Calculate blob_size for a specific start_checkpoint by summing length_bytes.
    pub async fn calculate_blob_size(&self, start_checkpoint: i64) -> Result<i64> {
        let conn = self.pool.get().await.context("Failed to get connection")?;

        conn.interact(move |conn| {
            // Use raw SQL to avoid Numeric type issues with SUM
            diesel::sql_query(
                "SELECT COALESCE(SUM(length_bytes), 0)::BIGINT as total FROM checkpoint_index_entry WHERE start_checkpoint = $1"
            )
            .bind::<diesel::sql_types::BigInt, _>(start_checkpoint)
            .get_result::<BlobSizeResult>(conn)
            .map(|r| r.total)
        })
        .await
        .map_err(|e| anyhow::anyhow!("Interact error: {}", e))?
        .map_err(|e| anyhow::anyhow!("Database error: {}", e))
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
