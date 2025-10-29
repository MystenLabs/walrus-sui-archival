// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use tokio::sync::RwLock;

/// In-memory holder for checkpoint data stored as BCS-encoded bytes.
/// Thread-safe container that stores checkpoints by their sequence number.
#[derive(Debug, Clone)]
pub struct InMemoryCheckpointHolder {
    /// Map of checkpoint number to BCS-encoded checkpoint data.
    checkpoints: Arc<RwLock<HashMap<CheckpointSequenceNumber, Vec<u8>>>>,
}

impl InMemoryCheckpointHolder {
    /// Create a new empty in-memory checkpoint holder.
    pub fn new() -> Self {
        Self {
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Store a checkpoint in memory.
    pub async fn store(&self, checkpoint_number: CheckpointSequenceNumber, bcs_data: Vec<u8>) {
        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.insert(checkpoint_number, bcs_data);
    }

    /// Retrieve a checkpoint from memory.
    pub async fn get(&self, checkpoint_number: CheckpointSequenceNumber) -> Option<Vec<u8>> {
        let checkpoints = self.checkpoints.read().await;
        checkpoints.get(&checkpoint_number).cloned()
    }

    /// Remove a checkpoint from memory.
    pub async fn remove(&self, checkpoint_number: CheckpointSequenceNumber) {
        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.remove(&checkpoint_number);
    }

    /// Check if a checkpoint exists in memory.
    pub async fn contains(&self, checkpoint_number: CheckpointSequenceNumber) -> bool {
        let checkpoints = self.checkpoints.read().await;
        checkpoints.contains_key(&checkpoint_number)
    }

    /// Get the number of checkpoints currently stored in memory.
    pub async fn len(&self) -> usize {
        let checkpoints = self.checkpoints.read().await;
        checkpoints.len()
    }

    /// Check if the holder is empty.
    pub async fn is_empty(&self) -> bool {
        let checkpoints = self.checkpoints.read().await;
        checkpoints.is_empty()
    }

    /// Clear all checkpoints from memory.
    pub async fn clear(&self) {
        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.clear();
    }
}

impl Default for InMemoryCheckpointHolder {
    fn default() -> Self {
        Self::new()
    }
}
