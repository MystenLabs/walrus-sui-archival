// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{IntGauge, Registry};

/// Metrics for the walrus-sui-archival service.
pub struct Metrics {
    /// Latest successfully downloaded checkpoint number.
    pub latest_downloaded_checkpoint: IntGauge,
}

impl Metrics {
    /// Create and register metrics with the provided registry.
    pub fn new(registry: &Registry) -> Self {
        let latest_downloaded_checkpoint = IntGauge::new(
            "latest_downloaded_checkpoint",
            "Latest successfully downloaded checkpoint number",
        )
        .expect("metrics defined at compile time must be valid");

        registry
            .register(Box::new(latest_downloaded_checkpoint.clone()))
            .expect("metrics defined at compile time must be valid");

        Self {
            latest_downloaded_checkpoint,
        }
    }
}
