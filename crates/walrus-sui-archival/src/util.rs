// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{path::Path, time::Duration};

use anyhow::{Context, Result};
use serde::Deserialize;
use sui_sdk::{types::base_types::ObjectID as SuiObjectID, wallet_context::WalletContext};
use tokio::fs;
use walrus_core::{BlobId, Epoch};
use walrus_sdk::{
    ObjectID,
    client::{StoreArgs, WalrusNodeClient, responses::BlobStoreResult},
    config::ClientConfig,
    store_optimizations::StoreOptimizations,
    sui::client::{BlobPersistence, PostStoreAction, SuiContractClient},
};

use crate::metrics::Metrics;

#[derive(Deserialize)]
#[allow(dead_code)]
struct MetadataBlobPointer {
    id: SuiObjectID,
    blob_id: Option<Vec<u8>>,
}

/// Hidden reexports for the bin_version macro.
pub mod _hidden {
    pub use const_str::concat;
    pub use git_version::git_version;
}

/// Define constants that hold the git revision and package versions.
///
/// Defines two global `const`s:
///   `GIT_REVISION`: The git revision as specified by the `GIT_REVISION` env
/// variable provided at   compile time, or the current git revision as
/// discovered by running `git describe`.
///
///   `VERSION`: The value of the `CARGO_PKG_VERSION` environment variable
/// concatenated with the   value of `GIT_REVISION`.
///
/// Note: This macro must only be used from a binary, if used inside a library
/// this will fail to compile.
#[macro_export]
macro_rules! bin_version {
    () => {
        $crate::git_revision!();

        const VERSION: &str =
            $crate::_hidden::concat!(env!("CARGO_PKG_VERSION"), "-", GIT_REVISION);
    };
}

/// Defines constant that holds the git revision at build time.
///
///   `GIT_REVISION`: The git revision as specified by the `GIT_REVISION` env
/// variable provided at   compile time, or the current git revision as
/// discovered by running `git describe`.
///
/// Note: This macro must only be used from a binary, if used inside a library
/// this will fail to compile.
#[macro_export]
macro_rules! git_revision {
    () => {
        /// The Git revision obtained through `git describe` at compile time.
        const GIT_REVISION: &str = {
            if let Some(revision) = option_env!("GIT_REVISION") {
                revision
            } else {
                let version = $crate::_hidden::git_version!(
                    args = ["--always", "--abbrev=12", "--dirty", "--exclude", "*"],
                    fallback = ""
                );
                if version.is_empty() {
                    panic!("unable to query git revision");
                }
                version
            }
        };
    };
}

/// Uploads a blob file to Walrus with exponential backoff retry logic.
///
/// Returns tuple of (blob_id, object_id, end_epoch) on success.
pub async fn upload_blob_to_walrus_with_retry(
    walrus_client: &WalrusNodeClient<SuiContractClient>,
    blob_file_path: &Path,
    min_retry_duration: Duration,
    max_retry_duration: Duration,
    store_epoch_length: u32,
    burn_blob: bool,
    metrics: &Metrics,
) -> Result<(BlobId, ObjectID, Epoch)> {
    let blob = fs::read(blob_file_path).await.context("read blob file")?;

    let mut store_args = StoreArgs::default_with_epochs(store_epoch_length);
    store_args.persistence = BlobPersistence::Permanent;

    // TODO(zhewu): check if this is necessary. Currently, we turn off the optimization
    // because we have to have a blob object owned by us so that we can extend it.
    store_args.store_optimizations = StoreOptimizations::none();

    if burn_blob {
        store_args.post_store = PostStoreAction::Burn;
    }

    // Infinite retry with exponential backoff.
    let mut retry_delay = min_retry_duration;
    let max_retry_delay = max_retry_duration;

    loop {
        match walrus_client
            .reserve_and_store_blobs_retry_committees(&[blob.as_slice()], &[], &store_args)
            .await
        {
            Ok(results) => {
                if let Some(blob_store_result) = results.first() {
                    // Check if the blob was successfully stored.
                    if blob_store_result.is_not_stored() {
                        // Track upload failures with not_stored status.
                        metrics.blobs_uploaded_not_stored.inc();

                        tracing::error!(
                            "blob upload failed with is_not_stored status, retrying in {:?}",
                            retry_delay
                        );
                        tokio::time::sleep(retry_delay).await;
                        // Exponential backoff.
                        retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                        continue;
                    }

                    // Track successful uploads.
                    metrics.blobs_uploaded_success.inc();

                    let (blob_id, object_id, end_epoch) = match blob_store_result {
                        BlobStoreResult::NewlyCreated { blob_object, .. } => (
                            blob_object.blob_id,
                            blob_object.id,
                            blob_object.storage.end_epoch,
                        ),
                        _ => {
                            // At this point, we should only have the NewlyCreated result.
                            panic!("unexpected blob store result: {:?}", blob_store_result);
                        }
                    };

                    // Successfully stored.
                    return Ok((blob_id, object_id, end_epoch));
                } else {
                    tracing::error!(
                        "blob upload returned empty results, retrying in {:?}",
                        retry_delay
                    );
                    tokio::time::sleep(retry_delay).await;
                    // Exponential backoff.
                    retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
                }
            }
            Err(e) => {
                // Track upload failures.
                metrics.blobs_uploaded_failed.inc();

                tracing::error!("blob upload failed: {}, retrying in {:?}", e, retry_delay);
                tokio::time::sleep(retry_delay).await;
                // Exponential backoff.
                retry_delay = std::cmp::min(retry_delay * 2, max_retry_delay);
            }
        }
    }
}

/// Fetches the blob ID from the metadata pointer object on-chain.
///
/// Returns the BlobId if it exists, or None if the pointer is not set.
pub async fn fetch_metadata_blob_id(
    client_config_path: impl AsRef<Path>,
    metadata_pointer_object_id: SuiObjectID,
) -> Result<Option<BlobId>> {
    // initialize wallet context.
    let (client_config, _) =
        ClientConfig::load_from_multi_config(client_config_path.as_ref(), Some("testnet"))?;
    let wallet = WalletContext::new(
        client_config
            .wallet_config
            .ok_or_else(|| anyhow::anyhow!("wallet config is required"))?
            .path()
            .ok_or_else(|| anyhow::anyhow!("wallet config path is required"))?,
    )?;

    let sui_client = wallet.get_client().await?;

    // read the metadata pointer object.
    let object_response = sui_client
        .read_api()
        .get_object_with_options(
            metadata_pointer_object_id,
            sui_sdk::rpc_types::SuiObjectDataOptions::new().with_bcs(),
        )
        .await?;

    let object_data = object_response
        .data
        .ok_or_else(|| anyhow::anyhow!("metadata pointer object not found"))?;

    // extract blob_id from the object.
    if let Some(bcs_data) = object_data.bcs {
        if let sui_sdk::rpc_types::SuiRawData::MoveObject(move_obj) = bcs_data {
            // decode BCS to extract the Option<vector<u8>> blob_id field.
            let pointer: MetadataBlobPointer = bcs::from_bytes(&move_obj.bcs_bytes)?;

            if let Some(blob_id_bytes) = pointer.blob_id {
                // convert Vec<u8> to BlobId.
                if blob_id_bytes.len() == 32 {
                    let mut array = [0u8; 32];
                    array.copy_from_slice(&blob_id_bytes);
                    return Ok(Some(BlobId(array)));
                } else {
                    return Err(anyhow::anyhow!(
                        "invalid blob_id length: expected 32 bytes, got {}",
                        blob_id_bytes.len()
                    ));
                }
            } else {
                return Ok(None);
            }
        }
    }

    Err(anyhow::anyhow!(
        "failed to extract blob_id from metadata pointer object"
    ))
}
