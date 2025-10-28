// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use anyhow::Result;
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::SuiAddress;
use tokio::sync::Mutex;
use walrus_sdk::{SuiReadClient, client::WalrusNodeClient, sui::client::SuiContractClient};

/// A client that wraps both WalrusNodeClient and WalletContext with mutex protection.
/// This ensures that walrus_client and wallet cannot be used at the same time.
pub struct SuiInteractiveClient {
    inner: Arc<Mutex<SuiInteractiveClientInner>>,
    pub active_address: SuiAddress,
}

struct SuiInteractiveClientInner {
    walrus_client: WalrusNodeClient<SuiContractClient>,
    wallet: WalletContext,
}

impl SuiInteractiveClient {
    /// Create a new SuiInteractiveClient.
    pub fn new(
        walrus_client: WalrusNodeClient<SuiContractClient>,
        mut wallet: WalletContext,
    ) -> Self {
        let active_address = wallet.active_address().unwrap();
        Self {
            inner: Arc::new(Mutex::new(SuiInteractiveClientInner {
                walrus_client,
                wallet,
            })),
            active_address,
        }
    }

    /// Execute a function with access to the walrus client.
    ///
    /// The mutex is held for the duration of the function execution,
    /// ensuring exclusive access to the inner client.
    pub async fn with_walrus_client<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&WalrusNodeClient<SuiContractClient>) -> Result<R>,
    {
        let inner = self.inner.lock().await;
        f(&inner.walrus_client)
    }

    /// Execute an async function with access to the walrus client.
    ///
    /// The mutex is held for the duration of the async function execution,
    /// ensuring exclusive access to the inner client.
    pub async fn with_walrus_client_async<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(
            &'a WalrusNodeClient<SuiContractClient>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<R>> + Send + 'a>,
        >,
    {
        let inner = self.inner.lock().await;
        f(&inner.walrus_client).await
    }

    /// Execute a function with access to the wallet context.
    ///
    /// The mutex is held for the duration of the function execution,
    /// ensuring exclusive access to the inner wallet.
    pub async fn with_wallet<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&WalletContext) -> Result<R>,
    {
        let inner = self.inner.lock().await;
        f(&inner.wallet)
    }

    /// Execute an async function with access to the wallet context.
    ///
    /// The mutex is held for the duration of the async function execution,
    /// ensuring exclusive access to the inner wallet.
    pub async fn with_wallet_async<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(
            &'a WalletContext,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<R>> + Send + 'a>,
        >,
    {
        let inner = self.inner.lock().await;
        f(&inner.wallet).await
    }

    /// Execute a function with mutable access to the wallet context.
    ///
    /// The mutex is held for the duration of the function execution,
    /// ensuring exclusive access to the inner wallet.
    pub async fn with_wallet_mut<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut WalletContext) -> Result<R>,
    {
        let mut inner = self.inner.lock().await;
        f(&mut inner.wallet)
    }

    /// Execute an async function with mutable access to the wallet context.
    ///
    /// The mutex is held for the duration of the async function execution,
    /// ensuring exclusive access to the inner wallet.
    pub async fn with_wallet_mut_async<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(
            &'a mut WalletContext,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<R>> + Send + 'a>,
        >,
    {
        let mut inner = self.inner.lock().await;
        f(&mut inner.wallet).await
    }

    /// Get the SuiContractClient without holding the lock.
    ///
    /// This method locks briefly to clone the sui_client reference, then releases the lock.
    /// This allows calling sui_client methods without blocking other operations.
    pub async fn get_sui_read_client(&self) -> Arc<SuiReadClient> {
        let inner = self.inner.lock().await;
        inner.walrus_client.sui_client().read_client.clone()
    }
}

impl Clone for SuiInteractiveClient {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            active_address: self.active_address.clone(),
        }
    }
}
