// Main JavaScript for the Walrus Sui Archival website.

import { SuiClient } from 'https://esm.sh/@mysten/sui@1.14.0/client';
import { Transaction } from 'https://esm.sh/@mysten/sui@1.14.0/transactions';
import { getWallets } from 'https://esm.sh/@mysten/wallet-standard@0.17.0';

// Global state for wallet.
let currentWallet = null;
let currentAccount = null;

/**
 * Fetch the shared fund balance using Sui TypeScript SDK.
 */
async function fetchSharedFundBalance() {
    try {
        const packageId = CONFIG.getPackageId();
        const fundObjectId = CONFIG.getFundObjectId();

        // Check if package and fund are configured for this network.
        if (!packageId || !fundObjectId) {
            console.log('package ID or fund object ID not configured for this network');
            return null;
        }

        const suiClient = new SuiClient({ url: CONFIG.getSuiRpcEndpoint() });

        // Create a transaction to call the get_balance function.
        const tx = new Transaction();
        tx.moveCall({
            target: `${packageId}::archival_blob::get_balance`,
            arguments: [tx.object(fundObjectId)],
        });

        // Execute in dev-inspect mode (for view functions).
        const result = await suiClient.devInspectTransactionBlock({
            transactionBlock: tx,
            sender: '0x0000000000000000000000000000000000000000000000000000000000000000',
        });

        // Extract the return value from the result.
        if (result.results && result.results[0] && result.results[0].returnValues) {
            const returnValue = result.results[0].returnValues[0];
            // The balance is returned as a u64, which is an 8-byte array.
            const balanceBytes = returnValue[0];
            // Convert bytes to BigInt (little-endian).
            let balance = 0n;
            for (let i = balanceBytes.length - 1; i >= 0; i--) {
                balance = (balance << 8n) | BigInt(balanceBytes[i]);
            }
            return balance;
        }

        return null;
    } catch (error) {
        console.error('error fetching shared fund balance:', error);
        return null;
    }
}

/**
 * Format WAL balance for display.
 */
function formatWalBalance(balance) {
    if (balance === null) {
        return 'Not available';
    }
    // WAL token has 9 decimals.
    const decimals = 9;
    const divisor = 10n ** BigInt(decimals);
    const wholePart = balance / divisor;
    const fractionalPart = balance % divisor;

    // Format with 2 decimal places.
    const formatted = wholePart.toString() + '.' + fractionalPart.toString().padStart(decimals, '0').slice(0, 2);
    return formatted;
}

/**
 * Fetch the current Walrus epoch from the system object.
 */
async function fetchWalrusEpoch() {
    try {
        const walrusSystemObjectId = CONFIG.getWalrusSystemObjectId();

        if (!walrusSystemObjectId) {
            console.log('walrus system object ID not configured for this network');
            return null;
        }

        const suiClient = new SuiClient({ url: CONFIG.getSuiRpcEndpoint() });

        // Fetch the Walrus system object.
        const systemObject = await suiClient.getObject({
            id: walrusSystemObjectId,
            options: {
                showContent: true,
            },
        });

        if (!systemObject.data || !systemObject.data.content) {
            console.error('failed to fetch Walrus system object');
            return null;
        }

        console.log('system object:', systemObject.data.content);

        // Get the system object's UID.
        const fields = systemObject.data.content.fields;
        const systemObjectUid = fields.id.id;

        console.log('system object UID:', systemObjectUid);

        // Fetch the SystemStateInnerV1 dynamic field.
        const dynamicFields = await suiClient.getDynamicFields({
            parentId: systemObjectUid,
        });

        console.log('dynamic fields:', dynamicFields);

        if (!dynamicFields.data || dynamicFields.data.length === 0) {
            console.error('failed to fetch dynamic fields');
            return null;
        }

        // Find the SystemStateInnerV1 dynamic field (should be the first one).
        const systemStateInnerField = dynamicFields.data[0];

        console.log('system state inner field:', systemStateInnerField);

        // Fetch the actual dynamic field object.
        const systemStateInner = await suiClient.getDynamicFieldObject({
            parentId: systemObjectUid,
            name: systemStateInnerField.name,
        });

        console.log('system state inner:', systemStateInner);

        if (!systemStateInner.data || !systemStateInner.data.content) {
            console.error('failed to fetch SystemStateInnerV1');
            return null;
        }

        // Navigate to committee.epoch.
        const innerFields = systemStateInner.data.content.fields;
        const committee = innerFields.value.fields.committee;
        const epoch = committee.fields.epoch;

        console.log('current Walrus epoch:', epoch);
        return parseInt(epoch);
    } catch (error) {
        console.error('error fetching Walrus epoch:', error);
        console.error('error stack:', error.stack);
        return null;
    }
}

/**
 * Fetch blobs expiring before a given epoch.
 */
async function fetchExpiringBlobs(epoch) {
    try {
        const url = CONFIG.getUrl(`/v1/app_blobs_expired_before_epoch?epoch=${epoch}`);
        console.log('fetching expiring blobs from:', url);

        const response = await fetch(url);

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        console.error('error fetching expiring blobs:', error);
        throw error;
    }
}

/**
 * Refresh blob end epochs by sending object IDs to the server.
 */
async function refreshBlobEndEpochs(objectIds) {
    try {
        const url = CONFIG.getUrl('/v1/app_refresh_blob_end_epoch');
        console.log('refreshing blob end epochs for', objectIds.length, 'blobs');

        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                object_ids: objectIds,
            }),
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        console.log('blob refresh response:', data);
        return data;
    } catch (error) {
        console.error('error refreshing blob end epochs:', error);
        throw error;
    }
}

/**
 * Fetch homepage information from the API.
 */
async function fetchHomepageInfo() {
    try {
        const url = CONFIG.getUrl('/v1/app_info_for_homepage');
        console.log('fetching homepage info from:', url);

        const response = await fetch(url);

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        console.error('error fetching homepage info:', error);
        throw error;
    }
}

/**
 * Format size in bytes to human-readable format.
 */
function formatSize(bytes) {
    const GB = 1024 * 1024 * 1024;
    return (bytes / GB).toFixed(2);
}

/**
 * Render the homepage with the fetched data.
 */
function renderHomepage(data) {
    // Update stats.
    document.getElementById('blob-count').textContent = data.blob_count || 0;
    document.getElementById('total-checkpoints').textContent = data.total_checkpoints || 0;
    document.getElementById('checkpoint-range').textContent =
        `${data.earliest_checkpoint || 0} - ${data.latest_checkpoint || 0}`;
    document.getElementById('total-size').textContent = formatSize(data.total_size || 0);

    // Update metadata section if available.
    const metadataSection = document.getElementById('metadata-section');
    if (data.metadata_info) {
        const blobIdDisplay = data.metadata_info.current_metadata_blob_id
            ? `<code>${data.metadata_info.current_metadata_blob_id}</code>`
            : '<em style="color: #999;">Not set</em>';

        metadataSection.innerHTML = `
            <h2>📋 Metadata Tracking</h2>
            <div class="metadata-info">
                <p><strong>On-Chain Metadata Pointer:</strong> <code>${data.metadata_info.metadata_pointer_object_id}</code></p>
                <p><strong>Current Metadata Blob ID:</strong> ${blobIdDisplay}</p>
                <p class="metadata-description">
                    The archival system maintains an on-chain metadata blob that contains a snapshot
                    of all checkpoint blob information. This enables disaster recovery and quick
                    bootstrapping of new archival nodes.
                </p>
            </div>
        `;
        metadataSection.style.display = 'block';
    } else {
        metadataSection.style.display = 'none';
    }

    // Update network indicator.
    document.getElementById('network-name').textContent = CONFIG.network;

    // Hide loading indicator and show content.
    document.getElementById('loading').style.display = 'none';
    document.getElementById('content').style.display = 'block';
}

/**
 * Display error message.
 */
function displayError(error) {
    document.getElementById('loading').style.display = 'none';
    document.getElementById('error').style.display = 'block';
    document.getElementById('error-message').textContent = error.message || 'Unknown error occurred';
}

/**
 * Connect to Sui wallet using Wallet Standard.
 */
async function connectWallet() {
    try {
        // Get all registered wallets.
        const wallets = getWallets();
        const availableWallets = wallets.get();

        if (availableWallets.length === 0) {
            alert('No Sui wallet detected. Please install Slush, Suiet, or another Sui wallet.');
            return;
        }

        // For simplicity, connect to the first available wallet.
        // In a production app, you might want to show a wallet selection UI.
        let selectedWallet = availableWallets[0];

        // Prefer Slush wallet if available.
        const slushWallet = availableWallets.find(w => w.name.toLowerCase().includes('slush') || w.name.toLowerCase().includes('sui wallet'));
        if (slushWallet) {
            selectedWallet = slushWallet;
        }

        console.log('connecting to wallet:', selectedWallet.name);

        // Connect to the wallet using standard:connect feature.
        const connectFeature = selectedWallet.features['standard:connect'];
        if (!connectFeature) {
            alert('Wallet does not support connection');
            return;
        }

        // Request connection.
        const result = await connectFeature.connect();

        if (result.accounts && result.accounts.length > 0) {
            currentWallet = selectedWallet;
            currentAccount = result.accounts[0];

            console.log('connected to account:', currentAccount.address);

            updateWalletUI();
        }
    } catch (error) {
        console.error('error connecting wallet:', error);
        alert('Failed to connect wallet: ' + error.message);
    }
}

/**
 * Disconnect wallet.
 */
async function disconnectWallet() {
    try {
        // Call the wallet's disconnect feature if available.
        if (currentWallet && currentWallet.features['standard:disconnect']) {
            const disconnectFeature = currentWallet.features['standard:disconnect'];
            await disconnectFeature.disconnect();
        }
    } catch (error) {
        console.error('error disconnecting wallet:', error);
    } finally {
        // Clear local state regardless of whether disconnect succeeded.
        currentWallet = null;
        currentAccount = null;
        updateWalletUI();
    }
}

/**
 * Update wallet UI based on connection state.
 */
function updateWalletUI() {
    const connectBtn = document.getElementById('connect-wallet-btn');
    const walletInfo = document.getElementById('wallet-info');
    const walletAddress = document.getElementById('wallet-address');

    if (currentAccount) {
        // Wallet connected.
        connectBtn.style.display = 'none';
        walletInfo.style.display = 'flex';

        // Show shortened address.
        const addr = currentAccount.address;
        const shortened = addr.slice(0, 6) + '...' + addr.slice(-4);
        walletAddress.textContent = shortened;
        walletAddress.title = addr;
    } else {
        // Wallet disconnected.
        connectBtn.style.display = 'block';
        walletInfo.style.display = 'none';
    }
}

/**
 * Contribute to the shared fund.
 */
async function contribute() {
    const amountInput = document.getElementById('contribution-amount');
    const contributeBtn = document.getElementById('contribute-btn');
    const statusDiv = document.getElementById('contribution-status');

    // Check wallet connection first.
    if (!currentAccount) {
        showStatus('Please connect your wallet first', 'error');
        return;
    }

    const amount = parseFloat(amountInput.value);

    // Validation.
    if (isNaN(amount) || amount <= 0) {
        showStatus('Please enter a valid amount', 'error');
        return;
    }

    if (amount > 100) {
        showStatus('Maximum contribution is 100 WAL (experimental limit)', 'error');
        return;
    }

    try {
        contributeBtn.disabled = true;
        showStatus('Preparing transaction...', 'info');

        const packageId = CONFIG.getPackageId();
        const fundObjectId = CONFIG.getFundObjectId();
        const walCoinType = CONFIG.getWalCoinType();

        if (!packageId || !fundObjectId || !walCoinType) {
            showStatus('Configuration not available for this network', 'error');
            contributeBtn.disabled = false;
            return;
        }

        const suiClient = new SuiClient({ url: CONFIG.getSuiRpcEndpoint() });

        // Convert amount to MIST (9 decimals for WAL).
        const decimals = 9;
        const amountInMist = BigInt(Math.floor(amount * Math.pow(10, decimals)));

        // Get user's WAL coins.
        const coins = await suiClient.getCoins({
            owner: currentAccount.address,
            coinType: walCoinType,
        });

        if (coins.data.length === 0) {
            showStatus('No WAL tokens found in your wallet', 'error');
            contributeBtn.disabled = false;
            return;
        }

        // Create transaction.
        const tx = new Transaction();

        // Split or merge coins to get the exact amount.
        const [paymentCoin] = tx.splitCoins(tx.object(coins.data[0].coinObjectId), [amountInMist]);

        // Call deposit function.
        tx.moveCall({
            target: `${packageId}::archival_blob::deposit`,
            arguments: [
                tx.object(fundObjectId),
                paymentCoin,
            ],
        });

        showStatus('Waiting for wallet approval...', 'info');

        // Get the signAndExecuteTransactionBlock feature from the wallet.
        const signAndExecuteFeature = currentWallet.features['sui:signAndExecuteTransactionBlock'];
        if (!signAndExecuteFeature) {
            showStatus('Wallet does not support transaction signing', 'error');
            contributeBtn.disabled = false;
            return;
        }

        // Sign and execute transaction using Wallet Standard.
        const result = await signAndExecuteFeature.signAndExecuteTransactionBlock({
            transactionBlock: tx,
            account: currentAccount,
            chain: CONFIG.network === 'mainnet' ? 'sui:mainnet' : 'sui:testnet',
            options: {
                showEffects: true,
            },
        });

        if (result.effects?.status?.status === 'success') {
            showStatus(`Successfully contributed ${amount} WAL! Updating balance...`, 'success');

            // Clear input.
            amountInput.value = '';

            // Refresh fund balance immediately.
            try {
                const fundBalance = await fetchSharedFundBalance();
                const fundBalanceElement = document.getElementById('fund-balance');
                fundBalanceElement.textContent = formatWalBalance(fundBalance);

                // Update status with final message.
                showStatus(`Successfully contributed ${amount} WAL! Transaction: ${result.digest}`, 'success');
            } catch (error) {
                console.error('error refreshing balance:', error);
                showStatus(`Successfully contributed ${amount} WAL! Transaction: ${result.digest} (balance will update shortly)`, 'success');
            }
        } else {
            showStatus('Transaction failed: ' + (result.effects?.status?.error || 'Unknown error'), 'error');
        }
    } catch (error) {
        console.error('contribution error:', error);
        showStatus('Error: ' + error.message, 'error');
    } finally {
        contributeBtn.disabled = false;
    }
}

/**
 * Show status message.
 */
function showStatus(message, type) {
    const statusDiv = document.getElementById('contribution-status');
    statusDiv.textContent = message;
    statusDiv.className = `contribution-status ${type}`;
}

/**
 * Show status message for extend blobs.
 */
function showExtendStatus(message, type) {
    const statusDiv = document.getElementById('extend-blobs-status');
    statusDiv.textContent = message;
    statusDiv.className = `contribution-status ${type}`;
}

/**
 * Extend blobs using the shared fund.
 */
async function extendBlobs() {
    const countInput = document.getElementById('extend-blob-count');
    const extendBtn = document.getElementById('extend-blobs-btn');

    // Check wallet connection first.
    if (!currentAccount) {
        showExtendStatus('Please connect your wallet first', 'error');
        return;
    }

    const count = parseInt(countInput.value);

    // Validation.
    if (isNaN(count) || count <= 0) {
        showExtendStatus('Please enter a valid number of blobs', 'error');
        return;
    }

    if (count > 100) {
        showExtendStatus('Maximum 100 blobs can be extended at once', 'error');
        return;
    }

    try {
        extendBtn.disabled = true;
        showExtendStatus('Fetching current Walrus epoch...', 'info');

        // Get configuration.
        const packageId = CONFIG.getPackageId();
        const fundObjectId = CONFIG.getFundObjectId();
        const walrusSystemObjectId = CONFIG.getWalrusSystemObjectId();

        if (!packageId || !fundObjectId || !walrusSystemObjectId) {
            showExtendStatus('Configuration not available for this network', 'error');
            extendBtn.disabled = false;
            return;
        }

        // Fetch the current Walrus epoch.
        const currentEpoch = await fetchWalrusEpoch();
        if (currentEpoch === null) {
            showExtendStatus('Failed to fetch current Walrus epoch', 'error');
            extendBtn.disabled = false;
            return;
        }

        showExtendStatus(`Current epoch: ${currentEpoch}. Fetching expiring blobs...`, 'info');

        // Fetch blobs expiring before epoch + 3.
        const targetEpoch = currentEpoch + 3;
        const expiringBlobs = await fetchExpiringBlobs(targetEpoch);

        if (!expiringBlobs || expiringBlobs.length === 0) {
            showExtendStatus(`No blobs found expiring before epoch ${targetEpoch}`, 'info');
            extendBtn.disabled = false;
            return;
        }

        // Limit to the requested count.
        const blobsToExtend = expiringBlobs.slice(0, count);

        // Display the list of blobs that will be extended for confirmation.
        console.log('=== Blobs to be Extended ===');
        console.log(`Total expiring blobs found: ${expiringBlobs.length}`);
        console.log(`Blobs to extend: ${blobsToExtend.length}`);
        console.log('---');

        let confirmationMessage = `Will extend ${blobsToExtend.length} blob(s) by 5 epochs:\n\n`;

        for (let i = 0; i < blobsToExtend.length; i++) {
            const blob = blobsToExtend[i];
            const blobIdShort = blob.blob_id.slice(0, 8) + '...' + blob.blob_id.slice(-6);
            console.log(`${i + 1}. Blob ID: ${blob.blob_id}`);
            console.log(`   Current end epoch: ${blob.end_epoch}`);
            console.log(`   Will extend to: ${blob.end_epoch + 5}`);
            console.log('---');

            confirmationMessage += `${i + 1}. ${blobIdShort} (epoch ${blob.end_epoch} → ${blob.end_epoch + 5})\n`;
        }

        showExtendStatus(`Found ${expiringBlobs.length} expiring blob(s). Preparing to extend ${blobsToExtend.length} blob(s)...`, 'info');

        // Show confirmation dialog.
        const confirmed = confirm(confirmationMessage + '\nDo you want to proceed?');

        if (!confirmed) {
            showExtendStatus('Extension cancelled by user', 'info');
            extendBtn.disabled = false;
            return;
        }

        showExtendStatus('Building transaction...', 'info');

        const suiClient = new SuiClient({ url: CONFIG.getSuiRpcEndpoint() });

        // Get the Walrus system object to find its initial shared version.
        const systemObject = await suiClient.getObject({
            id: walrusSystemObjectId,
            options: {
                showOwner: true,
            },
        });

        if (!systemObject.data || !systemObject.data.owner || systemObject.data.owner.Shared?.initial_shared_version === undefined) {
            showExtendStatus('Failed to fetch Walrus system object details', 'error');
            extendBtn.disabled = false;
            return;
        }

        const systemInitialSharedVersion = systemObject.data.owner.Shared.initial_shared_version;

        // Fetch fund object details.
        const fundObject = await suiClient.getObject({
            id: fundObjectId,
            options: {
                showOwner: true,
            },
        });

        if (!fundObject.data || !fundObject.data.owner || fundObject.data.owner.Shared?.initial_shared_version === undefined) {
            showExtendStatus('Failed to fetch fund object details', 'error');
            extendBtn.disabled = false;
            return;
        }

        const fundInitialSharedVersion = fundObject.data.owner.Shared.initial_shared_version;

        // Build the PTB.
        const tx = new Transaction();

        // Add fund and system objects as shared objects.
        const fundArg = tx.sharedObjectRef({
            objectId: fundObjectId,
            initialSharedVersion: fundInitialSharedVersion,
            mutable: true,
        });

        const systemArg = tx.sharedObjectRef({
            objectId: walrusSystemObjectId,
            initialSharedVersion: systemInitialSharedVersion,
            mutable: true,
        });

        // For each blob, fetch its details and add extend call.
        for (const blob of blobsToExtend) {
            const blobObjectId = blob.object_id;

            console.log('blob object ID:', blobObjectId);

            // Fetch blob object details to get initial shared version.
            const blobObject = await suiClient.getObject({
                id: blobObjectId,
                options: {
                    showOwner: true,
                },
            });

            if (!blobObject.data || !blobObject.data.owner || blobObject.data.owner.Shared?.initial_shared_version === undefined) {
                console.warn(`skipping blob ${blobObjectId}: failed to fetch details`);
                continue;
            }

            const blobInitialSharedVersion = blobObject.data.owner.Shared.initial_shared_version;

            const blobArg = tx.sharedObjectRef({
                objectId: blobObjectId,
                initialSharedVersion: blobInitialSharedVersion,
                mutable: true,
            });

            // Call extend_shared_blob_using_shared_funds.
            // Extend by 5 epochs.
            tx.moveCall({
                target: `${packageId}::archival_blob::extend_shared_blob_using_shared_funds`,
                arguments: [
                    fundArg,
                    systemArg,
                    blobArg,
                    tx.pure.u32(5), // Extended epochs.
                ],
            });
        }

        showExtendStatus('Waiting for wallet approval...', 'info');

        // Get the signAndExecuteTransactionBlock feature from the wallet.
        const signAndExecuteFeature = currentWallet.features['sui:signAndExecuteTransactionBlock'];
        if (!signAndExecuteFeature) {
            showExtendStatus('Wallet does not support transaction signing', 'error');
            extendBtn.disabled = false;
            return;
        }

        // Sign and execute transaction.
        const result = await signAndExecuteFeature.signAndExecuteTransactionBlock({
            transactionBlock: tx,
            account: currentAccount,
            chain: CONFIG.network === 'mainnet' ? 'sui:mainnet' : 'sui:testnet',
            options: {
                showEffects: true,
            },
        });

        if (result.effects?.status?.status === 'success') {
            showExtendStatus(`Successfully extended ${blobsToExtend.length} blob(s)! Refreshing blob data...`, 'success');

            // Clear input.
            countInput.value = '10';

            // Send the list of extended blobs to the server to refresh their end epochs.
            try {
                const objectIds = blobsToExtend.map(blob => blob.object_id);
                await refreshBlobEndEpochs(objectIds);
                showExtendStatus(`Successfully extended ${blobsToExtend.length} blob(s)! Transaction: ${result.digest}`, 'success');
            } catch (error) {
                console.error('error refreshing blob end epochs:', error);
                showExtendStatus(`Successfully extended ${blobsToExtend.length} blob(s)! Transaction: ${result.digest} (blob data will refresh shortly)`, 'success');
            }
        } else {
            showExtendStatus('Transaction failed: ' + (result.effects?.status?.error || 'Unknown error'), 'error');
        }
    } catch (error) {
        console.error('extend blobs error:', error);
        showExtendStatus('Error: ' + error.message, 'error');
    } finally {
        extendBtn.disabled = false;
    }
}

/**
 * Initialize the page.
 */
async function init() {
    try {
        // Fetch both homepage info and fund balance in parallel.
        const [data, fundBalance] = await Promise.all([
            fetchHomepageInfo(),
            fetchSharedFundBalance(),
        ]);

        renderHomepage(data);

        // Update fund balance display.
        const fundBalanceElement = document.getElementById('fund-balance');
        fundBalanceElement.textContent = formatWalBalance(fundBalance);

        // Setup wallet event listeners.
        document.getElementById('connect-wallet-btn').addEventListener('click', connectWallet);
        document.getElementById('disconnect-wallet-btn').addEventListener('click', disconnectWallet);
        document.getElementById('contribute-btn').addEventListener('click', contribute);
        document.getElementById('extend-blobs-btn').addEventListener('click', extendBlobs);

        // Initialize wallet UI.
        updateWalletUI();
    } catch (error) {
        displayError(error);
    }
}

// Load data when the page is ready.
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}
