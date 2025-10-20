# Walrus Sui Archival Website

This is a static website that displays information from the Walrus Sui Archival system. It fetches data from the REST API and renders it client-side.

## Structure

```
website/
├── index.html          # Main homepage
├── blobs.html          # Blob list page
├── checkpoint.html     # Checkpoint lookup page
├── css/
│   └── style.css       # Stylesheet
├── js/
│   ├── config.js       # Network configuration
│   ├── main.js         # Homepage JavaScript
│   ├── blobs.js        # Blobs page JavaScript
│   └── checkpoint.js   # Checkpoint page JavaScript
├── images/
│   └── walrus_image.png # Walrus logo
└── README.md           # This file
```

## Configuration

To switch between networks, edit `js/config.js`:

```javascript
const CONFIG = {
    network: 'testnet', // Options: 'localnet', 'testnet', 'mainnet'
    // ...
};
```

### Available Networks

- **localnet**: `http://localhost:9185` (for local development)
  - Sui RPC: `https://fullnode.testnet.sui.io:443`
  - Package ID: `0xab933040652f54d198b2f73f3ca8d3ac0d65e5146a8e7911a7a7fb5aa992fa56`
  - Fund Object ID: `0xaf8c83d3888d1584ecfbd8ded69065b9bcc476ff112d1bb403366a0f0f70f266`
  - WAL Coin Type: `0x8270feb7375eee355e64fdb69c50abb6b5f9393a722883c1cf45f8e26048810a::wal::WAL`
- **testnet**: `https://walrus-sui-archival.testnet.walrus.space`
  - Sui RPC: `https://fullnode.testnet.sui.io:443`
  - Package ID: `0xab933040652f54d198b2f73f3ca8d3ac0d65e5146a8e7911a7a7fb5aa992fa56`
  - Fund Object ID: `0xaf8c83d3888d1584ecfbd8ded69065b9bcc476ff112d1bb403366a0f0f70f266`
  - WAL Coin Type: `0x8270feb7375eee355e64fdb69c50abb6b5f9393a722883c1cf45f8e26048810a::wal::WAL`
- **mainnet**: `https://walrus-sui-archival.mainnet.walrus.space`
  - Sui RPC: `https://fullnode.mainnet.sui.io:443`
  - Package ID: Not set yet (placeholder)
  - Fund Object ID: Not set yet (placeholder)
  - WAL Coin Type: Not set yet (placeholder)

## Deployment

### Option 1: Serve Locally

Use any static file server. For example, with Python:

```bash
# Python 3
python3 -m http.server 8000

# Python 2
python -m SimpleHTTPServer 8000
```

Then open http://localhost:8000 in your browser.

### Option 2: Serve with Node.js

```bash
npx http-server .
```

### Option 3: Deploy to Static Hosting

This website can be deployed to any static hosting service:

- **GitHub Pages**: Push to a `gh-pages` branch
- **Netlify**: Drag and drop the `website/` folder
- **Vercel**: Deploy via CLI or web interface
- **AWS S3**: Upload to an S3 bucket with static website hosting enabled
- **Cloudflare Pages**: Connect your repository

## Features

- **Dynamic Data Loading**: Fetches live data from the archival API
- **Network Switching**: Easy configuration to switch between testnet/mainnet
- **Sui TypeScript SDK Integration**: Calls on-chain Move functions to fetch shared fund balance
- **Wallet Standard Integration**: Uses Sui Wallet Standard for universal wallet support
- **Multi-Wallet Support**: Automatic wallet detection with preference for Slush wallet
- **Shared Blob Lifetime Management**: Displays the current WAL balance in the shared fund
- **Contribution Feature**: Contribute WAL tokens directly to the shared fund (max 100 WAL experimental limit)
- **Responsive Design**: Works on desktop and mobile devices
- **Error Handling**: Displays friendly error messages if data cannot be loaded
- **Loading States**: Shows loading spinner while fetching data

## API Endpoints

The website fetches data from the following JSON endpoints:

### Homepage
- `/v1/app_info_for_homepage` - Returns JSON with homepage statistics

### Blobs Page
- `/v1/app_blobs` - Returns JSON array of all blobs

### Checkpoint Page
- `/v1/app_checkpoint?checkpoint=X` - Returns JSON with checkpoint information

### Example Homepage Response

```json
{
  "blob_count": 42,
  "total_checkpoints": 12345,
  "earliest_checkpoint": 1000,
  "latest_checkpoint": 13345,
  "total_size": 5368709120,
  "metadata_info": {
    "metadata_pointer_object_id": "0x123...",
    "contract_package_id": "0x456...",
    "current_metadata_blob_id": "AkV8qvSaD9n9Vjo97sZGGXD8QD1eDC8KNbdRZhQ5aqPc"
  }
}
```

### Example Blobs Response

```json
[
  {
    "blob_id": "AkV8qvSaD9n9Vjo97sZGGXD8QD1eDC8KNbdRZhQ5aqPc",
    "object_id": "0x123...",
    "start_checkpoint": 1000,
    "end_checkpoint": 2000,
    "end_of_epoch": true,
    "expiry_epoch": 100,
    "is_shared_blob": false,
    "entries_count": 1001,
    "total_size": 1048576
  }
]
```

### Example Checkpoint Response

```json
{
  "checkpoint_number": 1234,
  "blob_id": "AkV8qvSaD9n9Vjo97sZGGXD8QD1eDC8KNbdRZhQ5aqPc",
  "object_id": "0x123...",
  "index": 234,
  "offset": 245760,
  "length": 1024
}
```

## Development

### Testing Locally

1. Edit `js/config.js` to point to your local server:
   ```javascript
   endpoints: {
       testnet: 'http://localhost:8080',
       // ...
   }
   ```

2. Start the archival REST API server

3. Serve the website locally (see deployment options above)

### Browser Compatibility

This website uses modern JavaScript features:
- Fetch API
- Async/await
- ES6 syntax

Supported browsers:
- Chrome 55+
- Firefox 52+
- Safari 10.1+
- Edge 15+

## CORS Considerations

If you encounter CORS errors when fetching data, ensure the REST API server has CORS enabled for the domain serving this website.

For local development, you may need to:
1. Use a CORS browser extension
2. Configure the REST API server to allow CORS from `localhost`
3. Use a reverse proxy

## Sui TypeScript SDK Integration

The website uses multiple Mysten Labs packages:
- **`@mysten/sui`**: Core SDK for blockchain interaction
- **`@mysten/wallet-standard`**: Universal wallet connection and signing

All packages are loaded from CDN (esm.sh) for easy browser integration.

### How It Works

The homepage displays the shared fund balance by:

1. Importing the required SDKs from a CDN (esm.sh):
   ```javascript
   import { SuiClient } from 'https://esm.sh/@mysten/sui@1.14.0/client';
   import { Transaction } from 'https://esm.sh/@mysten/sui@1.14.0/transactions';
   import { getWallets } from 'https://esm.sh/@mysten/wallet-standard@0.17.0';
   ```

2. Creating a transaction to call the `archival_blob::get_balance` view function:
   ```javascript
   const tx = new Transaction();
   tx.moveCall({
       target: `${packageId}::archival_blob::get_balance`,
       arguments: [tx.object(fundObjectId)],
   });
   ```

3. Executing the transaction in dev-inspect mode (no gas required):
   ```javascript
   const result = await suiClient.devInspectTransactionBlock({
       transactionBlock: tx,
       sender: '0x0000000000000000000000000000000000000000000000000000000000000000',
   });
   ```

4. Extracting the return value (u64 balance) and converting it to a human-readable format

### Shared Blob Lifetime Management

The archival system uses a shared fund (ArchivalBlobFund) that holds WAL tokens for automatically extending the lifetime of shared blobs. The website displays:

- **Shared Fund Balance**: Current WAL balance in the fund (fetched from on-chain)
- Community members can deposit WAL tokens into this fund to support long-term archival storage

## Wallet Integration & Contribution

### Connecting Your Wallet

1. Click the **"Connect Wallet"** button in the top-right corner
2. Your Sui wallet extension will prompt you to approve the connection
3. Once connected, your wallet address will be displayed (shortened format)

### Supported Wallets

The website uses the Sui Wallet Standard and works with any compliant wallet:
- **Slush** (formerly Sui Wallet - official wallet by Mysten Labs) - **Recommended**
- Suiet
- Ethos Wallet
- Martian Wallet
- And other wallets that implement the Sui Wallet Standard

**Note:** The app will automatically prefer Slush wallet if multiple wallets are installed.

### Contributing to the Shared Fund

Once your wallet is connected:

1. The contribution form will appear in the Shared Blob Lifetime Management section
2. Enter the amount of WAL tokens you want to contribute (max 100 WAL)
3. Click **"Contribute"**
4. Approve the transaction in your wallet
5. Wait for confirmation

**Important Notes:**
- Maximum contribution is 100 WAL per transaction (experimental limit)
- You must have WAL tokens in your wallet
- The transaction will deduct a small amount of SUI for gas fees
- The fund balance will update automatically after successful contribution

### How It Works

The wallet integration uses the **Sui Wallet Standard** (`@mysten/wallet-standard`):

1. **Wallet Discovery**: Automatically detects all installed Sui wallets using `getWallets()`
2. **Connection**: Uses the `standard:connect` feature to establish a connection
3. **Transaction Signing**: Uses `sui:signAndExecuteTransactionBlock` to sign and execute transactions

The contribution process:

1. Fetches your WAL coin objects from the blockchain
2. Creates a transaction that splits the exact amount you want to contribute
3. Calls `archival_blob::deposit` to transfer the tokens to the shared fund
4. Signs and executes the transaction through your wallet using the Wallet Standard API

## License

Copyright (c) Mysten Labs, Inc.
SPDX-License-Identifier: Apache-2.0
