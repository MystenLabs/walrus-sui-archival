// Configuration for the Walrus Sui Archival website.

const CONFIG = {
    // Network configuration.
    // Change this to 'localnet', 'testnet', or 'mainnet' depending on the target network.
    network: 'localnet', // Options: 'localnet', 'testnet', 'mainnet'

    // API endpoints for different networks.
    endpoints: {
        localnet: 'http://localhost:9185',
        testnet: 'https://walrus-sui-archival.testnet.walrus.space',
        mainnet: 'https://walrus-sui-archival.mainnet.walrus.space',
    },

    // Sui RPC endpoints for different networks.
    suiRpcEndpoints: {
        localnet: 'https://fullnode.testnet.sui.io:443',
        testnet: 'https://fullnode.testnet.sui.io:443',
        mainnet: 'https://fullnode.mainnet.sui.io:443',
    },

    // Contract package IDs for different networks.
    packageIds: {
        localnet: '0xab933040652f54d198b2f73f3ca8d3ac0d65e5146a8e7911a7a7fb5aa992fa56',
        testnet: '0xab933040652f54d198b2f73f3ca8d3ac0d65e5146a8e7911a7a7fb5aa992fa56',
        mainnet: null, // Placeholder: Not set yet.
    },

    // Fund object IDs for different networks.
    fundObjectIds: {
        localnet: '0xaf8c83d3888d1584ecfbd8ded69065b9bcc476ff112d1bb403366a0f0f70f266',
        testnet: '0xaf8c83d3888d1584ecfbd8ded69065b9bcc476ff112d1bb403366a0f0f70f266',
        mainnet: null, // Placeholder: Not set yet.
    },

    // WAL coin types for different networks.
    walCoinTypes: {
        localnet: '0x8270feb7375eee355e64fdb69c50abb6b5f9393a722883c1cf45f8e26048810a::wal::WAL',
        testnet: '0x8270feb7375eee355e64fdb69c50abb6b5f9393a722883c1cf45f8e26048810a::wal::WAL',
        mainnet: null, // Placeholder: Not set yet.
    },

    // Walrus system object IDs for different networks.
    walrusSystemObjectIds: {
        localnet: '0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af',
        testnet: '0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af',
        mainnet: null, // Placeholder: Not set yet.
    },

    // Get the current API endpoint based on the selected network.
    getApiEndpoint() {
        return this.endpoints[this.network];
    },

    // Get the full URL for a given path.
    getUrl(path) {
        return `${this.getApiEndpoint()}${path}`;
    },

    // Get the Sui RPC endpoint for the current network.
    getSuiRpcEndpoint() {
        return this.suiRpcEndpoints[this.network];
    },

    // Get the package ID for the current network.
    getPackageId() {
        return this.packageIds[this.network];
    },

    // Get the fund object ID for the current network.
    getFundObjectId() {
        return this.fundObjectIds[this.network];
    },

    // Get the WAL coin type for the current network.
    getWalCoinType() {
        return this.walCoinTypes[this.network];
    },

    // Get the Walrus system object ID for the current network.
    getWalrusSystemObjectId() {
        return this.walrusSystemObjectIds[this.network];
    },
};

// Export for use in other modules.
if (typeof module !== 'undefined' && module.exports) {
    module.exports = CONFIG;
}
