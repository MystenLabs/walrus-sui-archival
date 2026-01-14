// Configuration for the Walrus Sui Archival website.

const CONFIG = {
    // Network configuration.
    // Change this to 'localnet', 'testnet', or 'mainnet' depending on the target network.
    network: 'mainnet', // Options: 'localnet', 'testnet', 'mainnet'

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
        localnet: '0x5066f4e1c7ec56acb4df4f53c7a0ebb0aab06fb161593c6ea4ed8e4b7f91246c',
        testnet: '0x5066f4e1c7ec56acb4df4f53c7a0ebb0aab06fb161593c6ea4ed8e4b7f91246c',
        mainnet: '0x88fe0bdce11ce0c0a8fc37b2e9682e21d05051cd31522ac88dcff3076c051dc6',
    },

    // Fund object IDs for different networks.
    fundObjectIds: {
        localnet: '0x1723afa986fecfcb37067b6ae1ab9a6e29b964b0b56adf7b4145190abf316bf2',
        testnet: '0x1723afa986fecfcb37067b6ae1ab9a6e29b964b0b56adf7b4145190abf316bf2',
        mainnet: '0x1aed91664f6a2a929b795d2b71dd8707f674131367b8ae4650565685e8eeba62',
    },

    // WAL coin types for different networks.
    walCoinTypes: {
        localnet: '0x8270feb7375eee355e64fdb69c50abb6b5f9393a722883c1cf45f8e26048810a::wal::WAL',
        testnet: '0x8270feb7375eee355e64fdb69c50abb6b5f9393a722883c1cf45f8e26048810a::wal::WAL',
        mainnet: '0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59::wal::WAL',
    },

    // Walrus system object IDs for different networks.
    walrusSystemObjectIds: {
        localnet: '0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af',
        testnet: '0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af',
        mainnet: '0x2134d52768ea07e8c43570ef975eb3e4c27a39fa6396bef985b5abc58d03ddd2',
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
