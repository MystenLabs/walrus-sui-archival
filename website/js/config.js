// Configuration for the Walrus Sui Archival website.

const CONFIG = {
    // Network configuration.
    // Change this to 'mainnet' or 'testnet' depending on the target network.
    network: 'testnet', // Options: 'testnet', 'mainnet'

    // API endpoints for different networks.
    endpoints: {
        testnet: 'https://walrus-sui-archival.testnet.walrus.space',
        mainnet: 'https://walrus-sui-archival.mainnet.walrus.space',
    },

    // Get the current API endpoint based on the selected network.
    getApiEndpoint() {
        return this.endpoints[this.network];
    },

    // Get the full URL for a given path.
    getUrl(path) {
        return `${this.getApiEndpoint()}${path}`;
    },
};

// Export for use in other modules.
if (typeof module !== 'undefined' && module.exports) {
    module.exports = CONFIG;
}
