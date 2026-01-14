// Network configuration for Walrus Sui Archival

export type NetworkType = "localnet" | "testnet" | "mainnet";

export interface NetworkConfig {
  apiEndpoint: string;
  suiRpcUrl: string;
  packageId: string;
  sharedFundObjectId: string;
  walCoinType: string;
  walrusSystemObjectId: string;
}

const configs: Record<NetworkType, NetworkConfig> = {
  localnet: {
    apiEndpoint: "http://localhost:9185",
    suiRpcUrl: "https://fullnode.testnet.sui.io:443",
    packageId: "0x5066f4e1c7ec56acb4df4f53c7a0ebb0aab06fb161593c6ea4ed8e4b7f91246c",
    sharedFundObjectId: "0x1723afa986fecfcb37067b6ae1ab9a6e29b964b0b56adf7b4145190abf316bf2",
    walCoinType: "0x8270feb7375eee355e64fdb69c50abb6b5f9393a722883c1cf45f8e26048810a::wal::WAL",
    walrusSystemObjectId: "0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af",
  },
  testnet: {
    apiEndpoint: "https://walrus-sui-archival.testnet.walrus.space",
    suiRpcUrl: "https://fullnode.testnet.sui.io:443",
    packageId: "0x5066f4e1c7ec56acb4df4f53c7a0ebb0aab06fb161593c6ea4ed8e4b7f91246c",
    sharedFundObjectId: "0x1723afa986fecfcb37067b6ae1ab9a6e29b964b0b56adf7b4145190abf316bf2",
    walCoinType: "0x8270feb7375eee355e64fdb69c50abb6b5f9393a722883c1cf45f8e26048810a::wal::WAL",
    walrusSystemObjectId: "0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af",
  },
  mainnet: {
    apiEndpoint: "https://walrus-sui-archival.mainnet.walrus.space",
    suiRpcUrl: "https://fullnode.mainnet.sui.io:443",
    packageId: "0x88fe0bdce11ce0c0a8fc37b2e9682e21d05051cd31522ac88dcff3076c051dc6",
    sharedFundObjectId: "0x1aed91664f6a2a929b795d2b71dd8707f674131367b8ae4650565685e8eeba62",
    walCoinType: "0x356a26eb9e012a68958082340d4c4116e7f55615cf27affcff209cf0ae544f59::wal::WAL",
    walrusSystemObjectId: "0x2134d52768ea07e8c43570ef975eb3e4c27a39fa6396bef985b5abc58d03ddd2",
  },
};

// Default to mainnet
export const CURRENT_NETWORK: NetworkType = "mainnet";
export const config = configs[CURRENT_NETWORK];

export function formatAddress(address: string, length: number = 8): string {
  if (address.length <= length * 2 + 3) return address;
  return `${address.slice(0, length)}...${address.slice(-length)}`;
}

export function formatSize(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
}

export function formatNumber(num: number): string {
  return num.toLocaleString();
}
