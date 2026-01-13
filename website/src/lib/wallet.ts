"use client";

import { getWallets, Wallet, WalletAccount } from "@mysten/wallet-standard";
import { SuiClient, SuiTransactionBlockResponse } from "@mysten/sui/client";
import { Transaction } from "@mysten/sui/transactions";
import { config } from "./config";

let currentWallet: Wallet | null = null;
let currentAccount: WalletAccount | null = null;

interface ConnectFeature {
  connect: () => Promise<{ accounts: WalletAccount[] }>;
}

interface DisconnectFeature {
  disconnect: () => Promise<void>;
}

interface SignAndExecuteFeature {
  signAndExecuteTransaction: (input: {
    transaction: Uint8Array;
    account: WalletAccount;
    chain: string;
  }) => Promise<unknown>;
}

export function getAvailableWallets(): Wallet[] {
  const { get } = getWallets();
  return get().filter((wallet) =>
    wallet.features["standard:connect"] !== undefined
  );
}

export async function connectWallet(preferredWalletName?: string): Promise<WalletAccount | null> {
  const wallets = getAvailableWallets();

  if (wallets.length === 0) {
    throw new Error("No wallets found. Please install a Sui wallet.");
  }

  // Try to find preferred wallet (Slush) or use first available
  let wallet = wallets.find((w) =>
    w.name.toLowerCase().includes(preferredWalletName?.toLowerCase() || "slush")
  );
  if (!wallet) {
    wallet = wallets[0];
  }

  const connectFeature = wallet.features["standard:connect"] as ConnectFeature | undefined;
  if (!connectFeature) {
    throw new Error("Wallet does not support connect feature");
  }

  const result = await connectFeature.connect();
  if (result.accounts.length === 0) {
    throw new Error("No accounts found in wallet");
  }

  currentWallet = wallet;
  currentAccount = result.accounts[0];

  return currentAccount;
}

export function disconnectWallet(): void {
  if (currentWallet && currentWallet.features["standard:disconnect"]) {
    const disconnectFeature = currentWallet.features["standard:disconnect"] as DisconnectFeature;
    disconnectFeature.disconnect();
  }
  currentWallet = null;
  currentAccount = null;
}

export function getCurrentAccount(): WalletAccount | null {
  return currentAccount;
}

export function getSuiClient(): SuiClient {
  return new SuiClient({ url: config.suiRpcUrl });
}

export async function signAndExecuteTransaction(
  tx: Transaction
): Promise<SuiTransactionBlockResponse> {
  if (!currentWallet || !currentAccount) {
    throw new Error("No wallet connected");
  }

  const signFeature = currentWallet.features["sui:signAndExecuteTransaction"] as SignAndExecuteFeature | undefined;
  if (!signFeature) {
    throw new Error("Wallet does not support transaction signing");
  }

  const client = getSuiClient();
  const txBytes = await tx.build({ client });

  const result = await signFeature.signAndExecuteTransaction({
    transaction: txBytes,
    account: currentAccount,
    chain: `sui:${config.suiRpcUrl.includes("mainnet") ? "mainnet" : "testnet"}`,
  });

  return result as SuiTransactionBlockResponse;
}

export async function getSharedFundBalance(): Promise<bigint> {
  const client = getSuiClient();

  const tx = new Transaction();
  tx.moveCall({
    target: `${config.packageId}::archival_blob::get_balance`,
    arguments: [tx.object(config.sharedFundObjectId)],
  });

  try {
    const result = await client.devInspectTransactionBlock({
      transactionBlock: tx,
      sender: "0x0000000000000000000000000000000000000000000000000000000000000000",
    });

    if (result.results && result.results[0]?.returnValues) {
      const returnValue = result.results[0].returnValues[0];
      if (returnValue) {
        // The balance is returned as a u64, which is an 8-byte array (little-endian)
        const balanceBytes = returnValue[0] as number[];
        let balance = 0n;
        for (let i = balanceBytes.length - 1; i >= 0; i--) {
          balance = (balance << 8n) | BigInt(balanceBytes[i]);
        }
        return balance;
      }
    }
  } catch (error) {
    console.error("Failed to get shared fund balance:", error);
  }

  return BigInt(0);
}

export async function getCurrentWalrusEpoch(): Promise<number> {
  const client = getSuiClient();

  try {
    // First, get the Walrus system object
    const systemObject = await client.getObject({
      id: config.walrusSystemObjectId,
      options: { showContent: true },
    });

    if (!systemObject.data?.content) {
      console.error("Failed to fetch Walrus system object");
      return 0;
    }

    // Get the system object's UID
    const fields = (systemObject.data.content as { fields: Record<string, unknown> }).fields;
    const systemObjectUid = (fields.id as { id: string }).id;

    // Fetch the SystemStateInnerV1 dynamic field
    const dynamicFields = await client.getDynamicFields({
      parentId: systemObjectUid,
    });

    if (!dynamicFields.data || dynamicFields.data.length === 0) {
      console.error("Failed to fetch dynamic fields");
      return 0;
    }

    // Find the SystemStateInnerV1 dynamic field (should be the first one)
    const systemStateInnerField = dynamicFields.data[0];

    // Fetch the actual dynamic field object
    const systemStateInner = await client.getDynamicFieldObject({
      parentId: systemObjectUid,
      name: systemStateInnerField.name,
    });

    if (!systemStateInner.data?.content) {
      console.error("Failed to fetch SystemStateInnerV1");
      return 0;
    }

    // Navigate to committee.epoch
    const innerFields = (systemStateInner.data.content as { fields: Record<string, unknown> }).fields;
    const valueFields = (innerFields.value as { fields: Record<string, unknown> }).fields;
    const committee = valueFields.committee as { fields: Record<string, unknown> };
    const epoch = committee.fields.epoch;

    return typeof epoch === "number" ? epoch : parseInt(epoch as string, 10);
  } catch (error) {
    console.error("Failed to get current Walrus epoch:", error);
  }

  return 0;
}

export async function contributeToSharedFund(amountWal: number): Promise<SuiTransactionBlockResponse> {
  if (!currentAccount) {
    throw new Error("No wallet connected");
  }

  if (amountWal <= 0 || amountWal > 100) {
    throw new Error("Amount must be between 0 and 100 WAL");
  }

  const client = getSuiClient();
  const amountMist = BigInt(Math.floor(amountWal * 1e9));

  // Get user's WAL coins
  const coins = await client.getCoins({
    owner: currentAccount.address,
    coinType: config.walCoinType,
  });

  if (coins.data.length === 0) {
    throw new Error("No WAL tokens found in wallet");
  }

  const tx = new Transaction();

  // Split the exact amount from the first coin
  const [depositCoin] = tx.splitCoins(tx.object(coins.data[0].coinObjectId), [
    tx.pure.u64(amountMist),
  ]);

  // Call deposit function
  tx.moveCall({
    target: `${config.packageId}::archival_blob::deposit`,
    arguments: [tx.object(config.sharedFundObjectId), depositCoin],
  });

  return signAndExecuteTransaction(tx);
}

export async function extendBlobsUsingSharedFund(
  blobObjectIds: string[],
  epochsToExtend: number = 5
): Promise<SuiTransactionBlockResponse> {
  if (!currentAccount) {
    throw new Error("No wallet connected");
  }

  if (blobObjectIds.length === 0) {
    throw new Error("No blobs to extend");
  }

  const client = getSuiClient();
  const tx = new Transaction();

  // Get initial shared version for the fund object
  const fundObject = await client.getObject({
    id: config.sharedFundObjectId,
    options: { showOwner: true },
  });

  let fundInitialSharedVersion: string | undefined;
  if (fundObject.data?.owner && typeof fundObject.data.owner === "object" && "Shared" in fundObject.data.owner) {
    fundInitialSharedVersion = fundObject.data.owner.Shared.initial_shared_version.toString();
  }

  // Get initial shared version for the Walrus system object
  const systemObject = await client.getObject({
    id: config.walrusSystemObjectId,
    options: { showOwner: true },
  });

  let systemInitialSharedVersion: string | undefined;
  if (systemObject.data?.owner && typeof systemObject.data.owner === "object" && "Shared" in systemObject.data.owner) {
    systemInitialSharedVersion = systemObject.data.owner.Shared.initial_shared_version.toString();
  }

  // Create shared object refs
  const fundArg = tx.sharedObjectRef({
    objectId: config.sharedFundObjectId,
    initialSharedVersion: fundInitialSharedVersion || "1",
    mutable: true,
  });

  const systemArg = tx.sharedObjectRef({
    objectId: config.walrusSystemObjectId,
    initialSharedVersion: systemInitialSharedVersion || "1",
    mutable: true,
  });

  for (const objectId of blobObjectIds) {
    // Get blob object details
    const blobObject = await client.getObject({
      id: objectId,
      options: { showOwner: true },
    });

    let blobInitialSharedVersion: string | undefined;
    if (blobObject.data?.owner && typeof blobObject.data.owner === "object" && "Shared" in blobObject.data.owner) {
      blobInitialSharedVersion = blobObject.data.owner.Shared.initial_shared_version.toString();
    }

    const blobArg = tx.sharedObjectRef({
      objectId: objectId,
      initialSharedVersion: blobInitialSharedVersion || "1",
      mutable: true,
    });

    tx.moveCall({
      target: `${config.packageId}::archival_blob::extend_shared_blob_using_shared_funds`,
      arguments: [
        fundArg,
        systemArg,
        blobArg,
        tx.pure.u32(epochsToExtend),
      ],
    });
  }

  return signAndExecuteTransaction(tx);
}
