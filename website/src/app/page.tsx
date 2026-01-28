"use client";

import { useState } from "react";
import Link from "next/link";
import Image from "next/image";
import { api, ExpiredBlobInfo } from "@/lib/api";
import { formatSize, formatNumber, formatAddress, CURRENT_NETWORK } from "@/lib/config";
import {
  connectWallet,
  disconnectWallet,
  extendBlobsUsingSharedFund,
} from "@/lib/wallet";
import {
  useHomepageInfo,
  useSharedFundBalance,
  useWalrusEpoch,
  useContribute,
} from "@/lib/queries";
import Modal from "@/components/Modal";

export default function HomePage() {
  // React Query hooks
  const { data: homepageInfo, isLoading, error, refetch } = useHomepageInfo();
  const { data: sharedFundBalance, refetch: refetchBalance } = useSharedFundBalance();
  const { data: currentEpoch } = useWalrusEpoch();
  const contributeMutation = useContribute();

  // Wallet state
  const [walletAddress, setWalletAddress] = useState<string | null>(null);

  // Form state
  const [contributeAmount, setContributeAmount] = useState("");
  const [extendBlobCount, setExtendBlobCount] = useState("10");
  const [isExtending, setIsExtending] = useState(false);
  const [contributionStatus, setContributionStatus] = useState<{
    type: "success" | "error" | "info";
    text: string;
  } | null>(null);
  const [extendStatus, setExtendStatus] = useState<{
    type: "success" | "error" | "info";
    text: string;
  } | null>(null);

  // Modal state for extend blobs confirmation
  const [showExtendModal, setShowExtendModal] = useState(false);
  const [blobsToExtend, setBlobsToExtend] = useState<ExpiredBlobInfo[]>([]);
  const [isExtendingInModal, setIsExtendingInModal] = useState(false);

  const handleConnectWallet = async () => {
    try {
      const account = await connectWallet();
      if (account) {
        setWalletAddress(account.address);
      }
    } catch (err) {
      setContributionStatus({
        type: "error",
        text: err instanceof Error ? err.message : "Failed to connect wallet",
      });
    }
  };

  const handleDisconnectWallet = () => {
    disconnectWallet();
    setWalletAddress(null);
  };

  const handleContribute = async (e: React.FormEvent) => {
    e.preventDefault();
    const amount = parseFloat(contributeAmount);
    if (isNaN(amount) || amount <= 0 || amount > 100) {
      setContributionStatus({ type: "error", text: "Amount must be between 0 and 100 WAL" });
      return;
    }

    try {
      setContributionStatus({ type: "info", text: "Signing transaction..." });
      await contributeMutation.mutateAsync(amount);
      setContributionStatus({ type: "success", text: `Successfully contributed ${amount} WAL!` });
      setContributeAmount("");
    } catch (err) {
      setContributionStatus({
        type: "error",
        text: err instanceof Error ? err.message : "Failed to contribute",
      });
    }
  };

  const handleExtendBlobs = async () => {
    if (!currentEpoch) {
      setExtendStatus({ type: "error", text: "Could not determine current epoch" });
      return;
    }

    const count = parseInt(extendBlobCount, 10);
    if (isNaN(count) || count <= 0 || count > 100) {
      setExtendStatus({ type: "error", text: "Count must be between 1 and 100" });
      return;
    }

    try {
      setIsExtending(true);
      setExtendStatus({ type: "info", text: "Fetching expiring blobs..." });

      // Get blobs expiring within 3 epochs
      const expiringBlobs = await api.getBlobsExpiredBeforeEpoch(currentEpoch + 3);
      const blobs = expiringBlobs.slice(0, count);

      if (blobs.length === 0) {
        setExtendStatus({ type: "info", text: "No blobs need extension" });
        setIsExtending(false);
        return;
      }

      // Show confirmation modal
      setBlobsToExtend(blobs);
      setShowExtendModal(true);
      setIsExtending(false);
      setExtendStatus(null);
    } catch (err) {
      setExtendStatus({
        type: "error",
        text: err instanceof Error ? err.message : "Failed to fetch blobs",
      });
      setIsExtending(false);
    }
  };

  const handleConfirmExtend = async () => {
    try {
      setIsExtendingInModal(true);

      const objectIds = blobsToExtend.map((b) => b.object_id);
      await extendBlobsUsingSharedFund(objectIds);

      // Notify server to refresh blob end epochs
      await api.refreshBlobEndEpoch(objectIds);

      setExtendStatus({
        type: "success",
        text: `Extended ${blobsToExtend.length} blobs by 5 epochs!`,
      });

      // Close modal and refresh data
      setShowExtendModal(false);
      setBlobsToExtend([]);
      refetch();
      refetchBalance();
    } catch (err) {
      setExtendStatus({
        type: "error",
        text: err instanceof Error ? err.message : "Failed to extend blobs",
      });
      setShowExtendModal(false);
    } finally {
      setIsExtendingInModal(false);
    }
  };

  const handleCancelExtend = () => {
    setShowExtendModal(false);
    setBlobsToExtend([]);
    setExtendStatus({ type: "info", text: "Extension cancelled by user" });
  };

  const formatBalance = (balance: bigint): string => {
    const wal = Number(balance) / 1e9;
    return wal.toFixed(4);
  };

  if (isLoading) {
    return (
      <div className="container">
        <div className="loading">
          <div className="spinner"></div>
          <p>loading archival data...</p>
        </div>
      </div>
    );
  }

  if (error && !homepageInfo) {
    return (
      <div className="container">
        <div className="error">
          <h2>Error Loading Data</h2>
          <div className="error-message">
            {error instanceof Error ? error.message : "Failed to load data"}
          </div>
          <button onClick={() => refetch()} className="wallet-btn" style={{ marginTop: "20px" }}>
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="container">
      {/* Wallet section */}
      <div className="wallet-section">
        {walletAddress ? (
          <div className="wallet-info">
            <span className="wallet-address">{formatAddress(walletAddress)}</span>
            <button onClick={handleDisconnectWallet} className="disconnect-btn">
              Disconnect
            </button>
          </div>
        ) : (
          <button onClick={handleConnectWallet} className="wallet-btn">
            Connect Wallet
          </button>
        )}
      </div>

      <h1>Walrus Sui Archival</h1>
      <div className="subtitle">Decentralized Checkpoint Archival System</div>

      {/* Network indicator */}
      <div className="network-indicator">
        Connected to: <span className="network-name">{CURRENT_NETWORK}</span>
      </div>

      {/* Walrus image */}
      <div className="walrus-container">
        <Image
          src="/walrus_image.png"
          alt="Walrus"
          className="walrus-image"
          width={400}
          height={300}
          style={{ display: "block", margin: "0 auto" }}
          onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }}
        />
      </div>

      {/* Stats grid */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-label">Total Blobs</div>
          <div className="stat-value">{homepageInfo ? formatNumber(homepageInfo.blob_count) : "0"}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Total Checkpoints</div>
          <div className="stat-value">{homepageInfo ? formatNumber(homepageInfo.total_checkpoints) : "0"}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Total Size</div>
          <div className="stat-value">{homepageInfo ? formatSize(homepageInfo.total_size) : "0 B"}</div>
        </div>
      </div>

      {/* Navigation Buttons */}
      <div className="nav-buttons">
        <Link href="/blobs" className="nav-button">
          <div className="nav-button-title">View All Blobs</div>
          <div className="nav-button-desc">Browse all archived checkpoint blobs and their metadata</div>
        </Link>
        <Link href="/checkpoint" className="nav-button">
          <div className="nav-button-title">Query Checkpoint</div>
          <div className="nav-button-desc">Look up a specific checkpoint by number</div>
        </Link>
      </div>

      {/* Archival Blob Management section */}
      <div className="shared-blob-section">
        <h2>Archival Blob Management</h2>
        <div className="shared-blob-info">
          <p>
            <strong>Shared Fund Balance:</strong>{" "}
            <span className="fund-value">
              {sharedFundBalance !== undefined ? formatBalance(sharedFundBalance) : "Loading..."}
            </span>{" "}
            WAL
          </p>
          <p className="shared-blob-description">
            The archival system uses a shared fund to automatically extend the lifetime of shared blobs.
            Community members can deposit WAL tokens into this fund to support long-term archival storage.
          </p>
        </div>

        <div className="contribution-grid">
          {/* Contribution section */}
          <div className="contribution-section">
            <h3>Contribute to the Fund</h3>
            {walletAddress ? (
              <>
                <form onSubmit={handleContribute} className="contribution-form">
                  <label htmlFor="contribution-amount">Amount (WAL):</label>
                  <input
                    type="number"
                    id="contribution-amount"
                    min="0"
                    max="100"
                    step="0.01"
                    placeholder="Enter amount"
                    value={contributeAmount}
                    onChange={(e) => setContributeAmount(e.target.value)}
                    disabled={contributeMutation.isPending}
                  />
                  <button
                    type="submit"
                    className="contribute-btn"
                    disabled={contributeMutation.isPending}
                  >
                    Contribute
                  </button>
                </form>
                <p className="contribution-note">
                  <strong>Note:</strong> Experimental limit - maximum 100 WAL per contribution.
                </p>
              </>
            ) : (
              <p className="contribution-note">Connect wallet to contribute</p>
            )}
            {contributionStatus && (
              <div className={`contribution-status ${contributionStatus.type}`}>
                {contributionStatus.text}
              </div>
            )}
          </div>

          {/* Extend Blobs section */}
          <div className="contribution-section">
            <h3>Extend Blobs</h3>
            <p className="shared-blob-description">
              Extend the lifetime of expiring blobs using the shared fund. This will query blobs expiring in the next 3 epochs and extend them.
            </p>
            {walletAddress ? (
              <>
                <div className="contribution-form">
                  <label htmlFor="extend-blob-count">Number of blobs to extend:</label>
                  <input
                    type="number"
                    id="extend-blob-count"
                    min="1"
                    max="100"
                    step="1"
                    value={extendBlobCount}
                    onChange={(e) => setExtendBlobCount(e.target.value)}
                    disabled={isExtending}
                  />
                  <button
                    type="button"
                    className="contribute-btn"
                    onClick={handleExtendBlobs}
                    disabled={isExtending}
                  >
                    Extend Blobs
                  </button>
                </div>
                <p className="contribution-note">
                  <strong>Note:</strong> Each call will extend selected blobs by 5 epochs.
                </p>
              </>
            ) : (
              <p className="contribution-note">Connect wallet to extend blobs</p>
            )}
            {extendStatus && (
              <div className={`contribution-status ${extendStatus.type}`}>
                {extendStatus.text}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Metadata Info */}
      {homepageInfo?.metadata_info && (
        <div className="metadata-section" style={{ display: "block" }}>
          <h2>Metadata Tracking</h2>
          <div className="metadata-info">
            <p>
              <strong>Metadata Pointer:</strong>{" "}
              <code>{formatAddress(homepageInfo.metadata_info.metadata_pointer_object_id, 16)}</code>
            </p>
            {homepageInfo.metadata_info.current_metadata_blob_id && (
              <p>
                <strong>Current Metadata Blob:</strong>{" "}
                <code>{homepageInfo.metadata_info.current_metadata_blob_id}</code>
              </p>
            )}
          </div>
        </div>
      )}

      {/* Technical Documentation Link */}
      <div className="footer-links">
        <Link href="/tech" className="footer-link">
          Technical Documentation
        </Link>
      </div>

      {/* Extend Blobs Confirmation Modal */}
      <Modal
        isOpen={showExtendModal}
        onClose={handleCancelExtend}
        onConfirm={handleConfirmExtend}
        title={`Extend ${blobsToExtend.length} Blob(s)`}
        confirmText="Extend Blobs"
        cancelText="Cancel"
        isLoading={isExtendingInModal}
      >
        <p>The following blobs will be extended by 5 epochs:</p>
        <ul className="blob-list">
          {blobsToExtend.map((blob, index) => (
            <li key={blob.object_id}>
              <span>{index + 1}. </span>
              <span>{blob.blob_id.slice(0, 8)}...{blob.blob_id.slice(-6)}</span>
              <span className="blob-arrow">&rarr;</span>
              <span className="blob-epoch">epoch {blob.end_epoch} &rarr; {blob.end_epoch + 5}</span>
            </li>
          ))}
        </ul>
      </Modal>
    </div>
  );
}
