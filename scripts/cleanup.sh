#!/bin/bash

set -e

echo "Starting cleanup process..."

# Clear on-chain metadata blob pointer.
echo "Clearing on-chain metadata blob pointer..."
cargo run --release -- clear-metadata-blob-id
echo "Metadata blob pointer cleared."

# Burn all blobs owned by the wallet.
echo "Burning all blobs owned by the wallet..."
cargo run --release -- burn-all-blobs
echo "All blobs burned."

# Remove local directories.
echo "Removing local directories..."
rm -rf archival_db archival_snapshots checkpoint_blobs downloaded_checkpoint_dir
echo "Local directories removed."

echo "Cleanup completed successfully."
