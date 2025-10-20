// Main JavaScript for the Walrus Sui Archival website.

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
                <p><strong>Metadata Package ID:</strong> <code>${data.metadata_info.contract_package_id}</code></p>
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
 * Initialize the page.
 */
async function init() {
    try {
        const data = await fetchHomepageInfo();
        renderHomepage(data);
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
