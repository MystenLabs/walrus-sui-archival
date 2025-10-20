// JavaScript for the blobs page.

/**
 * Format size in bytes to human-readable format.
 */
function formatSize(bytes) {
    const KB = 1024;
    const MB = KB * 1024;
    const GB = MB * 1024;
    const TB = GB * 1024;

    if (bytes >= TB) {
        return `${(bytes / TB).toFixed(2)} TB`;
    } else if (bytes >= GB) {
        return `${(bytes / GB).toFixed(2)} GB`;
    } else if (bytes >= MB) {
        return `${(bytes / MB).toFixed(2)} MB`;
    } else if (bytes >= KB) {
        return `${(bytes / KB).toFixed(2)} KB`;
    } else {
        return `${bytes} B`;
    }
}

/**
 * Fetch all blobs from the API.
 */
async function fetchBlobs() {
    try {
        const url = CONFIG.getUrl('/v1/app_blobs');
        console.log('fetching blobs from:', url);

        const response = await fetch(url);

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        console.error('error fetching blobs:', error);
        throw error;
    }
}

/**
 * Render the blobs table.
 */
function renderBlobs(blobs) {
    const contentDiv = document.getElementById('blob-content');

    if (blobs.length === 0) {
        contentDiv.innerHTML = '<p>no blobs tracked in the database.</p>';
        return;
    }

    contentDiv.innerHTML = `<p>found ${blobs.length} blob(s) in the database:</p>`;

    // Add note about asterisk.
    contentDiv.innerHTML += '<div class="note"><strong>Note:</strong> * indicates the blob is at the end of an epoch.</div>\n';

    // Create table with container for horizontal scrolling.
    let tableHtml = '<div class="table-container">\n';
    tableHtml += '<table>\n';
    tableHtml += '<tr>\n';
    tableHtml += '<th>Blob ID</th>\n';
    tableHtml += '<th>Object ID</th>\n';
    tableHtml += '<th>Start Checkpoint</th>\n';
    tableHtml += '<th>End Checkpoint</th>\n';
    tableHtml += '<th>Expiry Epoch</th>\n';
    tableHtml += '<th>Entries</th>\n';
    tableHtml += '<th>Size</th>\n';
    tableHtml += '</tr>\n';

    let totalEntries = 0;
    let totalSize = 0;

    for (const blob of blobs) {
        totalEntries += blob.entries_count;
        totalSize += blob.total_size;

        // Add * prefix to blob_id if end_of_epoch is true.
        const blobIdDisplay = blob.end_of_epoch ? `*${blob.blob_id}` : blob.blob_id;

        tableHtml += '<tr>\n';
        tableHtml += `<td>${blobIdDisplay}</td>\n`;
        tableHtml += `<td>${blob.object_id}</td>\n`;
        tableHtml += `<td>${blob.start_checkpoint}</td>\n`;
        tableHtml += `<td>${blob.end_checkpoint}</td>\n`;
        tableHtml += `<td>${blob.expiry_epoch}</td>\n`;
        tableHtml += `<td>${blob.entries_count}</td>\n`;
        tableHtml += `<td>${formatSize(blob.total_size)}</td>\n`;
        tableHtml += '</tr>\n';
    }

    tableHtml += '</table>\n';
    tableHtml += '</div>\n';
    contentDiv.innerHTML += tableHtml;

    // Add summary.
    const summaryDiv = document.getElementById('summary');
    summaryDiv.innerHTML = `
        <h2>Summary</h2>
        <p>Total blobs: ${blobs.length}</p>
        <p>Total checkpoint entries: ${totalEntries}</p>
        <p>Total data size: ${formatSize(totalSize)}</p>
    `;

    // Hide loading and show content.
    document.getElementById('loading').style.display = 'none';
    document.getElementById('content').style.display = 'block';
}

/**
 * Display error message.
 */
function displayError(error) {
    document.getElementById('loading').style.display = 'none';
    document.getElementById('error').style.display = 'block';
    document.getElementById('error-message').textContent = error.message || 'unknown error occurred';
}

/**
 * Initialize the page.
 */
async function init() {
    try {
        const blobs = await fetchBlobs();
        renderBlobs(blobs);
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
