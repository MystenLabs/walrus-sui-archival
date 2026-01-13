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

// Pagination state.
let allBlobs = [];
let currentPage = 1;
const ITEMS_PER_PAGE = 500;

/**
 * Render the blobs table for the current page.
 */
function renderBlobs(blobs) {
    allBlobs = blobs;
    renderPage(currentPage);
}

/**
 * Render a specific page of blobs.
 */
function renderPage(page) {
    const contentDiv = document.getElementById('blob-content');
    currentPage = page;

    if (allBlobs.length === 0) {
        contentDiv.innerHTML = '<p>no blobs tracked in the database.</p>';
        return;
    }

    // Calculate pagination.
    const totalPages = Math.ceil(allBlobs.length / ITEMS_PER_PAGE);
    const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
    const endIndex = Math.min(startIndex + ITEMS_PER_PAGE, allBlobs.length);
    const pageBlobs = allBlobs.slice(startIndex, endIndex);

    contentDiv.innerHTML = `<p>showing ${startIndex + 1}-${endIndex} of ${allBlobs.length} blob(s) (page ${currentPage} of ${totalPages}):</p>`;

    // Add pagination controls at the top.
    if (totalPages > 1) {
        contentDiv.innerHTML += renderPaginationControls(currentPage, totalPages);
    }

    // Add note about red blob IDs.
    contentDiv.innerHTML += '<div class="note"><strong>Note:</strong> Blob IDs in <span style="color: #dc3545; font-weight: bold;">red</span> indicate the blob is at the end of an epoch.</div>\n';

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

    for (const blob of pageBlobs) {
        // Wrap blob_id in red if end_of_epoch is true.
        const blobIdDisplay = blob.end_of_epoch
            ? `<span class="end-of-epoch-blob">${blob.blob_id}</span>`
            : blob.blob_id;

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

    // Add pagination controls.
    if (totalPages > 1) {
        contentDiv.innerHTML += renderPaginationControls(currentPage, totalPages);
    }

    // Calculate and display summary for all blobs.
    let totalEntries = 0;
    let totalSize = 0;
    for (const blob of allBlobs) {
        totalEntries += blob.entries_count;
        totalSize += blob.total_size;
    }

    const summaryDiv = document.getElementById('summary');
    summaryDiv.innerHTML = `
        <h2>Summary</h2>
        <p>Total blobs: ${allBlobs.length}</p>
        <p>Total checkpoint entries: ${totalEntries}</p>
        <p>Total data size: ${formatSize(totalSize)}</p>
    `;

    // Hide loading and show content.
    document.getElementById('loading').style.display = 'none';
    document.getElementById('content').style.display = 'block';

    // Scroll to top.
    window.scrollTo(0, 0);
}

/**
 * Render pagination controls.
 */
function renderPaginationControls(current, total) {
    let html = '<div class="pagination">\n';

    // Previous button.
    if (current > 1) {
        html += `<button onclick="renderPage(${current - 1})">« Previous</button>\n`;
    } else {
        html += '<button disabled>« Previous</button>\n';
    }

    // Page numbers - show first, last, current and nearby pages.
    const pageNumbers = [];

    // Always show first page.
    pageNumbers.push(1);

    // Add pages around current page.
    for (let i = Math.max(2, current - 2); i <= Math.min(total - 1, current + 2); i++) {
        if (!pageNumbers.includes(i)) {
            pageNumbers.push(i);
        }
    }

    // Always show last page.
    if (total > 1 && !pageNumbers.includes(total)) {
        pageNumbers.push(total);
    }

    // Render page numbers with ellipsis.
    let prevPage = 0;
    for (const page of pageNumbers) {
        // Add ellipsis if there's a gap.
        if (page - prevPage > 1) {
            html += '<span class="pagination-ellipsis">...</span>\n';
        }

        if (page === current) {
            html += `<button class="active">${page}</button>\n`;
        } else {
            html += `<button onclick="renderPage(${page})">${page}</button>\n`;
        }
        prevPage = page;
    }

    // Next button.
    if (current < total) {
        html += `<button onclick="renderPage(${current + 1})">Next »</button>\n`;
    } else {
        html += '<button disabled>Next »</button>\n';
    }

    html += '</div>\n';
    return html;
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
