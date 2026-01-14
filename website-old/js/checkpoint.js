// JavaScript for the checkpoint page.

/**
 * Fetch checkpoint information from the API.
 */
async function fetchCheckpoint(checkpointNumber, showContent) {
    try {
        let url = CONFIG.getUrl(`/v1/app_checkpoint?checkpoint=${checkpointNumber}`);
        if (showContent) {
            url += '&show_content=true';
        }
        console.log('fetching checkpoint from:', url);

        const response = await fetch(url);

        if (!response.ok) {
            if (response.status === 404) {
                throw new Error(`checkpoint ${checkpointNumber} not found`);
            }
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        console.error('error fetching checkpoint:', error);
        throw error;
    }
}

/**
 * Create an interactive JSON tree view.
 */
function createJsonTree(obj, level = 0) {
    const indent = '  '.repeat(level);
    let html = '';

    if (obj === null) {
        return '<span class="json-null">null</span>';
    }

    if (typeof obj !== 'object') {
        if (typeof obj === 'string') {
            return `<span class="json-string">"${obj}"</span>`;
        } else if (typeof obj === 'number') {
            return `<span class="json-number">${obj}</span>`;
        } else if (typeof obj === 'boolean') {
            return `<span class="json-boolean">${obj}</span>`;
        }
        return String(obj);
    }

    const isArray = Array.isArray(obj);
    const entries = Object.entries(obj);

    if (entries.length === 0) {
        return isArray ? '[]' : '{}';
    }

    const toggleId = 'toggle-' + Math.random().toString(36).substr(2, 9);
    const openBracket = isArray ? '[' : '{';
    const closeBracket = isArray ? ']' : '}';

    html += `<span class="json-toggle" onclick="toggleJsonNode('${toggleId}')">▶</span> ${openBracket}\n`;
    html += `<div id="${toggleId}" class="json-collapsed">`;

    entries.forEach(([key, value], index) => {
        const isLast = index === entries.length - 1;
        const comma = isLast ? '' : ',';

        if (typeof value === 'object' && value !== null && (Array.isArray(value) || Object.keys(value).length > 0)) {
            // Nested object or array.
            const keyDisplay = isArray ? `<span class="json-index">[${key}]</span>: ` : `<span class="json-key">"${key}"</span>: `;
            html += `${indent}  ${keyDisplay}`;
            html += createJsonTree(value, level + 1);
            html += `${comma}\n`;
        } else {
            // Primitive value.
            const keyDisplay = isArray ? `<span class="json-index">[${key}]</span>: ` : `<span class="json-key">"${key}"</span>: `;
            html += `${indent}  ${keyDisplay}${createJsonTree(value, level + 1)}${comma}\n`;
        }
    });

    html += `</div>${indent}${closeBracket}`;
    return html;
}

/**
 * Toggle JSON node visibility.
 */
function toggleJsonNode(id) {
    const element = document.getElementById(id);
    const toggle = element.previousElementSibling;

    if (element.classList.contains('json-collapsed')) {
        element.classList.remove('json-collapsed');
        element.classList.add('json-expanded');
        toggle.textContent = '▼';
    } else {
        element.classList.remove('json-expanded');
        element.classList.add('json-collapsed');
        toggle.textContent = '▶';
    }
}

/**
 * Display checkpoint result.
 */
function displayCheckpointResult(data) {
    // Update all fields.
    document.getElementById('checkpoint-number').textContent = data.checkpoint_number;
    document.getElementById('result-checkpoint').textContent = data.checkpoint_number;
    document.getElementById('result-blob-id').textContent = data.blob_id;
    document.getElementById('result-object-id').textContent = data.object_id;
    document.getElementById('result-index').textContent = data.index;
    document.getElementById('result-offset').textContent = data.offset;
    document.getElementById('result-length').textContent = data.length;

    // Display content if available.
    const contentContainer = document.getElementById('content-container');
    if (data.content) {
        const contentDiv = document.getElementById('result-content');
        contentDiv.innerHTML = createJsonTree(data.content);
        contentContainer.style.display = 'block';
    } else {
        contentContainer.style.display = 'none';
    }

    // Show result, hide loading.
    document.getElementById('loading').style.display = 'none';
    document.getElementById('result').style.display = 'block';
    document.getElementById('form-error').style.display = 'none';
}

/**
 * Display error message.
 */
function displayError(error) {
    document.getElementById('loading').style.display = 'none';
    document.getElementById('result').style.display = 'none';
    document.getElementById('form-error').textContent = `Error: ${error.message}`;
    document.getElementById('form-error').style.display = 'block';
}

/**
 * Handle form submission.
 */
async function handleFormSubmit(event) {
    event.preventDefault();

    const checkpointInput = document.getElementById('checkpoint');
    const checkpointNumber = parseInt(checkpointInput.value, 10);
    const showContentCheckbox = document.getElementById('show-content');
    const showContent = showContentCheckbox ? showContentCheckbox.checked : false;

    if (isNaN(checkpointNumber) || checkpointNumber < 0) {
        displayError(new Error('please enter a valid checkpoint number'));
        return;
    }

    // Show loading, hide previous results/errors.
    document.getElementById('loading').style.display = 'block';
    document.getElementById('result').style.display = 'none';
    document.getElementById('form-error').style.display = 'none';

    try {
        const data = await fetchCheckpoint(checkpointNumber, showContent);
        displayCheckpointResult(data);
    } catch (error) {
        displayError(error);
    }
}

/**
 * Initialize the page.
 */
function init() {
    const form = document.getElementById('checkpoint-form');
    form.addEventListener('submit', handleFormSubmit);
}

// Initialize when the page is ready.
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}
