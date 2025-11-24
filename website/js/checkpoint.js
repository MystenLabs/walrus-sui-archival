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
        const contentPre = document.getElementById('result-content');
        contentPre.textContent = JSON.stringify(data.content, null, 2);
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
