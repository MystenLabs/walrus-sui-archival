-- Create checkpoint_blob_info table
CREATE TABLE IF NOT EXISTS checkpoint_blob_info (
    -- Primary key: start_checkpoint is the unique identifier for each blob
    start_checkpoint BIGINT PRIMARY KEY,

    -- End checkpoint (inclusive)
    end_checkpoint BIGINT NOT NULL,

    -- Blob identifier (stored as hex string for easier debugging)
    blob_id VARCHAR(128) NOT NULL,

    -- Object ID (stored as hex string)
    object_id VARCHAR(66) NOT NULL,

    -- Whether this blob contains the last transaction of the Sui epoch
    end_of_epoch BOOLEAN NOT NULL DEFAULT FALSE,

    -- Walrus epoch when this blob expires
    blob_expiration_epoch INTEGER NOT NULL,

    -- Whether this blob is wrapped as SharedArchivalBlob
    is_shared_blob BOOLEAN NOT NULL DEFAULT FALSE,

    -- Version of the format
    version INTEGER NOT NULL DEFAULT 1,

    -- Timestamps for tracking
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create index entries table for checkpoint lookups
CREATE TABLE IF NOT EXISTS checkpoint_index_entry (
    -- Composite primary key
    start_checkpoint BIGINT NOT NULL,
    checkpoint_number BIGINT NOT NULL,

    -- Offset and length within the blob
    offset_bytes BIGINT NOT NULL,
    length_bytes BIGINT NOT NULL,

    PRIMARY KEY (start_checkpoint, checkpoint_number),

    -- Foreign key to parent blob
    FOREIGN KEY (start_checkpoint) REFERENCES checkpoint_blob_info(start_checkpoint) ON DELETE CASCADE
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_checkpoint_blob_info_end_checkpoint ON checkpoint_blob_info(end_checkpoint);
CREATE INDEX IF NOT EXISTS idx_checkpoint_blob_info_blob_id ON checkpoint_blob_info(blob_id);
CREATE INDEX IF NOT EXISTS idx_checkpoint_blob_info_object_id ON checkpoint_blob_info(object_id);
CREATE INDEX IF NOT EXISTS idx_checkpoint_blob_info_created_at ON checkpoint_blob_info(created_at);

-- Index for finding checkpoint by number (useful for range queries)
CREATE INDEX IF NOT EXISTS idx_checkpoint_index_entry_checkpoint_number ON checkpoint_index_entry(checkpoint_number);

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to auto-update updated_at
DROP TRIGGER IF EXISTS update_checkpoint_blob_info_updated_at ON checkpoint_blob_info;
CREATE TRIGGER update_checkpoint_blob_info_updated_at
    BEFORE UPDATE ON checkpoint_blob_info
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
