-- Simplify checkpoint_index_entry to use checkpoint_number as primary key
-- since each checkpoint number is globally unique.

-- Drop the old index (no longer needed as checkpoint_number becomes primary key)
DROP INDEX IF EXISTS idx_checkpoint_index_entry_checkpoint_number;

-- Create new table with simplified schema
CREATE TABLE checkpoint_index_entry_new (
    checkpoint_number BIGINT PRIMARY KEY,
    start_checkpoint BIGINT NOT NULL,
    offset_bytes BIGINT NOT NULL,
    length_bytes BIGINT NOT NULL,
    FOREIGN KEY (start_checkpoint) REFERENCES checkpoint_blob_info(start_checkpoint) ON DELETE CASCADE
);

-- Copy data from old table
INSERT INTO checkpoint_index_entry_new (checkpoint_number, start_checkpoint, offset_bytes, length_bytes)
SELECT checkpoint_number, start_checkpoint, offset_bytes, length_bytes
FROM checkpoint_index_entry;

-- Drop old table
DROP TABLE checkpoint_index_entry;

-- Rename new table
ALTER TABLE checkpoint_index_entry_new RENAME TO checkpoint_index_entry;

-- Create index on start_checkpoint for foreign key lookups
CREATE INDEX idx_checkpoint_index_entry_start_checkpoint ON checkpoint_index_entry(start_checkpoint);
