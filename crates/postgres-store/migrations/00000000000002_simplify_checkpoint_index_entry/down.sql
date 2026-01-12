-- Revert to composite primary key (start_checkpoint, checkpoint_number)

-- Drop the index
DROP INDEX IF EXISTS idx_checkpoint_index_entry_start_checkpoint;

-- Create old table structure
CREATE TABLE checkpoint_index_entry_old (
    start_checkpoint BIGINT NOT NULL,
    checkpoint_number BIGINT NOT NULL,
    offset_bytes BIGINT NOT NULL,
    length_bytes BIGINT NOT NULL,
    PRIMARY KEY (start_checkpoint, checkpoint_number),
    FOREIGN KEY (start_checkpoint) REFERENCES checkpoint_blob_info(start_checkpoint) ON DELETE CASCADE
);

-- Copy data
INSERT INTO checkpoint_index_entry_old (start_checkpoint, checkpoint_number, offset_bytes, length_bytes)
SELECT start_checkpoint, checkpoint_number, offset_bytes, length_bytes
FROM checkpoint_index_entry;

-- Drop new table
DROP TABLE checkpoint_index_entry;

-- Rename old table back
ALTER TABLE checkpoint_index_entry_old RENAME TO checkpoint_index_entry;

-- Recreate original index
CREATE INDEX idx_checkpoint_index_entry_checkpoint_number ON checkpoint_index_entry(checkpoint_number);
