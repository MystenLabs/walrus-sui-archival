-- Add blob_size column to checkpoint_blob_info table
-- This stores the sum of length_bytes from all checkpoint_index_entry records
-- with the matching start_checkpoint
ALTER TABLE checkpoint_blob_info ADD COLUMN blob_size BIGINT;
