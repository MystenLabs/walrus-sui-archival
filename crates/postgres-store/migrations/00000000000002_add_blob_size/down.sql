-- Remove blob_size column from checkpoint_blob_info table
ALTER TABLE checkpoint_blob_info DROP COLUMN blob_size;
