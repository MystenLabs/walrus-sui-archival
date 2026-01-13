-- Add index on blob_expiration_epoch for efficient expired blob queries
CREATE INDEX idx_checkpoint_blob_info_expiration_epoch ON checkpoint_blob_info (blob_expiration_epoch);
