-- Drop tables in reverse order due to foreign key constraints
DROP TABLE IF EXISTS checkpoint_index_entry;
DROP TABLE IF EXISTS checkpoint_blob_info;

-- Drop the trigger function
DROP FUNCTION IF EXISTS update_updated_at_column();
