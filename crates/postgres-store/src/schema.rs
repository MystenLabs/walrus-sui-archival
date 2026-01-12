// @generated automatically by Diesel CLI.

diesel::table! {
    checkpoint_blob_info (start_checkpoint) {
        start_checkpoint -> Int8,
        end_checkpoint -> Int8,
        #[max_length = 128]
        blob_id -> Varchar,
        #[max_length = 66]
        object_id -> Varchar,
        end_of_epoch -> Bool,
        blob_expiration_epoch -> Int4,
        is_shared_blob -> Bool,
        version -> Int4,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    checkpoint_index_entry (checkpoint_number) {
        checkpoint_number -> Int8,
        start_checkpoint -> Int8,
        offset_bytes -> Int8,
        length_bytes -> Int8,
    }
}

diesel::joinable!(checkpoint_index_entry -> checkpoint_blob_info (start_checkpoint));

diesel::allow_tables_to_appear_in_same_query!(checkpoint_blob_info, checkpoint_index_entry,);
