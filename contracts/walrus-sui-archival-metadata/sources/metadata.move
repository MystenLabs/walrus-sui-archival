module walrus_sui_archival_metadata::metadata {
    use std::vector;
    use std::option::{Self, Option};
    use sui::tx_context::{Self, TxContext};
    use sui::object::{Self, UID};
    use sui::transfer;

    // Error codes.
    const EInvalidBlobIdLength: u64 = 0;

    struct AdminCap has key, store {
        id: UID,
    }

    struct MetadataBlobPointer has key {
        id: UID,
        blob_id: Option<vector<u8>>,
    }

    fun init(ctx: &mut TxContext) {
        let admin_cap = AdminCap {
            id: object::new(ctx),
        };
        transfer::transfer(admin_cap, tx_context::sender(ctx));

        // create metadata blob pointer.
        let metadata_pointer = MetadataBlobPointer {
            id: object::new(ctx),
            blob_id: option::none(),
        };
        transfer::share_object(metadata_pointer);
    }

    #[test_only]
    public fun init_for_testing(ctx: &mut TxContext) {
        init(ctx);
    }

    public fun update_metadata_blob_pointer(
        _admin_cap: &AdminCap,
        metadata_pointer: &mut MetadataBlobPointer,
        new_blob_id: vector<u8>,
    ) {
        assert!(vector::length(&new_blob_id) == 32, EInvalidBlobIdLength);
        metadata_pointer.blob_id = option::some(new_blob_id);
    }

    public fun clear_metadata_blob_pointer(
        _admin_cap: &AdminCap,
        metadata_pointer: &mut MetadataBlobPointer,
    ) {
        metadata_pointer.blob_id = option::none();
    }

    public fun delete_metadata_blob_pointer(
        _admin_cap: &AdminCap,
        metadata_pointer: MetadataBlobPointer,
    ) {
        let MetadataBlobPointer { id, blob_id: _ } = metadata_pointer;
        object::delete(id);
    }

    public fun get_blob_id(metadata_pointer: &MetadataBlobPointer): &Option<vector<u8>> {
        &metadata_pointer.blob_id
    }
}