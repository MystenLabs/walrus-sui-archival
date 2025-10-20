module walrus_sui_archival_metadata::archival_metadata {
    use walrus_sui_archival_metadata::admin::AdminCap;

    // Error codes.
    const EInvalidBlobIdLength: u64 = 0;

    public struct MetadataBlobPointer has key {
        id: UID,
        blob_id: Option<vector<u8>>,
    }

    /// Initialize the module, creating and sharing the metadata blob pointer.
    fun init(ctx: &mut TxContext) {
        let metadata_pointer = MetadataBlobPointer {
            id: object::new(ctx),
            blob_id: option::none(),
        };
        transfer::share_object(metadata_pointer);
    }

    /// Create and share the metadata blob pointer.
    public fun create_metadata_pointer(_admin_cap: &AdminCap, ctx: &mut TxContext) {
        let metadata_pointer = MetadataBlobPointer {
            id: object::new(ctx),
            blob_id: option::none(),
        };
        transfer::share_object(metadata_pointer);
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

    #[test_only]
    public fun init_for_testing(ctx: &mut TxContext) {
        init(ctx);
    }
}