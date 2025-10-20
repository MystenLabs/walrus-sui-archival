module walrus_sui_archival_metadata::admin {
    /// Admin capability shared across all modules.
    public struct AdminCap has key, store {
        id: UID,
    }

    /// Initialize the module, creating and transferring the admin cap.
    fun init(ctx: &mut TxContext) {
        let admin_cap = AdminCap {
            id: object::new(ctx),
        };
        transfer::transfer(admin_cap, tx_context::sender(ctx));
    }

    #[test_only]
    public fun init_for_testing(ctx: &mut TxContext) {
        init(ctx);
    }
}
