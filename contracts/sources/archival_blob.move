module walrus_sui_archival_metadata::archival_blob {
    use sui::tx_context::TxContext;
    use sui::object::{Self, UID};
    use sui::transfer;
    use sui::balance::{Self, Balance};
    use sui::coin::{Self, Coin};
    use wal::wal::WAL;
    use walrus::system::System;
    use walrus::blob::Blob;
    use walrus_sui_archival_metadata::admin::AdminCap;

    // Error codes.
    const EInvalidAmount: u64 = 0;

    /// Global fund that holds WAL tokens for extending blobs.
    struct ArchivalBlobFund has key {
        id: UID,
        balance: Balance<WAL>,
    }

    /// A wrapper around Blob that can be funded from the ArchivalBlobFund and extended.
    struct SharedBlob has key {
        id: UID,
        blob: Blob,
    }

    /// Create a new ArchivalBlobFund and share it.
    public fun create(
        _admin_cap: &AdminCap,
        ctx: &mut TxContext,
    ) {
        let archival_blob_fund = ArchivalBlobFund {
            id: object::new(ctx),
            balance: balance::zero(),
        };
        transfer::share_object(archival_blob_fund);
    }

    /// Deposit WAL tokens into the fund.
    public fun deposit(
        archival_blob_fund: &mut ArchivalBlobFund,
        payment: Coin<WAL>,
    ) {
        let coin_balance = coin::into_balance(payment);
        balance::join(&mut archival_blob_fund.balance, coin_balance);
    }

    /// Create and share a SharedBlob from a Blob.
    public fun create_shared_blob(blob: Blob, ctx: &mut TxContext) {
        let shared_blob = SharedBlob {
            id: object::new(ctx),
            blob,
        };
        transfer::share_object(shared_blob);
    }

    /// Extend a SharedBlob's storage period using funds from the ArchivalBlobFund.
    /// Takes a SharedBlob object, extends it by the specified number of epochs using the Walrus system.
    public fun extend_shared_blob_using_shared_funds(
        archival_blob_fund: &mut ArchivalBlobFund,
        system: &mut System,
        shared_blob: &mut SharedBlob,
        extended_epochs: u32,
        ctx: &mut TxContext,
    ) {
        use walrus::system;

        assert!(extended_epochs > 0, EInvalidAmount);

        // Withdraw all funds and create a coin.
        let payment_coin = coin::from_balance(
            balance::withdraw_all(&mut archival_blob_fund.balance),
            ctx
        );

        // Call the Walrus system extend function on the wrapped blob.
        // This will deduct the required amount from payment_coin and extend the blob.
        system::extend_blob(system, &mut shared_blob.blob, extended_epochs, &mut payment_coin);

        // Put any remaining funds back into the ArchivalBlobFund.
        balance::join(&mut archival_blob_fund.balance, coin::into_balance(payment_coin));
    }

    /// Extend a SharedBlob's storage period using the caller's own token.
    /// Takes a token as input, extends the blob, and the remaining token stays with the caller.
    public fun extend_shared_blob_using_token(
        system: &mut System,
        shared_blob: &mut SharedBlob,
        extended_epochs: u32,
        payment: &mut Coin<WAL>,
    ) {
        use walrus::system;

        assert!(extended_epochs > 0, EInvalidAmount);

        // Call the Walrus system extend function on the wrapped blob.
        // This will deduct the required amount from payment and extend the blob.
        // Any remaining funds stay in the payment coin, which is returned to the caller.
        system::extend_blob(system, &mut shared_blob.blob, extended_epochs, payment);
    }

    /// Delete a SharedBlob, burning the wrapped Blob.
    /// This is an admin-only operation.
    public fun delete_shared_blob(
        _admin_cap: &AdminCap,
        shared_blob: SharedBlob,
    ) {
        use walrus::blob;

        let SharedBlob { id, blob } = shared_blob;
        object::delete(id);
        blob::burn(blob);
    }

    /// Get a reference to the wrapped Blob.
    public fun blob(shared_blob: &SharedBlob): &Blob {
        &shared_blob.blob
    }

    /// Get the current balance of the fund.
    public fun get_balance(archival_blob_fund: &ArchivalBlobFund): u64 {
        balance::value(&archival_blob_fund.balance)
    }

    #[test_only]
    public fun destroy_for_testing(archival_blob_fund: ArchivalBlobFund) {
        let ArchivalBlobFund { id, balance } = archival_blob_fund;
        balance::destroy_for_testing(balance);
        object::delete(id);
    }
}
