#[test_only]
module walrus_sui_archival_metadata::archival_blob_tests {
    use sui::test_scenario as ts;
    use sui::coin;
    use wal::wal::WAL;
    use walrus_sui_archival_metadata::admin::{Self, AdminCap};
    use walrus_sui_archival_metadata::archival_blob::{Self, ArchivalBlobFund};

    const ADMIN: address = @0xAD;
    const USER: address = @0x123;

    #[test]
    fun test_create() {
        let scenario_val = ts::begin(ADMIN);
        let scenario = &mut scenario_val;

        // initialize admin module.
        admin::init_for_testing(ts::ctx(scenario));

        // admin creates the archival blob fund.
        ts::next_tx(scenario, ADMIN);
        {
            let admin_cap = ts::take_from_sender<AdminCap>(scenario);
            archival_blob::create(&admin_cap, ts::ctx(scenario));
            ts::return_to_sender(scenario, admin_cap);
        };

        // verify archival blob fund exists.
        ts::next_tx(scenario, USER);
        {
            let archival_blob_fund = ts::take_shared<ArchivalBlobFund>(scenario);
            assert!(archival_blob::get_balance(&archival_blob_fund) == 0, 0);
            ts::return_shared(archival_blob_fund);
        };

        ts::end(scenario_val);
    }

    #[test]
    fun test_deposit() {
        let scenario_val = ts::begin(ADMIN);
        let scenario = &mut scenario_val;

        // initialize admin module.
        admin::init_for_testing(ts::ctx(scenario));

        // admin creates the archival blob fund.
        ts::next_tx(scenario, ADMIN);
        {
            let admin_cap = ts::take_from_sender<AdminCap>(scenario);
            archival_blob::create(&admin_cap, ts::ctx(scenario));
            ts::return_to_sender(scenario, admin_cap);
        };

        // user deposits into the archival blob fund.
        ts::next_tx(scenario, USER);
        {
            let archival_blob_fund = ts::take_shared<ArchivalBlobFund>(scenario);
            let payment = coin::mint_for_testing<WAL>(1000, ts::ctx(scenario));
            archival_blob::deposit(&mut archival_blob_fund, payment);
            assert!(archival_blob::get_balance(&archival_blob_fund) == 1000, 0);
            ts::return_shared(archival_blob_fund);
        };

        ts::end(scenario_val);
    }

    // Note: Testing extend_shared_blob requires a Walrus System object and SharedBlob object,
    // which cannot be easily mocked in unit tests. Integration tests would be needed
    // to fully test the extend_shared_blob functionality.
}
