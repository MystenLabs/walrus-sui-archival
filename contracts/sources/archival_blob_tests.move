#[test_only]
module walrus_sui_archival_metadata::archival_blob_tests {
    use sui::test_scenario as ts;
    use sui::coin;
    use wal::wal::WAL;
    use walrus_sui_archival_metadata::archival_blob::{Self, ArchivalBlobFund};

    const ADMIN: address = @0xAD;
    const USER: address = @0x123;

    #[test]
    fun test_init() {
        let mut scenario_val = ts::begin(ADMIN);
        let scenario = &mut scenario_val;

        // initialize archival blob module.
        archival_blob::init_for_testing(ts::ctx(scenario));

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
        let mut scenario_val = ts::begin(ADMIN);
        let scenario = &mut scenario_val;

        // initialize archival blob module.
        archival_blob::init_for_testing(ts::ctx(scenario));

        // user deposits into the archival blob fund.
        ts::next_tx(scenario, USER);
        {
            let mut archival_blob_fund = ts::take_shared<ArchivalBlobFund>(scenario);
            let payment = coin::mint_for_testing<WAL>(1000, ts::ctx(scenario));
            archival_blob::deposit(&mut archival_blob_fund, payment);
            assert!(archival_blob::get_balance(&archival_blob_fund) == 1000, 0);
            ts::return_shared(archival_blob_fund);
        };

        ts::end(scenario_val);
    }

    // Note: Testing extend_shared_blob and delete_shared_blob requires a Walrus System object and SharedArchivalBlob object,
    // which cannot be easily mocked in unit tests. Integration tests would be needed
    // to fully test these functionalities.
}
