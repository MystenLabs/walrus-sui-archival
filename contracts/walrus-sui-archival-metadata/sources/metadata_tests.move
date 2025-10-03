#[test_only]
module walrus_sui_archival_metadata::metadata_tests {
    use std::option;
    use sui::test_scenario as ts;
    use walrus_sui_archival_metadata::metadata::{Self, AdminCap, MetadataBlobPointer};

    const ADMIN: address = @0xAD;
    const USER: address = @0x1;

    #[test]
    fun test_init() {
        let scenario_val = ts::begin(ADMIN);
        let scenario = &mut scenario_val;

        // initialize the module.
        metadata::init_for_testing(ts::ctx(scenario));

        // check that admin received the AdminCap.
        ts::next_tx(scenario, ADMIN);
        {
            let admin_cap = ts::take_from_sender<AdminCap>(scenario);
            ts::return_to_sender(scenario, admin_cap);
        };

        ts::end(scenario_val);
    }

    #[test]
    fun test_create_metadata_blob_pointer() {
        let scenario_val = ts::begin(ADMIN);
        let scenario = &mut scenario_val;

        // initialize module.
        metadata::init_for_testing(ts::ctx(scenario));

        // check that the pointer was created and shared with blob_id set to none.
        ts::next_tx(scenario, USER);
        {
            let pointer = ts::take_shared<MetadataBlobPointer>(scenario);
            let blob_id_opt = metadata::get_blob_id(&pointer);

            assert!(option::is_none(blob_id_opt), 0);

            ts::return_shared(pointer);
        };

        ts::end(scenario_val);
    }

    #[test]
    fun test_update_metadata_blob_pointer() {
        let scenario_val = ts::begin(ADMIN);
        let scenario = &mut scenario_val;

        // initialize module.
        metadata::init_for_testing(ts::ctx(scenario));

        // update pointer.
        ts::next_tx(scenario, ADMIN);
        {
            let admin_cap = ts::take_from_sender<AdminCap>(scenario);
            let pointer = ts::take_shared<MetadataBlobPointer>(scenario);

            let new_blob_id = vector[
                32u8, 31u8, 30u8, 29u8, 28u8, 27u8, 26u8, 25u8,
                24u8, 23u8, 22u8, 21u8, 20u8, 19u8, 18u8, 17u8,
                16u8, 15u8, 14u8, 13u8, 12u8, 11u8, 10u8, 9u8,
                8u8, 7u8, 6u8, 5u8, 4u8, 3u8, 2u8, 1u8
            ];

            metadata::update_metadata_blob_pointer(
                &admin_cap,
                &mut pointer,
                new_blob_id
            );

            let blob_id_opt = metadata::get_blob_id(&pointer);
            assert!(option::is_some(blob_id_opt), 0);

            let blob_id_ref = option::borrow(blob_id_opt);
            assert!(*blob_id_ref == new_blob_id, 1);

            ts::return_to_sender(scenario, admin_cap);
            ts::return_shared(pointer);
        };

        ts::end(scenario_val);
    }

    #[test]
    fun test_clear_metadata_blob_pointer() {
        let scenario_val = ts::begin(ADMIN);
        let scenario = &mut scenario_val;

        // initialize module.
        metadata::init_for_testing(ts::ctx(scenario));

        // first update the pointer with some blob id.
        ts::next_tx(scenario, ADMIN);
        {
            let admin_cap = ts::take_from_sender<AdminCap>(scenario);
            let pointer = ts::take_shared<MetadataBlobPointer>(scenario);

            let blob_id = vector[
                1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8,
                9u8, 10u8, 11u8, 12u8, 13u8, 14u8, 15u8, 16u8,
                17u8, 18u8, 19u8, 20u8, 21u8, 22u8, 23u8, 24u8,
                25u8, 26u8, 27u8, 28u8, 29u8, 30u8, 31u8, 32u8
            ];

            metadata::update_metadata_blob_pointer(
                &admin_cap,
                &mut pointer,
                blob_id
            );

            ts::return_to_sender(scenario, admin_cap);
            ts::return_shared(pointer);
        };

        // clear pointer.
        ts::next_tx(scenario, ADMIN);
        {
            let admin_cap = ts::take_from_sender<AdminCap>(scenario);
            let pointer = ts::take_shared<MetadataBlobPointer>(scenario);

            metadata::clear_metadata_blob_pointer(
                &admin_cap,
                &mut pointer
            );

            let blob_id_opt = metadata::get_blob_id(&pointer);
            assert!(option::is_none(blob_id_opt), 0);

            ts::return_to_sender(scenario, admin_cap);
            ts::return_shared(pointer);
        };

        ts::end(scenario_val);
    }

    #[test]
    fun test_delete_metadata_blob_pointer() {
        let scenario_val = ts::begin(ADMIN);
        let scenario = &mut scenario_val;

        // initialize module.
        metadata::init_for_testing(ts::ctx(scenario));

        // delete pointer.
        ts::next_tx(scenario, ADMIN);
        {
            let admin_cap = ts::take_from_sender<AdminCap>(scenario);
            let pointer = ts::take_shared<MetadataBlobPointer>(scenario);

            metadata::delete_metadata_blob_pointer(
                &admin_cap,
                pointer
            );

            ts::return_to_sender(scenario, admin_cap);
        };

        ts::end(scenario_val);
    }

    #[test]
    #[expected_failure(abort_code = metadata::EInvalidBlobIdLength)]
    fun test_update_with_invalid_blob_id_length() {
        let scenario_val = ts::begin(ADMIN);
        let scenario = &mut scenario_val;

        // initialize module.
        metadata::init_for_testing(ts::ctx(scenario));

        // update with invalid blob id.
        ts::next_tx(scenario, ADMIN);
        {
            let admin_cap = ts::take_from_sender<AdminCap>(scenario);
            let pointer = ts::take_shared<MetadataBlobPointer>(scenario);

            // invalid blob id (16 bytes instead of 32).
            let new_blob_id = vector[
                1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8,
                9u8, 10u8, 11u8, 12u8, 13u8, 14u8, 15u8, 16u8
            ];

            metadata::update_metadata_blob_pointer(
                &admin_cap,
                &mut pointer,
                new_blob_id
            );

            ts::return_to_sender(scenario, admin_cap);
            ts::return_shared(pointer);
        };

        ts::end(scenario_val);
    }
}
