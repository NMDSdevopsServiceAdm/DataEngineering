from unittest.mock import ANY, Mock, call, patch

import projects._02_sfc_internal._01_cqc_ratings.fargate.flatten_cqc_ratings as job

PATCH_PATH = "projects._02_sfc_internal._01_cqc_ratings.fargate.flatten_cqc_ratings"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.ratings_utils.create_benchmark_ratings_dataset")
    @patch(f"{PATCH_PATH}.ratings_utils.join_establishment_ids")
    @patch(f"{PATCH_PATH}.ratings_utils.add_good_and_outstanding_flag_column")
    @patch(f"{PATCH_PATH}.ratings_utils.select_ratings_for_benchmarks")
    @patch(f"{PATCH_PATH}.ratings_utils.add_location_id_hash")
    @patch(f"{PATCH_PATH}.ratings_utils.create_standard_ratings_dataset")
    @patch(f"{PATCH_PATH}.ratings_utils.add_numerical_ratings")
    @patch(f"{PATCH_PATH}.ratings_utils.add_latest_rating_flag_column")
    @patch(f"{PATCH_PATH}.ratings_utils.add_rating_sequence_column")
    @patch(f"{PATCH_PATH}.ratings_utils.remove_blank_and_duplicate_rows")
    @patch(f"{PATCH_PATH}.ratings_utils.recode_unknown_codes_to_null")
    @patch(f"{PATCH_PATH}.ratings_utils.merge_cqc_ratings")
    @patch(f"{PATCH_PATH}.pl.concat")
    @patch(
        f"{PATCH_PATH}.ratings_utils.raise_error_when_assessment_df_contains_overall_data"
    )
    @patch(f"{PATCH_PATH}.ratings_utils.prepare_assessment_ratings")
    @patch(f"{PATCH_PATH}.ratings_utils.prepare_historic_ratings")
    @patch(f"{PATCH_PATH}.ratings_utils.prepare_current_ratings")
    @patch(f"{PATCH_PATH}.ratings_utils.filter_to_first_import_of_most_recent_month")
    @patch(f"{PATCH_PATH}.ratings_utils.keep_latest_per_key")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_calls_every_step_and_writes_both_outputs(
        self,
        scan_parquet_mock: Mock,
        keep_latest_per_key_mock: Mock,
        filter_to_first_import_of_most_recent_month_mock: Mock,
        prepare_current_ratings_mock: Mock,
        prepare_historic_ratings_mock: Mock,
        prepare_assessment_ratings_mock: Mock,
        raise_error_when_assessment_df_contains_overall_data_mock: Mock,
        pl_concat_mock: Mock,
        merge_cqc_ratings_mock: Mock,
        recode_unknown_codes_to_null_mock: Mock,
        remove_blank_and_duplicate_rows_mock: Mock,
        add_rating_sequence_column_mock: Mock,
        add_latest_rating_flag_column_mock: Mock,
        add_numerical_ratings_mock: Mock,
        create_standard_ratings_dataset_mock: Mock,
        add_location_id_hash_mock: Mock,
        select_ratings_for_benchmarks_mock: Mock,
        add_good_and_outstanding_flag_column_mock: Mock,
        join_establishment_ids_mock: Mock,
        create_benchmark_ratings_dataset_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        scan_parquet_mock.side_effect = [Mock(), Mock(), Mock()]

        job.main(
            "cqc_full_snapshot_source",
            "cqc_locations_api_delta_source",
            "ascwds_workplace_source",
            "cqc_ratings_destination",
            "benchmark_ratings_destination",
        )

        assert scan_parquet_mock.call_count == 3
        keep_latest_per_key_mock.assert_called_once()
        filter_to_first_import_of_most_recent_month_mock.assert_called_once()
        prepare_current_ratings_mock.assert_called_once()
        prepare_historic_ratings_mock.assert_called_once()
        prepare_assessment_ratings_mock.assert_called_once()
        raise_error_when_assessment_df_contains_overall_data_mock.assert_called_once()
        pl_concat_mock.assert_called_once()
        merge_cqc_ratings_mock.assert_called_once()
        recode_unknown_codes_to_null_mock.assert_called_once()
        remove_blank_and_duplicate_rows_mock.assert_called_once()
        assert add_rating_sequence_column_mock.call_count == 2
        add_latest_rating_flag_column_mock.assert_called_once()
        add_numerical_ratings_mock.assert_called_once()
        create_standard_ratings_dataset_mock.assert_called_once()
        add_location_id_hash_mock.assert_called_once()
        select_ratings_for_benchmarks_mock.assert_called_once()
        add_good_and_outstanding_flag_column_mock.assert_called_once()
        join_establishment_ids_mock.assert_called_once()
        create_benchmark_ratings_dataset_mock.assert_called_once()
        assert sink_to_parquet_mock.call_count == 2

        expected_sink_calls = [
            call(ANY, "cqc_ratings_destination"),
            call(ANY, "benchmark_ratings_destination"),
        ]
        sink_to_parquet_mock.assert_has_calls(expected_sink_calls, any_order=True)
