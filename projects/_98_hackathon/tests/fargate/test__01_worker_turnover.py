from unittest.mock import Mock, call, patch

import polars as pl

import projects._98_hackathon.fargate._01_worker_turnover as job

PATCH_PATH = "projects._98_hackathon.fargate._01_worker_turnover"


class TestMain:
    CLEANED_WORKER_SOURCE = "some/worker/source"
    CLEANED_WORKPLACE_SOURCE = "some/workplace/source"
    RAW_WORKER_SOURCE = "some/raw/worker/source"
    TRAIN_DESTINATION = "some/train"
    HOLDOUT_DESTINATION = "some/holdout"
    TEST_DESTINATION = "some/test"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pl.collect_all")
    @patch(f"{PATCH_PATH}.wUtils.add_split_column")
    @patch(f"{PATCH_PATH}.wUtils.impute_worker_columns")
    @patch(f"{PATCH_PATH}.wUtils.add_country_code_group")
    @patch(f"{PATCH_PATH}.wUtils.add_worker_labels")
    @patch(f"{PATCH_PATH}.wUtils.add_nlw_and_pay_difference")
    @patch(f"{PATCH_PATH}.wUtils.combine_pay_and_hours")
    @patch(f"{PATCH_PATH}.wUtils.null_not_known_codes")
    @patch(f"{PATCH_PATH}.wUtils.add_valid_for_analysis_flag")
    @patch(f"{PATCH_PATH}.wUtils.add_headcount_reconciliation_gap")
    @patch(f"{PATCH_PATH}.wUtils.add_raw_worker_columns")
    @patch(f"{PATCH_PATH}.wUtils.add_turnover_rate")
    @patch(f"{PATCH_PATH}.wUtils.add_establishment_active_flag")
    @patch(f"{PATCH_PATH}.wUtils.add_is_leaver_flag")
    @patch(f"{PATCH_PATH}.wUtils.add_main_service_type")
    @patch(f"{PATCH_PATH}.wUtils.filter_worker_turnover_data")
    @patch(f"{PATCH_PATH}.wUtils.get_latest_save_date_per_establishment")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        get_latest_save_date_per_establishment_mock: Mock,
        filter_worker_turnover_data_mock: Mock,
        add_main_service_type_mock: Mock,
        add_is_leaver_flag_mock: Mock,
        add_establishment_active_flag_mock: Mock,
        add_turnover_rate_mock: Mock,
        add_raw_worker_columns_mock: Mock,
        add_headcount_reconciliation_gap_mock: Mock,
        add_valid_for_analysis_flag_mock: Mock,
        null_not_known_codes_mock: Mock,
        combine_pay_and_hours_mock: Mock,
        add_nlw_and_pay_difference_mock: Mock,
        add_worker_labels_mock: Mock,
        add_country_code_group_mock: Mock,
        impute_worker_columns_mock: Mock,
        add_split_column_mock: Mock,
        collect_all_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        worker_lf_mock, workplace_lf_mock, raw_worker_lf_mock = Mock(), Mock(), Mock()
        scan_parquet_mock.side_effect = [
            worker_lf_mock,
            workplace_lf_mock,
            raw_worker_lf_mock,
        ]
        splits = ["train", "holdout", "test"]
        destinations = [
            self.TRAIN_DESTINATION,
            self.HOLDOUT_DESTINATION,
            self.TEST_DESTINATION,
        ]
        # One lazy frame per split, so each split's destination is checked.
        split_lf_mocks = [Mock() for _ in splits]
        add_split_column_mock.return_value.filter.side_effect = split_lf_mocks
        sink_mocks = [Mock() for _ in splits]
        sink_to_parquet_mock.side_effect = sink_mocks

        job.main(
            self.CLEANED_WORKER_SOURCE,
            self.CLEANED_WORKPLACE_SOURCE,
            self.RAW_WORKER_SOURCE,
            self.TRAIN_DESTINATION,
            self.HOLDOUT_DESTINATION,
            self.TEST_DESTINATION,
        )

        assert scan_parquet_mock.call_args_list == [
            call(self.CLEANED_WORKER_SOURCE),
            call(
                self.CLEANED_WORKPLACE_SOURCE,
                selected_columns=[
                    job.AWPClean.establishment_id,
                    job.AWPClean.ascwds_workplace_import_date,
                    job.AWPClean.main_service_id_clean,
                ],
            ),
            call(self.RAW_WORKER_SOURCE, schema=job.wUtils.RAW_WORKER_SCHEMA),
        ]
        filter_worker_turnover_data_mock.assert_called_once_with(worker_lf_mock)
        filtered_lf_mock = (
            filter_worker_turnover_data_mock.return_value.drop.return_value
        )
        filter_worker_turnover_data_mock.return_value.drop.assert_called_once_with(
            job.AWKClean.location_id,
            job.AWKClean.main_job_role_id,
            job.AWKClean.main_job_role_clean,
            job.AWKClean.employment_status,
            job.AWKClean.employment_status_clean,
        )
        filtered_lf_mock.join.assert_called_once_with(
            workplace_lf_mock,
            left_on=[
                job.AWKClean.establishment_id,
                job.AWKClean.ascwds_worker_import_date,
            ],
            right_on=[
                job.AWPClean.establishment_id,
                job.AWPClean.ascwds_workplace_import_date,
            ],
            how="left",
        )
        add_main_service_type_mock.assert_called_once_with(
            filtered_lf_mock.join.return_value
        )
        add_main_service_type_mock.return_value.drop.assert_called_once_with(
            job.AWPClean.main_service_id_clean
        )
        main_service_dropped_lf = (
            add_main_service_type_mock.return_value.drop.return_value
        )
        main_service_dropped_lf.collect.assert_called_once_with()
        collected_lf = main_service_dropped_lf.collect.return_value.lazy.return_value
        get_latest_save_date_per_establishment_mock.assert_called_once_with(
            worker_lf_mock, collected_lf
        )
        add_is_leaver_flag_mock.assert_called_once_with(collected_lf)
        add_establishment_active_flag_mock.assert_called_once_with(
            add_is_leaver_flag_mock.return_value,
            get_latest_save_date_per_establishment_mock.return_value,
            job.wUtils.DEFAULT_MAX_MONTHS_SINCE_SAVE,
        )
        add_turnover_rate_mock.assert_called_once_with(
            add_establishment_active_flag_mock.return_value
        )
        add_raw_worker_columns_mock.assert_called_once_with(
            add_turnover_rate_mock.return_value, raw_worker_lf_mock
        )
        add_headcount_reconciliation_gap_mock.assert_called_once_with(
            add_raw_worker_columns_mock.return_value,
            job.wUtils.DEFAULT_MAX_HEADCOUNT_GAP,
        )
        add_valid_for_analysis_flag_mock.assert_called_once_with(
            add_headcount_reconciliation_gap_mock.return_value
        )
        add_valid_for_analysis_flag_mock.return_value.drop.assert_called_once_with(
            job.HackCols.establishment_in_both_snapshots,
            job.HackCols.months_since_save_start,
            job.HackCols.months_since_save_end,
            job.HackCols.establishment_active,
            job.HackCols.headcount_reconciliation_gap,
            job.HackCols.headcount_gap_within_tolerance,
            job.HackCols.plausible_turnover,
        )
        null_not_known_codes_mock.assert_called_once_with(
            add_valid_for_analysis_flag_mock.return_value.drop.return_value
        )
        null_not_known_codes_mock.return_value.with_columns.assert_called_once()
        combine_pay_and_hours_mock.assert_called_once_with(
            null_not_known_codes_mock.return_value.with_columns.return_value
        )
        add_worker_labels_mock.assert_called_once_with(
            combine_pay_and_hours_mock.return_value
        )
        assert add_country_code_group_mock.call_args_list == [
            call(add_worker_labels_mock.return_value, job.AWKClean.nationality),
            call(
                add_country_code_group_mock.return_value,
                job.AWKClean.country_of_birth,
            ),
        ]
        add_country_code_group_mock.return_value.drop.assert_called_once_with(
            job.wUtils.WORKER_CATEGORICAL_COLUMNS
        )
        impute_worker_columns_mock.assert_called_once_with(
            add_country_code_group_mock.return_value.drop.return_value,
            job.wUtils.IMPUTE_COLUMNS,
        )
        add_nlw_and_pay_difference_mock.assert_called_once_with(
            impute_worker_columns_mock.return_value
        )
        add_split_column_mock.assert_called_once_with(
            add_nlw_and_pay_difference_mock.return_value
        )
        filter_mock = add_split_column_mock.return_value.filter
        assert filter_mock.call_count == len(splits)
        for filter_call, split in zip(filter_mock.call_args_list, splits):
            assert filter_call.args[0].meta.eq(pl.col(job.HackCols.split) == split)
        assert sink_to_parquet_mock.call_args_list == [
            call(split_lf_mock, output_path=destination, lazy=True)
            for split_lf_mock, destination in zip(split_lf_mocks, destinations)
        ]
        collect_all_mock.assert_called_once_with(sink_mocks, engine="streaming")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pl.collect_all")
    @patch(f"{PATCH_PATH}.wUtils.add_country_code_group")
    @patch(f"{PATCH_PATH}.wUtils.add_worker_labels")
    @patch(f"{PATCH_PATH}.wUtils.add_nlw_and_pay_difference")
    @patch(f"{PATCH_PATH}.wUtils.combine_pay_and_hours")
    @patch(f"{PATCH_PATH}.wUtils.null_not_known_codes")
    @patch(f"{PATCH_PATH}.wUtils.add_valid_for_analysis_flag")
    @patch(f"{PATCH_PATH}.wUtils.add_headcount_reconciliation_gap")
    @patch(f"{PATCH_PATH}.wUtils.add_raw_worker_columns")
    @patch(f"{PATCH_PATH}.wUtils.add_turnover_rate")
    @patch(f"{PATCH_PATH}.wUtils.add_establishment_active_flag")
    @patch(f"{PATCH_PATH}.wUtils.add_is_leaver_flag")
    @patch(f"{PATCH_PATH}.wUtils.add_main_service_type")
    @patch(f"{PATCH_PATH}.wUtils.filter_worker_turnover_data")
    @patch(f"{PATCH_PATH}.wUtils.get_latest_save_date_per_establishment")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_passes_n_months_to_establishment_active_flag(
        self,
        scan_parquet_mock: Mock,
        get_latest_save_date_per_establishment_mock: Mock,
        filter_worker_turnover_data_mock: Mock,
        add_main_service_type_mock: Mock,
        add_is_leaver_flag_mock: Mock,
        add_establishment_active_flag_mock: Mock,
        add_turnover_rate_mock: Mock,
        add_raw_worker_columns_mock: Mock,
        add_headcount_reconciliation_gap_mock: Mock,
        add_valid_for_analysis_flag_mock: Mock,
        null_not_known_codes_mock: Mock,
        combine_pay_and_hours_mock: Mock,
        add_nlw_and_pay_difference_mock: Mock,
        add_worker_labels_mock: Mock,
        add_country_code_group_mock: Mock,
        collect_all_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_WORKER_SOURCE,
            self.CLEANED_WORKPLACE_SOURCE,
            self.RAW_WORKER_SOURCE,
            self.TRAIN_DESTINATION,
            self.HOLDOUT_DESTINATION,
            self.TEST_DESTINATION,
            n_months=3,
        )

        add_establishment_active_flag_mock.assert_called_once_with(
            add_is_leaver_flag_mock.return_value,
            get_latest_save_date_per_establishment_mock.return_value,
            3,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pl.collect_all")
    @patch(f"{PATCH_PATH}.wUtils.add_country_code_group")
    @patch(f"{PATCH_PATH}.wUtils.add_worker_labels")
    @patch(f"{PATCH_PATH}.wUtils.combine_pay_and_hours")
    @patch(f"{PATCH_PATH}.wUtils.add_valid_for_analysis_flag")
    @patch(f"{PATCH_PATH}.wUtils.add_headcount_reconciliation_gap")
    @patch(f"{PATCH_PATH}.wUtils.add_raw_worker_columns")
    @patch(f"{PATCH_PATH}.wUtils.add_turnover_rate")
    @patch(f"{PATCH_PATH}.wUtils.add_establishment_active_flag")
    @patch(f"{PATCH_PATH}.wUtils.add_is_leaver_flag")
    @patch(f"{PATCH_PATH}.wUtils.add_main_service_type")
    @patch(f"{PATCH_PATH}.wUtils.filter_worker_turnover_data")
    @patch(f"{PATCH_PATH}.wUtils.get_latest_save_date_per_establishment")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_passes_max_gap_to_headcount_reconciliation_gap(
        self,
        scan_parquet_mock: Mock,
        get_latest_save_date_per_establishment_mock: Mock,
        filter_worker_turnover_data_mock: Mock,
        add_main_service_type_mock: Mock,
        add_is_leaver_flag_mock: Mock,
        add_establishment_active_flag_mock: Mock,
        add_turnover_rate_mock: Mock,
        add_raw_worker_columns_mock: Mock,
        add_headcount_reconciliation_gap_mock: Mock,
        add_valid_for_analysis_flag_mock: Mock,
        combine_pay_and_hours_mock: Mock,
        add_worker_labels_mock: Mock,
        add_country_code_group_mock: Mock,
        collect_all_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_WORKER_SOURCE,
            self.CLEANED_WORKPLACE_SOURCE,
            self.RAW_WORKER_SOURCE,
            self.TRAIN_DESTINATION,
            self.HOLDOUT_DESTINATION,
            self.TEST_DESTINATION,
            max_gap=5,
        )

        add_headcount_reconciliation_gap_mock.assert_called_once_with(
            add_raw_worker_columns_mock.return_value, 5
        )
