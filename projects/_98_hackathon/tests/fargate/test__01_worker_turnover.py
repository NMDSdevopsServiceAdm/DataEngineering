from unittest.mock import Mock, call, patch

import projects._98_hackathon.fargate._01_worker_turnover as job

PATCH_PATH = "projects._98_hackathon.fargate._01_worker_turnover"


class TestMain:
    CLEANED_WORKER_SOURCE = "some/worker/source"
    CLEANED_WORKPLACE_SOURCE = "some/workplace/source"
    FLAGGED_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.wUtils.add_establishment_active_flag")
    @patch(f"{PATCH_PATH}.wUtils.add_is_leaver_flag")
    @patch(f"{PATCH_PATH}.wUtils.add_main_service_type")
    @patch(f"{PATCH_PATH}.wUtils.filter_worker_turnover_data")
    @patch(f"{PATCH_PATH}.wUtils.get_latest_save_date_per_establishment")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_adds_establishment_active_after_leaver_flag(
        self,
        scan_parquet_mock: Mock,
        get_latest_save_date_per_establishment_mock: Mock,
        filter_worker_turnover_data_mock: Mock,
        add_main_service_type_mock: Mock,
        add_is_leaver_flag_mock: Mock,
        add_establishment_active_flag_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        worker_lf_mock, workplace_lf_mock = Mock(), Mock()
        scan_parquet_mock.side_effect = [worker_lf_mock, workplace_lf_mock]

        job.main(
            self.CLEANED_WORKER_SOURCE,
            self.CLEANED_WORKPLACE_SOURCE,
            self.FLAGGED_DESTINATION,
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
        ]
        filter_worker_turnover_data_mock.assert_called_once_with(worker_lf_mock)
        filtered_lf_mock = (
            filter_worker_turnover_data_mock.return_value.drop.return_value
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
        get_latest_save_date_per_establishment_mock.assert_called_once_with(
            scan_parquet_mock.return_value,
            filter_worker_turnover_data_mock.return_value,
        )
        add_main_service_type_mock.assert_called_once_with(
            filtered_lf_mock.join.return_value
        )
        filter_worker_turnover_data_mock.return_value.drop.assert_called_once_with(
            job.AWKClean.location_id
        )
        add_is_leaver_flag_mock.assert_called_once_with(
            add_main_service_type_mock.return_value
        )
        add_establishment_active_flag_mock.assert_called_once_with(
            add_is_leaver_flag_mock.return_value,
            get_latest_save_date_per_establishment_mock.return_value,
            job.DEFAULT_MAX_MONTHS_SINCE_SAVE,
        )
        sink_to_parquet_mock.assert_called_once_with(
            add_establishment_active_flag_mock.return_value,
            output_path=self.FLAGGED_DESTINATION,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
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
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.CLEANED_WORKER_SOURCE, self.FLAGGED_DESTINATION, n_months=3)

        add_establishment_active_flag_mock.assert_called_once_with(
            add_is_leaver_flag_mock.return_value,
            get_latest_save_date_per_establishment_mock.return_value,
            3,
        )
