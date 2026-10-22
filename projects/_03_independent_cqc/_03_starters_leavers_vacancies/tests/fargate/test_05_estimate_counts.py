from unittest.mock import Mock, call, patch

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._05_estimate_counts as job

PATCH_PATH = "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._05_estimate_counts"

EMPLOYMENT_STATUS_ESTIMATE_SOURCE = "some/employment_status/source"
SLV_ESTIMATE_SOURCE = "some/slv/source"
ESTIMATED_DATA_DESTINATION = "some/destination"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.add_estimated_counts")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        add_estimated_counts_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        employment_status_lf_mock = Mock(name="employment_status_lf")
        slv_lf_mock = Mock(name="slv_lf")
        scan_parquet_mock.side_effect = [employment_status_lf_mock, slv_lf_mock]

        job.main(
            EMPLOYMENT_STATUS_ESTIMATE_SOURCE,
            SLV_ESTIMATE_SOURCE,
            ESTIMATED_DATA_DESTINATION,
        )

        assert scan_parquet_mock.call_args_list == [
            call(
                EMPLOYMENT_STATUS_ESTIMATE_SOURCE,
                selected_columns=job.employment_status_estimate_columns,
            ),
            call(SLV_ESTIMATE_SOURCE, selected_columns=job.slv_estimate_columns),
        ]
        employment_status_lf_mock.join.assert_called_once_with(
            slv_lf_mock, on=job.JOIN_KEYS, how="left"
        )
        add_estimated_counts_mock.assert_called_once_with(
            employment_status_lf_mock.join.return_value
        )
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=add_estimated_counts_mock.return_value,
            output_path=ESTIMATED_DATA_DESTINATION,
        )
