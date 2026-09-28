from unittest.mock import Mock, call, patch

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._05_estimate_counts as job

PATCH_PATH = "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._05_estimate_counts"

SLV_ESTIMATE_SOURCE = "some/slv/source"
EMPLOYMENT_STATUS_ESTIMATE_SOURCE = "some/employment_status/source"
ESTIMATED_DATA_DESTINATION = "some/destination"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        slv_estimate_lf_mock = Mock(name="slv_estimate_lf")
        employment_status_estimate_lf_mock = Mock(name="employment_status_estimate_lf")
        scan_parquet_mock.side_effect = [
            slv_estimate_lf_mock,
            employment_status_estimate_lf_mock,
        ]

        job.main(
            SLV_ESTIMATE_SOURCE,
            EMPLOYMENT_STATUS_ESTIMATE_SOURCE,
            ESTIMATED_DATA_DESTINATION,
        )

        assert scan_parquet_mock.call_count == 2
        scan_parquet_mock.assert_has_calls(
            [
                call(SLV_ESTIMATE_SOURCE),
                call(EMPLOYMENT_STATUS_ESTIMATE_SOURCE),
            ]
        )
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=slv_estimate_lf_mock,
            output_path=ESTIMATED_DATA_DESTINATION,
        )
