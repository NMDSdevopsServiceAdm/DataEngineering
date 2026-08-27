from unittest.mock import Mock, patch

import projects._08_publication._01_job_role_estimates.fargate._02_clean_pub_data as job

PATCH_PATH = (
    "projects._08_publication._01_job_role_estimates.fargate._02_clean_pub_data"
)

TEST_SOURCE = "some/directory"
TEST_DESTINATION = "some/other/directory"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cUtils.add_ct_filter_dispersion_filter")
    @patch(f"{PATCH_PATH}.cUtils.add_ct_filter_consistent_service")
    @patch(f"{PATCH_PATH}.cUtils.add_ct_filter_has_ct_data")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs_all_steps_and_sinks_both_outputs(
        self,
        scan_parquet_mock: Mock,
        add_ct_filter_has_ct_data_mock: Mock,
        add_ct_filter_consistent_service_mock: Mock,
        add_ct_filter_dispersion_filter_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        merged_lf = Mock(name="merged_lf")
        scan_parquet_mock.return_value = merged_lf
        with_columns_lf = Mock(name="with_columns_lf")
        merged_lf.with_columns.return_value = with_columns_lf

        job.main(TEST_SOURCE, TEST_DESTINATION)

        scan_parquet_mock.assert_called_once_with(TEST_SOURCE)
        merged_lf.with_columns.assert_called_once_with(
            add_ct_filter_has_ct_data_mock.return_value,
            add_ct_filter_consistent_service_mock.return_value,
            add_ct_filter_dispersion_filter_mock.return_value,
        )
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=with_columns_lf,
            output_path=TEST_DESTINATION,
        )
