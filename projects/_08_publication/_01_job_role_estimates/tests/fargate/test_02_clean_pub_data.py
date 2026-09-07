from unittest.mock import Mock, patch

import projects._08_publication._01_job_role_estimates.fargate._02_clean_pub_data as job

PATCH_PATH = (
    "projects._08_publication._01_job_role_estimates.fargate._02_clean_pub_data"
)

TEST_SOURCE = "some/directory"
TEST_DESTINATION = "some/other/directory"


class TestMain:
    @patch(f"{PATCH_PATH}.cUtils.published_data_filter_expr")
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
        published_data_filter_expr_mock: Mock,
    ):
        merged_lf = Mock(name="merged_lf")
        scan_parquet_mock.return_value = merged_lf

        job.main(TEST_SOURCE, TEST_DESTINATION)

        scan_parquet_mock.assert_called_once_with(TEST_SOURCE)
        published_data_filter_expr_mock.assert_called_once_with()
        merged_lf.filter.assert_called_once_with(
            published_data_filter_expr_mock.return_value
        )
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=merged_lf.filter.return_value,
            output_path=TEST_DESTINATION,
        )
