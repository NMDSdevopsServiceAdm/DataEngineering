from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._10_publication.fargate._02_clean_pub_data as job

PATCH_PATH = "projects._03_independent_cqc._10_publication.fargate._02_clean_pub_data"

TEST_SOURCE = "some/directory"
TEST_DESTINATION = "some/other/directory"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        merged_lf = Mock(name="merged_lf")
        scan_parquet_mock.return_value = merged_lf

        job.main(TEST_SOURCE, TEST_DESTINATION)

        scan_parquet_mock.assert_called_once_with(TEST_SOURCE)
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=merged_lf,
            output_path=TEST_DESTINATION,
        )
