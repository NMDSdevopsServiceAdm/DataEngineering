from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._10_publication.fargate._02_clean as job

PATCH_PATH = "projects._03_independent_cqc._10_publication.fargate._02_clean"

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
        job.main(TEST_SOURCE, TEST_DESTINATION)

        scan_parquet_mock.assert_called_once_with(TEST_SOURCE)
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=TEST_DESTINATION,
        )
