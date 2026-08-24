from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._10_publication.fargate._01_merge as job

PATCH_PATH = "projects._03_independent_cqc._10_publication.fargate._01_merge"

TEST_ESTIMATES_SOURCE = "some/directory"
TEST_METADATA_SOURCE = "some/metadata/directory"
TEST_GEOGRAPHY_SOURCE = "some/geography/directory"
TEST_DESTINATION = "some/other/directory"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
            TEST_GEOGRAPHY_SOURCE,
            TEST_DESTINATION,
        )

        assert scan_parquet_mock.call_count == 3
        scan_parquet_mock.assert_any_call(TEST_ESTIMATES_SOURCE)
        scan_parquet_mock.assert_any_call(TEST_METADATA_SOURCE)
        scan_parquet_mock.assert_any_call(TEST_GEOGRAPHY_SOURCE)

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=TEST_DESTINATION,
        )
