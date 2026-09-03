from unittest.mock import Mock, patch

import projects._08_publication._01_job_role_estimates.fargate._01_merge_pub_data as job

PATCH_PATH = (
    "projects._08_publication._01_job_role_estimates.fargate._01_merge_pub_data"
)

TEST_ESTIMATES_SOURCE = "some/directory"
TEST_METADATA_SOURCE = "some/metadata/directory"
TEST_GEOGRAPHY_SOURCE = "some/geography/directory"
TEST_DESTINATION = "some/other/directory"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    # @patch(f"{PATCH_PATH}.mUtils.join_geography")
    # @patch(f"{PATCH_PATH}.mUtils.join_estimates_and_metadata")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        # join_estimates_and_metadata_mock: Mock,
        # join_geography_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        archived_jr_estimate_lf = Mock(name="archived_jr_estimate_lf")
        archived_jr_metadata_lf = Mock(name="archived_jr_metadata_lf")
        archived_geography_lf = Mock(name="archived_geography_lf")
        scan_parquet_mock.side_effect = [
            archived_jr_estimate_lf,
            archived_jr_metadata_lf,
            archived_geography_lf,
        ]
        # estimates_and_metadata_lf = Mock(name="estimates_and_metadata_lf")
        # join_estimates_and_metadata_mock.return_value = estimates_and_metadata_lf
        # merged_lf = Mock(name="merged_lf")
        # join_geography_mock.return_value = merged_lf

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

        # join_estimates_and_metadata_mock.assert_called_once_with(
        #     archived_jr_estimate_lf, archived_jr_metadata_lf
        # )
        # join_geography_mock.assert_called_once_with(
        #     estimates_and_metadata_lf, archived_geography_lf
        # )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=archived_jr_estimate_lf,
            output_path=TEST_DESTINATION,
        )
