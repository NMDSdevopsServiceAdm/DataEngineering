from unittest.mock import Mock, patch

import projects._03_independent_cqc._02_employment_status.fargate._02_clean as job

PATCH_PATH = "projects._03_independent_cqc._02_employment_status.fargate._02_clean"


class TestMain:
    MERGED_DATA_SOURCE = "some/source"
    CLEANED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cUtils.create_employment_status_percentage_columns")
    @patch(f"{PATCH_PATH}.cUtils.deduplicate_employment_status_counts")
    @patch(f"{PATCH_PATH}.cUtils.null_counts_for_low_location_ratio")
    @patch(f"{PATCH_PATH}.cUtils.null_counts_for_low_org_ratio")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        null_org_ratio_mock: Mock,
        null_location_ratio_mock: Mock,
        deduplicate_employment_status_counts_mock: Mock,
        create_employment_status_percentage_columns_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.MERGED_DATA_SOURCE,
            self.CLEANED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.MERGED_DATA_SOURCE)
        null_org_ratio_mock.assert_called_once_with(scan_parquet_mock.return_value)
        null_location_ratio_mock.assert_called_once_with(
            null_org_ratio_mock.return_value
        )
        deduplicate_employment_status_counts_mock.assert_called_once_with(
            null_location_ratio_mock.return_value
        )
        create_employment_status_percentage_columns_mock.assert_called_once_with(
            deduplicate_employment_status_counts_mock.return_value
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=create_employment_status_percentage_columns_mock.return_value,
            output_path=self.CLEANED_DATA_DESTINATION,
        )
