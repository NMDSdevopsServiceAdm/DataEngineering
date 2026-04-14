import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl
import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._01_merge as job

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._01_merge"


class MainTests(unittest.TestCase):
    ESTIMATE_SOURCE = "some/source"
    ASCWDS_JOB_ROLE_COUNT_SOURCE = "some/other/source"
    MERGED_DATA_DESTINATION = "some/destination"
    METADATA_DESTINATION = "some/other/destination"

    mock_estimate_lf = pl.LazyFrame(schema=job.transformation_columns)
    mock_prepared_job_role_counts_lf = pl.LazyFrame(schema=job.ascwds_columns_to_import)

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.join_estimates_to_ascwds")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        side_effect=[mock_estimate_lf, mock_prepared_job_role_counts_lf],
    )
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        join_estimates_to_ascwds_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.ESTIMATE_SOURCE,
            self.ASCWDS_JOB_ROLE_COUNT_SOURCE,
            self.MERGED_DATA_DESTINATION,
            self.METADATA_DESTINATION,
        )

        self.assertEqual(scan_parquet_mock.call_count, 2)
        scan_parquet_mock.assert_has_calls(
            [
                call(self.ESTIMATE_SOURCE),
                call(self.ASCWDS_JOB_ROLE_COUNT_SOURCE),
            ]
        )

        join_estimates_to_ascwds_mock.assert_called_once()

        self.assertEqual(sink_to_parquet_mock.call_count, 2)

        sink_to_parquet_mock.assert_has_calls(
            [
                call(
                    lazy_df=ANY,
                    output_path=self.MERGED_DATA_DESTINATION,
                    append=False,
                ),
                call(
                    lazy_df=ANY,
                    output_path=self.METADATA_DESTINATION,
                    append=False,
                ),
            ]
        )
