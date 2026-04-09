import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._02_clean as job

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._02_clean"


class MainTests(unittest.TestCase):
    MERGED_DATA_SOURCE = "some/source"
    CLEANED_DATA_DESTINATION = "some/destination"

    mock_estimated_job_role_posts_lf = pl.LazyFrame()

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.nullify_job_role_count_when_source_not_ascwds")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        side_effect=[mock_estimated_job_role_posts_lf],
    )
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        nullify_job_role_count_when_source_not_ascwds_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.MERGED_DATA_SOURCE,
            self.CLEANED_DATA_DESTINATION,
        )

        self.assertEqual(scan_parquet_mock.call_count, 1)
        scan_parquet_mock.assert_has_calls(
            [
                call(
                    self.MERGED_DATA_SOURCE,
                ),
            ]
        )

        nullify_job_role_count_when_source_not_ascwds_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.CLEANED_DATA_DESTINATION,
            append=False,
        )
