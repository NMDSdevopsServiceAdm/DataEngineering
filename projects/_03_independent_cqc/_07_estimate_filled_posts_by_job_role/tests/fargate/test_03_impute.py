import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._03_impute as job

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._03_impute"


class MainTests(unittest.TestCase):
    CLEANED_DATA_SOURCE = "some/source"
    IMPUTED_DATA_DESTINATION = "some/destination"

    mock_estimated_job_role_posts_lf = pl.LazyFrame()

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.iUtils.create_ascwds_job_role_rolling_ratio")
    @patch(f"{PATCH_PATH}.iUtils.create_imputed_ascwds_job_role_counts")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        side_effect=[mock_estimated_job_role_posts_lf],
    )
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        create_imputed_ascwds_job_role_counts_mock: Mock,
        create_ascwds_job_role_rolling_ratio_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.CLEANED_DATA_SOURCE, self.IMPUTED_DATA_DESTINATION)

        self.assertEqual(scan_parquet_mock.call_count, 1)
        scan_parquet_mock.assert_has_calls(
            [
                call(self.CLEANED_DATA_SOURCE),
            ]
        )

        create_imputed_ascwds_job_role_counts_mock.assert_called_once()

        create_ascwds_job_role_rolling_ratio_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.IMPUTED_DATA_DESTINATION,
            append=False,
        )
