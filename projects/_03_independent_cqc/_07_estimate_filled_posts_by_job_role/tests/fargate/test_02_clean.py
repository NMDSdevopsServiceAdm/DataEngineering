import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl
import polars.testing as pltesting

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._02_clean as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateFilledPostsByJobRoleCleanData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateFilledPostsByJobRoleCleanSchemas as Schemas,
)

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._02_clean"


class MainTests(unittest.TestCase):
    MERGED_DATA_SOURCE = "some/source"
    CLEANED_DATA_DESTINATION = "some/destination"

    mock_estimated_job_role_posts_lf = pl.LazyFrame()

    test_estimated_job_role_posts_lf = pl.LazyFrame(Data.test_data, Schemas.test_schema)
    expected_estimated_job_role_posts_lf = pl.LazyFrame(
        Data.expected_data,
        Schemas.expected_schema,
    )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cUtils.filter_job_role_group_outliers")
    @patch(f"{PATCH_PATH}.cUtils.filter_job_role_group_equal_zero")
    @patch(f"{PATCH_PATH}.cUtils.nullify_job_role_count_when_source_not_ascwds")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        side_effect=[mock_estimated_job_role_posts_lf],
    )
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        nullify_job_role_count_when_source_not_ascwds_mock: Mock,
        filter_job_role_group_equal_zero_mock: Mock,
        filter_job_role_group_outliers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.MERGED_DATA_SOURCE, self.CLEANED_DATA_DESTINATION)

        self.assertEqual(scan_parquet_mock.call_count, 1)
        scan_parquet_mock.assert_has_calls(
            [
                call(self.MERGED_DATA_SOURCE),
            ]
        )

        nullify_job_role_count_when_source_not_ascwds_mock.assert_called_once()
        filter_job_role_group_equal_zero_mock.assert_called_once()
        filter_job_role_group_outliers_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.CLEANED_DATA_DESTINATION,
            append=False,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        side_effect=[test_estimated_job_role_posts_lf],
    )
    def test_main_runs_with_data(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.MERGED_DATA_SOURCE, self.CLEANED_DATA_DESTINATION)
        expected_col_order = self.expected_estimated_job_role_posts_lf.columns
        print(
            sink_to_parquet_mock.call_args.kwargs["lazy_df"]
            .select(expected_col_order)
            .dtypes
        )
        print(self.expected_estimated_job_role_posts_lf.dtypes)
        pltesting.assert_frame_equal(
            sink_to_parquet_mock.call_args.kwargs["lazy_df"],
            self.expected_estimated_job_role_posts_lf,
            check_column_order=False,
        )
