import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._05_pivot as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateFilledPostsByJobRole04EstimatePivotData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateFilledPostsByJobRole04EstimatePivotSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._05_pivot"


class TestMain(unittest.TestCase):

    TEST_ESTIMATE_ROW_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/other/directory"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pivot_job_role_rows_to_columns")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_succeeds(
        self,
        scan_parquet_mock: Mock,
        pivot_job_role_rows_to_columns_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.TEST_ESTIMATE_ROW_SOURCE,
            self.TEST_DESTINATION,
        )

        scan_parquet_mock.assert_called_once()
        pivot_job_role_rows_to_columns_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.TEST_DESTINATION,
            partition_cols=[Keys.year],
            append=False,
        )


class TestPivotJobRoleRowsToColumns(unittest.TestCase):
    def test_function_returns_expected_values(self):
        test_lf = pl.LazyFrame(
            data=Data.input_data,
            schema=Schemas.input_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_data,
            schema=Schemas.expected_schema,
            orient="row",
        )
        returned_lf = job.pivot_job_role_rows_to_columns(
            test_lf, Data.test_job_roles_to_become_columns
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
