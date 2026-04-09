import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._04_estimate as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateFilledPostsByJobRole04EstimateData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateFilledPostsByJobRole04EstimateSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._04_estimate"


class TestMain(unittest.TestCase):
    TEST_IMPUTED_SOURCE = "some/directory"
    TEST_METADATA_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    mock_data = Mock(name="data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.adjust_managerial_filled_posts")
    @patch(f"{PATCH_PATH}.count_cqc_rm")
    @patch(f"{PATCH_PATH}.calculate_estimated_filled_posts_by_job_role")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_succeeds(
        self,
        scan_parquet_mock: Mock,
        calculate_estimated_filled_posts_by_job_role_mock: Mock,
        count_cqc_rm_mock: Mock,
        adjust_managerial_filled_posts_Mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.TEST_IMPUTED_SOURCE,
            self.TEST_METADATA_SOURCE,
            self.TEST_DESTINATION,
        )

        scan_parquet_mock.assert_called_once()
        calculate_estimated_filled_posts_by_job_role_mock.assert_called_once()
        count_cqc_rm_mock.assert_called_once()
        adjust_managerial_filled_posts_Mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            lazy_lf=ANY,
            output_path=self.TEST_DESTINATION,
            append=False,
        )


class TestCalculateEstimatedFilledPostsByJobRole(unittest.TestCase):
    def test_function_returns_expected_values(self):
        expected_lf = pl.LazyFrame(
            data=Data.calculate_estimated_filled_posts_by_job_role_rows,
            schema=Schemas.calculate_estimated_filled_posts_by_job_role_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(
            [
                IndCQC.ascwds_job_role_ratios_merged,
                IndCQC.estimate_filled_posts_by_job_role,
            ]
        )
        returned_lf = job.calculate_estimated_filled_posts_by_job_role(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestCountCqcRm(unittest.TestCase):
    def test_function_returns_expected_values(self):
        expected_lf = pl.LazyFrame(
            data=Data.count_cqc_rm_rows,
            schema=Schemas.count_cqc_rm_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(IndCQC.registered_manager_count)
        returned_lf = test_lf.with_columns(
            job.count_cqc_rm().alias(IndCQC.registered_manager_count)
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestFilterRowsAndPivotIntoColumns(unittest.TestCase):
    def test_function_returns_expected_values(self):
        test_lf = pl.LazyFrame(
            data=Data.filter_rows_and_pivot_into_columns_rows,
            schema=Schemas.filter_rows_and_pivot_into_columns_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_filter_rows_and_pivot_into_columns_rows,
            schema=Schemas.expected_filter_rows_and_pivot_into_columns_schema,
            orient="row",
        )
        returned_lf = job.filter_rows_and_pivot_into_columns(
            test_lf,
            Data.test_list_of_job_roles,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestRecalculateManagerialFilledPosts(unittest.TestCase):
    def test_function_returns_expected_values(self):
        test_lf = pl.LazyFrame(
            data=Data.recalculate_managerial_filled_posts_rows,
            schema=Schemas.recalculate_managerial_filled_posts_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_recalculate_managerial_filled_posts_rows,
            schema=Schemas.expected_recalculate_managerial_filled_posts_schema,
            orient="row",
        )
        returned_lf = job.recalculate_managerial_filled_posts(
            test_lf,
            Data.test_list_of_job_roles,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestUnpivotJobRolesIntoRows(unittest.TestCase):
    def test_function_returns_expected_values(self):
        test_lf = pl.LazyFrame(
            data=Data.unpivot_job_roles_into_rows_data,
            schema=Schemas.unpivot_job_roles_into_rows_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_unpivot_job_roles_into_rows_data,
            schema=Schemas.expected_unpivot_job_roles_into_rows_schema,
            orient="row",
        )
        returned_lf = job.unpivot_job_roles_into_rows(
            test_lf,
            Data.test_list_of_job_roles,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
