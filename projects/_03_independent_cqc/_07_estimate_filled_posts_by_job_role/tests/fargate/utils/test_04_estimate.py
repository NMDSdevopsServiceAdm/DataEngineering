import unittest

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

        print(returned_lf.collect().glimpse())

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
