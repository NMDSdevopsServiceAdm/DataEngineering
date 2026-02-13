import polars as pl
import polars.testing as pl_testing
import unittest

import projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator.utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CalculateAscwdsFilledPostsUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CalculateAscwdsFilledPostsUtilsSchemas as Schemas,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestAscwdsFilledPostsCalculatorUtils(unittest.TestCase):
    def setUp(self):

        self.lf = pl.LazyFrame(
            data=Data.common_checks_rows, schema=Schemas.common_checks_schema
        )

    def assert_result(self, lf: pl.LazyFrame, expr: pl.Expr, expected_values):
        result_col = "result"

        result = (
            lf.with_columns(expr.alias(result_col)).sort(IndCQC.location_id).collect()
        )

        expected = result.drop(result_col).with_columns(
            pl.Series(name=result_col, values=expected_values)
        )

        pl_testing.assert_frame_equal(result, expected)

    def test_ascwds_filled_posts_is_null(self):
        self.assert_result(
            self.lf,
            job.ascwds_filled_posts_is_null(),
            [True, False],
        )

    def test_selected_column_is_not_null(self):
        self.assert_result(
            self.lf,
            job.selected_column_is_not_null(IndCQC.ascwds_filled_posts),
            [False, True],
        )

    def test_selected_column_is_at_least_the_min_permitted_value(self):
        self.assert_result(
            self.lf,
            job.selected_column_is_at_least_the_min_permitted_value(
                IndCQC.total_staff_bounded
            ),
            [True, False],
        )

    def test_absolute_difference_between_total_staff_and_worker_records_below_cut_off(
        self,
    ):
        self.assert_result(
            self.lf,
            job.absolute_difference_between_total_staff_and_worker_records_below_cut_off(),
            [False, True],
        )

    def test_percentage_difference_between_total_staff_and_worker_records_below_cut_off(
        self,
    ):
        self.assert_result(
            self.lf,
            job.percentage_difference_between_total_staff_and_worker_records_below_cut_off(),
            [False, True],
        )

    def test_two_cols_are_equal_and_at_least_minimum_permitted_value(self):

        lf = pl.LazyFrame(
            data=Data.test_two_cols_are_equal_rows, schema=Schemas.common_checks_schema
        )

        self.assert_result(
            lf,
            job.two_cols_are_equal_and_at_least_minimum_permitted_value(
                IndCQC.total_staff_bounded,
                IndCQC.worker_records_bounded,
            ),
            [False, False, True],
        )

    def test_absolute_difference_between_two_columns(self):
        self.assert_result(
            self.lf,
            job.absolute_difference_between_two_columns(
                IndCQC.total_staff_bounded,
                IndCQC.worker_records_bounded,
            ),
            [7, 0],
        )

    def test_average_of_two_columns(self):
        self.assert_result(
            self.lf,
            job.average_of_two_columns(
                IndCQC.total_staff_bounded,
                IndCQC.worker_records_bounded,
            ),
            [5.5, 2.0],
        )


class TestSourceDescriptionAdded(unittest.TestCase):
    def test_add_source_description_added_to_source_column_when_required(self):
        input_lf = pl.LazyFrame(
            Data.source_missing_rows,
            Schemas.estimated_source_description_schema,
            orient="row",
        )

        returned_lf = job.add_source_description_to_source_column(
            input_lf,
            IndCQC.estimate_filled_posts,
            IndCQC.estimate_filled_posts_source,
            "model_name",
        )
        expected_lf = pl.LazyFrame(
            Data.expected_source_added_rows,
            Schemas.estimated_source_description_schema,
            orient="row",
        )

        returned_data = returned_lf.sort(IndCQC.location_id).collect()
        expected_data = expected_lf.sort(IndCQC.location_id).collect()

        self.assertEqual(expected_data.height, expected_data.height)
        pl_testing.assert_frame_equal(expected_data, returned_data)
