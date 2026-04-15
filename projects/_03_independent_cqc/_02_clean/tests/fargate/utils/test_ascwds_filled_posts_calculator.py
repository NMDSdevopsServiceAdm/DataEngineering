import unittest
from dataclasses import asdict

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CalculateAscwdsFilledPostsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CalculateAscwdsFilledPostsSchemas as Schemas,
)


class TestCalculationConstants(unittest.TestCase):
    def test_calculation_constants(self):
        expected_values = {
            "MAX_ABSOLUTE_DIFFERENCE": 5,
            "MAX_PERCENTAGE_DIFFERENCE": 0.1,
            "MIN_PERMITTED_POSTS": 3,
        }

        actual_values = asdict(job.CalculationConstants())

        expected_df = pl.DataFrame([expected_values])
        actual_df = pl.DataFrame([actual_values])

        pl_testing.assert_frame_equal(actual_df, expected_df)


class TestAscwdsFilledPostsCalculator(unittest.TestCase):
    def test_returns_expected_lazyframe(self):
        input_lf = pl.LazyFrame(
            Data.calculate_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
            orient="row",
        )

        returned_lf = job.calculate_ascwds_filled_posts(input_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestPopulateFromExactStaffMatch(unittest.TestCase):
    def test_returns_expected_lazyframe(self):
        input_lf = pl.LazyFrame(
            Data.calculate_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
            orient="row",
        )
        returned_lf = job.populate_from_exact_staff_match(input_lf)
        expected_lf = pl.LazyFrame(
            Data.expected_totalstaff_equal_wkrrecs_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestPopulateFromSimilarStaffCounts(unittest.TestCase):
    def test_returns_expected_lazyframe(self):
        input_lf = pl.LazyFrame(
            Data.calculate_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
            orient="row",
        )
        returned_lf = job.populate_from_similar_staff_counts(input_lf)
        expected_lf = pl.LazyFrame(
            Data.expected_difference_within_range_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestSetSourceForNewlyPopulatedRows(unittest.TestCase):
    def test_returns_expected_lazyframe(self):
        input_lf = pl.LazyFrame(
            Data.source_missing_rows,
            Schemas.source_description_schema,
            orient="row",
        )
        returned_lf = job.set_source_for_newly_populated_rows(input_lf, "model_name")
        expected_lf = pl.LazyFrame(
            Data.expected_source_added_rows,
            Schemas.source_description_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)
