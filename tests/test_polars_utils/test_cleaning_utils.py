import unittest

import polars as pl
import polars.testing as pl_testing

import polars_utils.cleaning_utils as job
from tests.test_polars_utils_data import CleaningUtilsData as Data
from tests.test_polars_utils_schemas import CleaningUtilsSchemas as Schemas
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class PolarsCleaningUtilsTests(unittest.TestCase):
    def setUp(self):
        pass


class AddAlignedDateColumnsTests(PolarsCleaningUtilsTests):
    def setUp(self):
        self.primary_column = CQCLClean.cqc_location_import_date
        self.secondary_column = AWPClean.ascwds_workplace_import_date

    def test_mix_of_misaligned_dates_joined_correctly(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_rows, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_primary_with_secondary_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )

    def test_exact_match_date_joined_correctly(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_single_row, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_exact_match_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_secondary_exact_match_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_closest_historical_date_joined_correctly(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_single_row, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_closest_historical_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_secondary_closest_historical_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )

    def test_future_date_only_returns_null_secondary_column(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_single_row, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_future_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_secondary_future_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )


class ColumnToDateTests(unittest.TestCase):
    def test_converts_date_string_without_hyphens_to_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_string_without_hyphens_rows,
            schema=Schemas.col_to_date_string_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_rows,
            schema=Schemas.expected_col_to_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_converts_date_integer_without_hyphens_to_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_integer_without_hyphens_rows,
            schema=Schemas.col_to_date_integer_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_rows,
            schema=Schemas.expected_col_to_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_converts_date_string_with_hyphens_to_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_string_with_hyphens_rows,
            schema=Schemas.col_to_date_string_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_rows,
            schema=Schemas.expected_col_to_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_adds_a_new_column_with_converted_date(self):
        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_with_new_col_rows,
            schema=Schemas.expected_col_to_date_with_new_col_schema,
        )
        returned_lf = job.column_to_date(
            expected_lf.drop("new_date_col"), "date_col", "new_date_col"
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CalculateFilledPostsPerBedRatioTests(unittest.TestCase):
    def test_calculate_filled_posts_per_bed_ratio(self):
        expected_lf = pl.LazyFrame(
            Data.expected_filled_posts_per_bed_ratio_rows,
            Schemas.expected_filled_posts_per_bed_ratio_schema,
            orient="row",
        )
        returned_lf = job.calculate_filled_posts_per_bed_ratio(
            expected_lf.drop(IndCQC.filled_posts_per_bed_ratio),
            IndCQC.ascwds_filled_posts_dedup,
            IndCQC.filled_posts_per_bed_ratio,
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class ReduceDatasetToEarliestFilePerMonthTests(unittest.TestCase):
    def test_reduce_dataset_to_earliest_file_per_month_returns_correct_rows(self):
        test_lf = pl.LazyFrame(
            Data.reduce_dataset_to_earliest_file_per_month_rows,
            Schemas.reduce_dataset_to_earliest_file_per_month_schema,
            orient="row",
        )
        returned_lf = job.reduce_dataset_to_earliest_file_per_month(test_lf)
        expected_lf = pl.LazyFrame(
            Data.expected_reduce_dataset_to_earliest_file_per_month_rows,
            Schemas.reduce_dataset_to_earliest_file_per_month_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CreateBandedBedCountColumnTests(unittest.TestCase):
    def test_create_banded_bed_count_column(self):
        expected_lf = pl.LazyFrame(
            Data.expected_create_banded_bed_count_column_rows,
            Schemas.expected_create_banded_bed_count_column_schema,
            orient="row",
        )
        test_splits = [0, 1, 25, float("Inf")]
        returned_lf = job.create_banded_bed_count_column(
            expected_lf.drop(IndCQC.number_of_beds_banded),
            IndCQC.number_of_beds_banded,
            test_splits,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
