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


class PolarsCleaningUtilsTests(unittest.TestCase):
    def setUp(self):
        pass


class AddAlignedDateColumnsTests(PolarsCleaningUtilsTests):
    def setUp(self):
        self.primary_column = CQCLClean.cqc_location_import_date
        self.secondary_column = AWPClean.ascwds_workplace_import_date

        self.primary_lf = pl.LazyFrame(
            Data.align_dates_primary_rows, Schemas.align_dates_primary_schema
        )
        self.secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_rows,
            Schemas.align_dates_secondary_schema,
        )
        self.column_order_for_assertion = [
            self.primary_column,
            self.secondary_column,
        ]
        self.returned_lf = job.add_aligned_date_column(
            self.primary_lf,
            self.secondary_lf,
            self.primary_column,
            self.secondary_column,
        )

    def test_expected_columns_are_returned(self):
        returned_columns = sorted(self.returned_lf.collect_schema().names())
        expected_columns = sorted(
            self.primary_lf.collect_schema().names() + [self.secondary_column]
        )

        self.assertEqual(returned_columns, expected_columns)

    def test_mix_of_misaligned_dates_joined_correctly(self):
        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_primary_with_secondary_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(
            self.returned_lf.select(self.column_order_for_assertion).sort(
                self.primary_column
            ),
            expected_lf.select(self.column_order_for_assertion).sort(
                self.primary_column
            ),
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

        pl_testing.assert_frame_equal(
            returned_lf.select(self.column_order_for_assertion),
            expected_lf.select(self.column_order_for_assertion),
        )

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
            returned_lf.select(self.column_order_for_assertion),
            expected_lf.select(self.column_order_for_assertion),
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
            returned_lf.select(self.column_order_for_assertion),
            expected_lf.select(self.column_order_for_assertion),
        )


class ColumnToDateTests(unittest.TestCase):
    def test_converts_date_string_without_hyphens_to_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_without_hyphens_rows,
            schema=Schemas.col_to_date_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_rows,
            schema=Schemas.expected_col_to_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

        self.assertEqual(returned_lf.collect_schema(), expected_lf.collect_schema())

    def test_converts_date_string_with_hyphens_to_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_with_hyphens_rows,
            schema=Schemas.col_to_date_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_rows,
            schema=Schemas.expected_col_to_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

        self.assertEqual(returned_lf.collect_schema(), expected_lf.collect_schema())

    def test_adds_a_new_column_with_converted_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_with_new_col_rows,
            schema=Schemas.col_to_date_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col", "new_date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_with_new_col_rows,
            schema=Schemas.expected_col_to_date_with_new_col_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

        self.assertEqual(returned_lf.collect_schema(), expected_lf.collect_schema())
