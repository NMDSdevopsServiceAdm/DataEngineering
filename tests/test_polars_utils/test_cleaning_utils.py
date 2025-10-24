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
    def setUp(self):
        self.sample_lf = pl.LazyFrame(
            data=Data.column_to_date_rows, schema=Schemas.sample_col_to_date_schema
        )

    def test_column_to_date_changes_data_type_to_date(self):
        returned_lf = job.column_to_date(self.sample_lf, "input_date_string")
        self.assertEqual(returned_lf.collect_schema().dtypes()[0], pl.Date())

    def test_column_to_date_inserts_new_column_if_specified(self):
        returned_lf = job.column_to_date(
            self.sample_lf, "input_date_string", "new_column"
        )
        self.assertTrue("new_column" in returned_lf.columns)
        self.assertEqual(returned_lf.collect_schema().dtypes()[2], pl.Date())

    def test_column_to_date_leaves_old_column_untouched_if_new_col_given(self):
        returned_lf = job.column_to_date(
            self.sample_lf, "input_date_string", "new_column"
        )
        pl_testing.assert_frame_equal(
            returned_lf.select("input_date_string"),
            self.sample_lf.select("input_date_string"),
        )

    def test_column_to_date_returns_expected_values(self):
        returned_lf = job.column_to_date(
            self.sample_lf, "input_date_string", "new_column"
        )
        pl_testing.assert_frame_equal(
            returned_lf.select("new_column"),
            self.sample_lf.select("expected_date_date").rename(
                {"expected_date_date": "new_column"}
            ),
        )
