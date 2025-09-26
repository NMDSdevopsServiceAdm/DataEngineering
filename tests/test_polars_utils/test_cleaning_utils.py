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


class TestCleaningUtilsAlignDates(unittest.TestCase):
    def setUp(self):
        self.primary_column = AWPClean.ascwds_workplace_import_date
        self.secondary_column = CQCLClean.cqc_location_import_date
        self.primary_df = pl.DataFrame(
            Data.align_dates_primary_rows, Schemas.align_dates_primary_schema
        )
        self.secondary_df = pl.DataFrame(
            Data.align_dates_secondary_rows, Schemas.align_dates_secondary_schema
        )
        self.expected_aligned_dates = pl.DataFrame(
            Data.expected_aligned_dates_rows, Schemas.expected_aligned_dates_schema
        )
        self.expected_cross_join_df = pl.DataFrame(
            Data.expected_cross_join_rows, Schemas.expected_aligned_dates_schema
        )
        self.expected_best_matches = pl.DataFrame(
            Data.expected_aligned_dates_rows, Schemas.expected_aligned_dates_schema
        )
        self.later_secondary_df = pl.DataFrame(
            Data.align_later_dates_secondary_rows, Schemas.align_dates_secondary_schema
        )
        self.expected_later_aligned_dates = pl.DataFrame(
            Data.expected_later_aligned_dates_rows,
            Schemas.expected_aligned_dates_schema,
        )
        self.merged_dates_df = pl.DataFrame(
            Data.expected_merged_rows, Schemas.expected_merged_dates_schema
        )
        self.later_merged_dates_df = pl.DataFrame(
            Data.expected_later_merged_rows, Schemas.expected_merged_dates_schema
        )
        self.column_order_for_assertion = [
            self.primary_column,
            self.secondary_column,
        ]

    def test_align_import_dates_completes(self):
        returned_df = job.align_import_dates(
            self.primary_df,
            self.secondary_df,
            self.primary_column,
            self.secondary_column,
        )

        self.assertFalse(returned_df.is_empty())

        # TODO make this into a real test with patching/mocking, job call and call counts.

    def test_align_import_dates_aligns_dates_correctly_when_secondary_data_starts_before_primary(
        self,
    ):
        returned_df = job.align_import_dates(
            self.primary_df,
            self.secondary_df,
            self.primary_column,
            self.secondary_column,
        )

        pl_testing.assert_frame_equal(
            returned_df.sort(self.primary_column),
            self.expected_aligned_dates.sort(self.primary_column),
        )

    def test_align_import_dates_aligns_dates_correctly_when_secondary_data_starts_later_than_primary(
        self,
    ):
        returned_df = job.align_import_dates(
            self.primary_df,
            self.later_secondary_df,
            self.primary_column,
            self.secondary_column,
        )

        pl_testing.assert_frame_equal(
            returned_df.sort(self.primary_column),
            self.expected_later_aligned_dates.sort(self.primary_column),
        )

    def test_cross_join_unique_dates_joins_correctly(self):
        returned_df = job.cross_join_unique_dates(
            self.primary_df,
            self.secondary_df,
            self.primary_column,
            self.secondary_column,
        )

        pl_testing.assert_frame_equal(
            returned_df.sort(self.primary_column, self.secondary_column),
            self.expected_cross_join_df.sort(
                self.primary_column, self.secondary_column
            ),
        )

    def test_determine_best_date_matches_selects_best_matches_correctly(self):
        returned_df = job.determine_best_date_matches(
            self.expected_cross_join_df,
            self.primary_column,
            self.secondary_column,
        )

        pl_testing.assert_frame_equal(
            returned_df.sort(self.primary_column, self.secondary_column),
            self.expected_best_matches.sort(self.primary_column, self.secondary_column),
        )

    def test_add_aligned_date_column_columns_completes(self):
        returned_df = job.add_aligned_date_column(
            self.primary_df,
            self.secondary_df,
            self.primary_column,
            self.secondary_column,
        )

        self.assertFalse(returned_df.is_empty())
        # TODO make this into a real test with patching/mocking, job call and call counts.

    def test_add_aligned_date_column_joins_secondary_date_correctly(
        self,
    ):
        returned_df = job.add_aligned_date_column(
            self.primary_df,
            self.secondary_df,
            self.primary_column,
            self.secondary_column,
        )

        pl_testing.assert_frame_equal(
            returned_df.sort(self.primary_column),
            self.merged_dates_df.select(returned_df.columns).sort(self.primary_column),
        )

    def test_add_aligned_date_column_joins_correctly_when_secondary_data_start_later_than_primary(
        self,
    ):
        returned_df = job.add_aligned_date_column(
            self.primary_df,
            self.later_secondary_df,
            self.primary_column,
            self.secondary_column,
        )

        pl_testing.assert_frame_equal(
            returned_df.select(self.column_order_for_assertion).sort(
                self.primary_column
            ),
            self.later_merged_dates_df.select(self.column_order_for_assertion).sort(
                self.primary_column
            ),
        )

    def test_add_aligned_date_column_returns_the_correct_number_of_rows(
        self,
    ):
        returned_df = job.add_aligned_date_column(
            self.primary_df,
            self.later_secondary_df,
            self.primary_column,
            self.secondary_column,
        )
        returned_rows = returned_df.height
        expected_rows = self.primary_df.height

        self.assertEqual(returned_rows, expected_rows)

    def test_add_aligned_date_column_returns_the_correct_number_of_columns(
        self,
    ):
        returned_df = job.add_aligned_date_column(
            self.primary_df,
            self.later_secondary_df,
            self.primary_column,
            self.secondary_column,
        )
        returned_columns = len(returned_df.columns)
        expected_columns = len(self.later_merged_dates_df.columns)

        self.assertEqual(returned_columns, expected_columns)
