import unittest

import polars as pl
import polars.testing as pl_testing

import polars_utils.raw_data_adjustments as job
from tests.test_polars_utils_data import RawDataAdjustmentsData as Data
from tests.test_polars_utils_schemas import RawDataAdjustmentsSchemas as Schemas


class IsValidLocationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_with_multiple_rows_to_remove_lf = pl.LazyFrame(
            Data.locations_data_with_multiple_rows_to_remove,
            Schemas.locations_data_schema,
        )
        self.test_with_single_rows_to_remove_lf = pl.LazyFrame(
            Data.locations_data_with_single_rows_to_remove,
            Schemas.locations_data_schema,
        )
        self.test_without_rows_to_remove_df = pl.LazyFrame(
            Data.locations_data_without_rows_to_remove,
            Schemas.locations_data_schema,
        )
        self.expected_lf = pl.LazyFrame(
            Data.locations_data_without_rows_to_remove, Schemas.locations_data_schema
        )

    def test_remove_records_from_locations_data_removes_multiple_rows_when_they_match_the_criteria(
        self,
    ):
        returned_lf = self.test_with_multiple_rows_to_remove_lf.filter(
            job.is_valid_location()
        )

        pl_testing.assert_frame_equal(returned_lf, self.expected_lf)

    def test_remove_records_from_locations_data_removes_single_rows_when_it_matches_the_criteria(
        self,
    ):
        returned_lf = self.test_with_single_rows_to_remove_lf.filter(
            job.is_valid_location()
        )

        pl_testing.assert_frame_equal(returned_lf, self.expected_lf)

    def test_remove_records_from_locations_data_does_not_remove_rows_when_they_do_not_match_the_criteria(
        self,
    ):
        returned_lf = self.test_without_rows_to_remove_df.filter(
            job.is_valid_location()
        )

        pl_testing.assert_frame_equal(returned_lf, self.expected_lf)
