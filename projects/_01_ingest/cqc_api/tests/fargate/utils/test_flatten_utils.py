import unittest

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.cqc_api.fargate.utils.flatten_utils as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    FlattenUtilsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    FlattenUtilsSchema as Schemas,
)


class CleanAndImputeRegistrationDateTests(unittest.TestCase):
    def test_does_not_change_valid_dates(self):
        # WHEN
        #   All dates are valid
        input_lf = pl.LazyFrame(
            data=Data.clean_registration_date_column_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_clean_registration_date_column_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_lf = job.clean_and_impute_registration_date(input_lf)

        # THEN
        #   The dates should be unchanged
        self.assertIsInstance(output_lf, pl.LazyFrame)
        pl_testing.assert_frame_equal(expected_lf, output_lf)

    def test_removes_time_from_datetime(self):
        # WHEN
        #   Dates are provided with a time element (YYYY-mm-dd HH:MM:SS)
        input_lf = pl.LazyFrame(
            data=Data.time_in_registration_date_column_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_time_in_registration_date_column_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_lf = job.clean_and_impute_registration_date(input_lf)

        # THEN
        #   The time elements should have been removed
        self.assertIsInstance(output_lf, pl.LazyFrame)
        pl_testing.assert_frame_equal(expected_lf, output_lf)

    def test_replaces_registration_dates_later_than_import_date_with_import_date(self):
        # WHEN
        #   The import date is before the registration date
        input_lf = pl.LazyFrame(
            data=Data.registration_date_after_import_date_column_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_registration_date_after_import_date_column_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_lf = job.clean_and_impute_registration_date(input_lf)

        # THEN
        #   The offending registration date should have been removed
        self.assertIsInstance(output_lf, pl.LazyFrame)
        pl_testing.assert_frame_equal(expected_lf, output_lf)

    def test_imputes_missing_registration_date_when_one_reg_date_for_location(self):
        # WHEN
        #   There is a missing value for a given location id, and just one registration date elsewhere for that location id
        input_lf = pl.LazyFrame(
            data=Data.registration_date_missing_single_reg_date_for_loc_column_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_registration_date_missing_single_reg_date_for_loc_column_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_lf = job.clean_and_impute_registration_date(input_lf)

        # THEN
        #   The value should be imputed to fill the missing registration date for that location id
        self.assertIsInstance(output_lf, pl.LazyFrame)
        pl_testing.assert_frame_equal(expected_lf, output_lf)

    def test_imputes_missing_registration_date_when_multiple_reg_date_for_location(
        self,
    ):
        # WHEN
        #   There is a missing value for a given location id, but multiple registration dates elsewhere for that location id
        input_lf = pl.LazyFrame(
            data=Data.registration_date_missing_multiple_reg_date_for_loc_column_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_registration_date_missing_multiple_reg_date_for_loc_column_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_lf = job.clean_and_impute_registration_date(input_lf)

        # THEN
        #   The earliest registration date for that location should be used
        self.assertIsInstance(output_lf, pl.LazyFrame)
        pl_testing.assert_frame_equal(expected_lf, output_lf)

    def test_imputes_missing_registration_date_from_first_import_date(
        self,
    ):
        # WHEN
        #   A location has no value for registration date at any point
        input_lf = pl.LazyFrame(
            data=Data.registration_date_missing_for_all_loc_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_registration_date_missing_for_all_loc_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_lf = job.clean_and_impute_registration_date(input_lf)

        # THEN
        #   The first import date for the location should be used
        self.assertIsInstance(output_lf, pl.LazyFrame)
        pl_testing.assert_frame_equal(expected_lf, output_lf)
        # THEN
        #   The first import date for the location should be used
        self.assertIsInstance(output_lf, pl.LazyFrame)
        pl_testing.assert_frame_equal(expected_lf, output_lf)
