import unittest

import polars as pl
import polars.testing as pl_testing
from openpyxl.styles.builtins import output

import projects._01_ingest.cqc_api.fargate.delta_clean_cqc_locations as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    CQCLocationsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    CQCLocationsSchema as Schemas,
)


class MainTests(unittest.TestCase):
    def test_main(self):
        pass


class CleanProviderIdColumnTests(unittest.TestCase):
    def test_does_not_change_valid_ids(self):
        # GIVEN
        input_df = pl.DataFrame(
            data=Data.clean_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )

        # WHEN
        output_df = job.clean_provider_id_column(input_df)

        # THEN
        self.assertIsInstance(output_df, pl.DataFrame)
        pl_testing.assert_frame_equal(output_df, input_df)

    def test_removes_long_provider_ids(self):
        # GIVEN
        input_df = pl.DataFrame(
            data=Data.long_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )
        expected_df = pl.DataFrame(
            data=Data.expected_long_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )

        # WHEN
        output_df = job.clean_provider_id_column(input_df)

        # THEN
        self.assertIsInstance(output_df, pl.DataFrame)
        pl_testing.assert_frame_equal(expected_df, output_df)

    def test_fills_missing_provider_id(self):
        # GIVEN
        input_df = pl.DataFrame(
            data=Data.missing_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )
        expected_df = pl.DataFrame(
            data=Data.expected_fill_missing_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )

        # WHEN
        output_df = job.clean_provider_id_column(input_df)

        # THEN
        self.assertIsInstance(output_df, pl.DataFrame)
        pl_testing.assert_frame_equal(expected_df, output_df)


class CleanAndImputeRegistrationDateTests(unittest.TestCase):
    def test_does_not_change_valid_dates(self):
        # WHEN
        input_df = pl.DataFrame(
            data=Data.clean_registration_date_column_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_df = pl.DataFrame(
            data=Data.expected_clean_registration_date_column_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_df = job.clean_and_impute_registration_date(input_df)

        # THEN
        self.assertIsInstance(output_df, pl.DataFrame)
        pl_testing.assert_frame_equal(expected_df, output_df)

    def test_removes_time_from_datetime(self):
        # WHEN
        input_df = pl.DataFrame(
            data=Data.time_in_registration_date_column_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_df = pl.DataFrame(
            data=Data.expected_time_in_registration_date_column_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_df = job.clean_and_impute_registration_date(input_df)

        # THEN
        self.assertIsInstance(output_df, pl.DataFrame)
        pl_testing.assert_frame_equal(expected_df, output_df)

    def test_removes_registration_dates_later_than_import_date(self):
        # WHEN
        input_df = pl.DataFrame(
            data=Data.registration_date_after_import_date_column_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_df = pl.DataFrame(
            data=Data.expected_registration_date_after_import_date_column_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_df = job.clean_and_impute_registration_date(input_df)

        # THEN
        self.assertIsInstance(output_df, pl.DataFrame)
        pl_testing.assert_frame_equal(expected_df, output_df)

    def test_imputes_missing_registration_date_when_one_reg_date_for_location(self):
        # WHEN
        input_df = pl.DataFrame(
            data=Data.registration_date_missing_single_reg_date_for_loc_column_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_df = pl.DataFrame(
            data=Data.expected_registration_date_missing_single_reg_date_for_loc_column_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_df = job.clean_and_impute_registration_date(input_df)

        # THEN
        self.assertIsInstance(output_df, pl.DataFrame)
        pl_testing.assert_frame_equal(expected_df, output_df)

    def test_imputes_missing_registration_date_when_multiple_reg_date_for_location(
        self,
    ):
        # WHEN
        input_df = pl.DataFrame(
            data=Data.registration_date_missing_multiple_reg_date_for_loc_column_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_df = pl.DataFrame(
            data=Data.expected_registration_date_missing_multiple_reg_date_for_loc_column_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_df = job.clean_and_impute_registration_date(input_df)

        # THEN
        self.assertIsInstance(output_df, pl.DataFrame)
        pl_testing.assert_frame_equal(expected_df, output_df)

    def test_imputes_missing_registration_date_from_first_import_date(
        self,
    ):
        # WHEN
        input_df = pl.DataFrame(
            data=Data.registration_date_missing_for_all_loc_rows,
            schema=Schemas.clean_registration_date_column_input_schema,
        )
        expected_df = pl.DataFrame(
            data=Data.expected_registration_date_missing_for_all_loc_rows,
            schema=Schemas.clean_registration_date_column_output_schema,
        )

        # WHEN
        output_df = job.clean_and_impute_registration_date(input_df)

        # THEN
        self.assertIsInstance(output_df, pl.DataFrame)
        pl_testing.assert_frame_equal(expected_df, output_df)


if __name__ == "__main__":
    unittest.main()
