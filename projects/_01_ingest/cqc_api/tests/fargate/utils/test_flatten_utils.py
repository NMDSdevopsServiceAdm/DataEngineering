import unittest

import polars as pl
import polars.testing as pl_testing

from projects._01_ingest.cqc_api.fargate.utils import flatten_utils as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    FlattenUtilsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    FlattenUtilsSchema as Schemas,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.utils.flatten_utils"


class ImputeMissingStructColumnsTests(unittest.TestCase):
    def test_single_struct_column_imputation(self):
        """Tests imputation for a single struct column with None and empty dicts."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_single_struct_col_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_single_struct_col_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_multiple_struct_columns_independent_imputation(self):
        """Ensures multiple struct columns are imputed independently and correctly."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_multiple_struct_cols_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_multiple_struct_cols_rows,
            schema=Schemas.expected_impute_missing_struct_two_cols_schema,
        )

        result_lf = job.impute_missing_struct_columns(
            lf, [CQCLClean.gac_service_types, CQCLClean.specialisms]
        )

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_empty_and_partial_structs(self):
        """Treats empty structs or all-null structs as missing, but preserves partially filled ones."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_empty_and_partial_structs_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_empty_and_partial_structs_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_imputation_partitions_correctly(self):
        """Verifies imputation does not leak values across location_id partitions."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_imputation_partitions_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_imputation_partitions_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_out_of_order_dates_are_ordered_before_imputation(self):
        """Ensures imputation follows chronological order based on date column."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_out_of_order_dates_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_out_of_order_dates_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_fully_null_column_remains_null_after_imputation(self):
        """If a struct column is entirely null, imputed column should remain all nulls."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_fully_null_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_fully_null_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_multiple_partitions_with_varied_missing_patterns(self):
        """Tests complex case with several partitions, ensuring each is filled correctly."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_multiple_partitions_and_missing_data_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_multiple_partitions_and_missing_data_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])

        pl_testing.assert_frame_equal(result_lf, expected_lf)


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
