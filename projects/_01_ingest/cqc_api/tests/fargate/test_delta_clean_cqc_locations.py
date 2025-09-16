import unittest
from unittest.mock import patch

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

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.delta_clean_cqc_locations"


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


class ImputeHistoricRelationshipsTest(unittest.TestCase):

    @patch(f"{PATCH_PATH}.get_predecessor_relationships")
    def test_when_no_relationships_returns_null_imputed_relationships(
        self, mock_get_predecessor_relationships
    ):
        # GIVEN
        #   Data where relationship column is null for all rows for a location
        input_df = pl.DataFrame(
            data=Data.impute_historic_relationships_no_relationships_rows,
            schema=Schemas.impute_historic_relationships_input_schema,
        )
        #   get_predecessor_relationships should return a null relationships_predecessors_only column
        predecessor_output = input_df.with_columns(
            pl.lit(None).alias("relationships_predecessors_only"),
            pl.lit(None).alias("first_known_relationships"),
        )
        mock_get_predecessor_relationships.return_value = predecessor_output

        # WHEN
        output_df = job.impute_historic_relationships(input_df)

        # THEN
        #   The returned dataframe should have just one new column - imputed_relationships, with null values
        expected_df = pl.DataFrame(
            data=Data.expected_impute_historic_relationships_no_relationships_rows,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df, check_column_order=False)

        #   The mock predecessor function should have been called once, with the input dataset + a null first_known_relationships column
        expected_get_predecessor_relationships_input = input_df.with_columns(
            pl.lit(None).alias("first_known_relationships")
        )
        mock_get_predecessor_relationships.assert_called_once()
        mock_predecessor_relationships_output = (
            mock_get_predecessor_relationships.call_args.args[0]
        )
        pl.testing.assert_frame_equal(
            expected_get_predecessor_relationships_input,
            mock_predecessor_relationships_output,
            check_column_order=False,
            check_dtype=False,
        )

    @patch(f"{PATCH_PATH}.get_predecessor_relationships")
    def test_uses_relationships_column_if_not_null(
        self, mock_get_predecessor_relationships
    ):
        # GIVEN
        #   Data where every row has relationships populated
        input_df = pl.DataFrame(
            data=Data.impute_historic_relationships_all_populated,
            schema=Schemas.impute_historic_relationships_input_schema,
        )
        #   get_predecessor_relationships should return a relationships_predecessors_only with values that WILL NOT BE USED
        dummy_value = [
            {
                "relatedLocationId": "UnusedID",
                "relatedLocationName": "UnusedName",
                "type": "UnusedType",
                "reason": "UnusedReason",
            }
        ]
        mock_get_predecessor_relationships.side_effect = lambda x: x.with_columns(
            pl.lit(dummy_value).alias("relationships_predecessors_only")
        )

        # WHEN
        output_df = job.impute_historic_relationships(input_df)

        # THEN
        #   The returned dataframe should have imputed_relationships equal to relationships
        expected_df = pl.DataFrame(
            data=Data.expected_impute_historic_relationships_all_populated,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)

    @patch(f"{PATCH_PATH}.get_predecessor_relationships")
    def test_uses_first_known_relationships_when_deregistered(
        self, mock_get_predecessor_relationships
    ):
        # GIVEN
        #   Data for the same location at 3 points, where relationships in the final timepoint is missing
        input_df = pl.DataFrame(
            data=Data.impute_historic_relationships_deregistered,
            schema=Schemas.impute_historic_relationships_input_schema,
        )
        #   get_predecessor_relationships should return a relationships_predecessors_only with values that WILL NOT BE USED
        dummy_value = [
            {
                "relatedLocationId": "UnusedID",
                "relatedLocationName": "UnusedName",
                "type": "UnusedType",
                "reason": "UnusedReason",
            }
        ]
        mock_get_predecessor_relationships.side_effect = lambda x: x.with_columns(
            pl.lit(dummy_value).alias("relationships_predecessors_only")
        )

        # WHEN
        output_df = job.impute_historic_relationships(input_df)

        # THEN
        #   The first value for the location id should have been filled into the missing row
        expected_df = pl.DataFrame(
            data=Data.expected_impute_historic_relationships_deregistered,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )

        pl_testing.assert_frame_equal(expected_df, output_df)

    @patch(f"{PATCH_PATH}.get_predecessor_relationships")
    def test_uses_predecessor_when_registered(self, mock_get_predecessor_relationships):
        # GIVEN
        #   Data for the same location at 3 points, where relationships in the final timepoint is missing
        input_df = pl.DataFrame(
            data=Data.impute_historic_relationships_registered,
            schema=Schemas.impute_historic_relationships_input_schema,
        )
        #   get_predecessor_relationships returns no predecessors
        dummy_value = [
            {
                "relatedLocationId": "PredecessorID",
                "relatedLocationName": "PredecessorName",
                "type": "PredecessorType",
                "reason": "PredecessorReason",
            }
        ]
        mock_get_predecessor_relationships.side_effect = lambda x: x.with_columns(
            pl.lit(dummy_value).alias("relationships_predecessors_only")
        )

        # WHEN
        output_df = job.impute_historic_relationships(input_df)

        # THEN
        #   The dummy value should have been imputed
        expected_df = pl.DataFrame(
            data=Data.expected_impute_historic_relationships_registered,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )

        pl_testing.assert_frame_equal(expected_df, output_df)

    @patch(f"{PATCH_PATH}.get_predecessor_relationships")
    def test_when_no_predecessor_registered_returns_null_imputed_relationships(
        self, mock_get_predecessor_relationships
    ):
        # GIVEN
        #   Data for the same location at 3 points, where relationships in the final timepoint is missing
        input_df = pl.DataFrame(
            data=Data.impute_historic_relationships_registered,
            schema=Schemas.impute_historic_relationships_input_schema,
        )
        #   get_predecessor_relationships should return a relationships_predecessors_only with values that WILL BE USED
        mock_get_predecessor_relationships.side_effect = lambda x: x.with_columns(
            pl.lit(None).alias("relationships_predecessors_only")
        )

        # WHEN
        output_df = job.impute_historic_relationships(input_df)

        # THEN
        #   The missing row should have a missing imputed value
        expected_df = pl.DataFrame(
            data=Data.expected_impute_historic_relationships_registered_no_predecessor,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )

        pl_testing.assert_frame_equal(expected_df, output_df)


if __name__ == "__main__":
    unittest.main()
