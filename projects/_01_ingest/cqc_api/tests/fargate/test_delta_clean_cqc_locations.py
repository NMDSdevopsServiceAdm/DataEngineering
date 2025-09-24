import unittest
from unittest import TestCase
from unittest.mock import patch, ANY

import polars as pl
import polars.testing as pl_testing

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


class ImputeHistoricRelationshipsTests(unittest.TestCase):

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


class GetPredecessorRelationshipsTests(unittest.TestCase):
    def test_when_no_relationships_returns_null_predecessors(self):
        # GIVEN
        #   Input where all rows have no first known relationship
        input_df = pl.DataFrame(
            data=Data.get_predecessor_relationships_null_first_known,
            schema=Schemas.get_predecessor_relationships_input_schema,
        )

        # WHEN
        output_df = job.get_predecessor_relationships(input_df)

        # THEN
        #   The predecessor relationship column should be null for all rows
        expected_df = pl.DataFrame(
            data=Data.expected_get_predecessor_relationships_null_first_known,
            schema=Schemas.expected_get_predecessor_relationships_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            [None, None], output_df["relationships_predecessors_only"].to_list()
        )

    def test_when_relationships_successor_only_returns_null_predecessors(self):
        # GIVEN
        #   Input where all rows have only successor relationships
        input_df = pl.DataFrame(
            data=Data.get_predecessor_relationships_successor_first_known,
            schema=Schemas.get_predecessor_relationships_input_schema,
        )

        # WHEN
        output_df = job.get_predecessor_relationships(input_df)

        # THEN
        #   The predecessor relationship column should be null for all rows
        expected_df = pl.DataFrame(
            data=Data.expected_get_predecessor_relationships_successor_first_known,
            schema=Schemas.expected_get_predecessor_relationships_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            [None, None], output_df["relationships_predecessors_only"].to_list()
        )

    def test_when_relationships_predecessor_only_returns_predecessor(self):
        # GIVEN
        #   Input where all rows have a predecessor relationship
        input_df = pl.DataFrame(
            data=Data.get_predecessor_relationships_predecessor_first_known,
            schema=Schemas.get_predecessor_relationships_input_schema,
        )

        # WHEN
        output_df = job.get_predecessor_relationships(input_df)

        # THEN
        #   The predecessor relationship column should have the predecessor values
        expected_df = pl.DataFrame(
            data=Data.expected_get_predecessor_relationships_predecessor_first_known,
            schema=Schemas.expected_get_predecessor_relationships_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            ["HSCA Predecessor", "HSCA Predecessor"],
            [
                r[0]["type"]
                for r in output_df["relationships_predecessors_only"].to_list()
            ],
        )

    def test_when_relationships_both_types_only_returns_predecessors(self):
        # GIVEN
        #   Input where all rows have a predecessor relationship and a successor relationship
        input_df = pl.DataFrame(
            data=Data.get_predecessor_relationships_both_types,
            schema=Schemas.get_predecessor_relationships_input_schema,
        )

        # WHEN
        output_df = job.get_predecessor_relationships(input_df)

        # THEN
        #   The predecessor relationship column should have the predecessor values
        expected_df = pl.DataFrame(
            data=Data.expected_get_predecessor_relationships_both_types,
            schema=Schemas.expected_get_predecessor_relationships_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            ["HSCA Predecessor", "HSCA Predecessor"],
            [
                r[0]["type"]
                for r in output_df["relationships_predecessors_only"].to_list()
            ],
        )

    def test_when_multiple_predecessors_returns_aggregated_predecessors(self):
        # GIVEN
        #   Input where there are multiple predecessor relationships for a location
        input_df = pl.DataFrame(
            data=Data.get_predecessor_multiple_predecessors,
            schema=Schemas.get_predecessor_relationships_input_schema,
        )

        # WHEN
        output_df = job.get_predecessor_relationships(input_df)

        # THEN
        #   The predecessor relationship column should have the predecessor values aggregated
        expected_df = pl.DataFrame(
            data=Data.expected_get_predecessor_multiple_predecessors,
            schema=Schemas.expected_get_predecessor_relationships_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            ["HSCA Predecessor", "HSCA Predecessor"],
            [
                r["type"]
                for r in output_df["relationships_predecessors_only"].to_list()[0]
            ],
        )


class ImputeMissingValuesForStructColumnTests(unittest.TestCase):
    def test_does_not_impute_if_existing_value(self):
        # GIVEN
        #   Input where the GAC service column is fully populated
        input_df = pl.DataFrame(
            data=Data.impute_struct_existing_values,
            schema=Schemas.impute_struct_input_schema,
        )

        # WHEN
        output_df = job.impute_missing_values_for_struct_column(
            input_df, "gacServiceTypes"
        )

        # THEN
        #   The imputed values should be equal to the input
        expected_df = pl.DataFrame(
            data=Data.expected_impute_struct_existing_values,
            schema=Schemas.expected_impute_struct_input_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            ["Name A", "Name B", "Name C"],
            [r[0]["name"] for r in output_df["imputed_gacServiceTypes"].to_list()],
        )

    def test_imputes_struct_backwards_if_possible(self):
        # GIVEN
        #   Input where the GAC service column has a missing middle value
        input_df = pl.DataFrame(
            data=Data.impute_struct_from_historic,
            schema=Schemas.impute_struct_input_schema,
        )

        # WHEN
        output_df = job.impute_missing_values_for_struct_column(
            input_df, "gacServiceTypes"
        )

        # THEN
        #   The missing value should be imputed from the historic row
        expected_df = pl.DataFrame(
            data=Data.expected_impute_struct_from_historic,
            schema=Schemas.expected_impute_struct_input_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)

        self.assertEqual(
            ["Name A", "Name A", "Name C"],
            [r[0]["name"] for r in output_df["imputed_gacServiceTypes"].to_list()],
        )

    def test_imputes_forwards_if_no_previous_value(self):
        # GIVEN
        #   Input where there is no historic value to impute from
        input_df = pl.DataFrame(
            data=Data.impute_struct_from_future,
            schema=Schemas.impute_struct_input_schema,
        )

        # WHEN
        output_df = job.impute_missing_values_for_struct_column(
            input_df, "gacServiceTypes"
        )

        # THEN
        #   The missing values should be imputed from the future row
        expected_df = pl.DataFrame(
            data=Data.expected_impute_struct_from_future,
            schema=Schemas.expected_impute_struct_input_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            ["Name C", "Name C", "Name C"],
            [r[0]["name"] for r in output_df["imputed_gacServiceTypes"].to_list()],
        )

    def test_when_no_values_returns_null_imputed_values(self):
        # GIVEN
        #   Input where there is no values for a particular location id to impute from
        input_df = pl.DataFrame(
            data=Data.impute_struct_null_values,
            schema=Schemas.impute_struct_input_schema,
        )

        # WHEN
        output_df = job.impute_missing_values_for_struct_column(
            input_df, "gacServiceTypes"
        )

        # THEN
        #   The location with no values should have null in the imputed column
        expected_df = pl.DataFrame(
            data=Data.expected_impute_struct_null_values,
            schema=Schemas.expected_impute_struct_input_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            [None, None, ANY],
            output_df["imputed_gacServiceTypes"].to_list(),
        )


class AssignPrimaryServiceTypeTests(unittest.TestCase):
    def test_assigns_care_home_with_nursing(self):
        # GIVEN
        #   Input where all rows have "Care home service with nursing" as one of their inputs
        input_df = pl.DataFrame(
            data=Data.allocate_primary_service_care_home_with_nursing,
            schema=Schemas.allocate_primary_service_input_schema,
        )

        # WHEN
        output_df = job.assign_primary_service_type(
            input_df,
        )

        # THEN
        #   All the rows should have been allocated as "Care home with nursing"
        expected_df = pl.DataFrame(
            data=Data.expected_allocate_primary_service_care_home_with_nursing,
            schema=Schemas.expected_allocate_primary_service_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            [
                "Care home with nursing",
                "Care home with nursing",
                "Care home with nursing",
            ],
            output_df["primary_service_type"].to_list(),
        )

    def test_assigns_care_home_only(self):
        # GIVEN
        #   Input where all rows have "Care home service without nursing" as one of their inputs
        #   and none have the preferential "Care home service with nursing"
        input_df = pl.DataFrame(
            data=Data.allocate_primary_service_care_home_only,
            schema=Schemas.allocate_primary_service_input_schema,
        )

        # WHEN
        output_df = job.assign_primary_service_type(
            input_df,
        )

        # THEN
        #   All the rows should have been allocated as "Care home without nursing"
        expected_df = pl.DataFrame(
            data=Data.expected_allocate_primary_service_care_home_only,
            schema=Schemas.expected_allocate_primary_service_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            [
                "Care home without nursing",
                "Care home without nursing",
                "Care home without nursing",
            ],
            output_df["primary_service_type"].to_list(),
        )

    def test_assigns_non_residential(self):
        # GIVEN
        #   Input where no rows have "Care home service with nursing" or "Care home service without nursing"
        input_df = pl.DataFrame(
            data=Data.allocate_primary_service_non_residential,
            schema=Schemas.allocate_primary_service_input_schema,
        )

        # WHEN
        output_df = job.assign_primary_service_type(
            input_df,
        )

        # THEN
        #   All the rows should have been allocated as "non-residential"
        expected_df = pl.DataFrame(
            data=Data.expected_allocate_primary_service_non_residential,
            schema=Schemas.expected_allocate_primary_service_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            [
                "non-residential",
                "non-residential",
            ],
            output_df["primary_service_type"].to_list(),
        )

    def test_assigns_all_types(self):
        # GIVEN
        #   Input where rows have a range of imputed services
        input_df = pl.DataFrame(
            data=Data.allocate_primary_service_all_types,
            schema=Schemas.allocate_primary_service_input_schema,
        )

        # WHEN
        output_df = job.assign_primary_service_type(
            input_df,
        )

        # THEN
        #   All the rows should have been allocated one of each type
        expected_df = pl.DataFrame(
            data=Data.expected_allocate_primary_service_all_types,
            schema=Schemas.expected_allocate_primary_service_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            [
                "Care home with nursing",
                "Care home without nursing",
                "non-residential",
            ],
            output_df["primary_service_type"].to_list(),
        )


class AssignCareHomeTests(unittest.TestCase):
    def test_assigns_care_homes(self):
        # GIVEN
        #   Input where rows have primary service of either care_home_with_nursing or care_home_only
        input_df = pl.DataFrame(
            data=Data.align_care_home_care_homes_rows,
            schema=Schemas.align_care_home_input_schema,
        )

        # WHEN
        output_df = job.assign_care_home(
            input_df,
        )

        # THEN
        #   All the rows should have been allocated as care homes
        expected_df = pl.DataFrame(
            data=Data.expected_align_care_home_care_homes_rows,
            schema=Schemas.expected_align_care_home_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            [
                "Y",
                "Y",
            ],
            output_df["careHome"].to_list(),
        )

    def test_assigns_non_care_homes(self):
        # GIVEN
        #   Input where rows have primary service of not care_home_with_nursing or care_home_only
        input_df = pl.DataFrame(
            data=Data.align_care_home_non_care_homes_rows,
            schema=Schemas.align_care_home_input_schema,
        )

        # WHEN
        output_df = job.assign_care_home(
            input_df,
        )

        # THEN
        #   All the rows should have been allocated as not care homes
        expected_df = pl.DataFrame(
            data=Data.expected_align_care_home_non_care_homes_rows,
            schema=Schemas.expected_align_care_home_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            [
                "N",
                "N",
            ],
            output_df["careHome"].to_list(),
        )


class AddRelatedLocationFlagTests(unittest.TestCase):
    def test_flags_y_when_related_locations(self):
        # GIVEN
        #   Input where rows have 1 or more imputed relationships
        input_df = pl.DataFrame(
            data=Data.related_location_flag_with_related_locations,
            schema=Schemas.related_location_flag_input_schema,
        )

        # WHEN
        output_df = job.add_related_location_flag(
            input_df,
        )

        # THEN
        #   All the rows should be flagged as Y for related location
        expected_df = pl.DataFrame(
            data=Data.expected_related_location_flag_with_related_locations,
            schema=Schemas.expected_related_location_flag_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            [
                "Y",
                "Y",
            ],
            output_df["related_location"].to_list(),
        )

    def test_flags_n_when_no_related_locations(self):
        # GIVEN
        #   Input where rows have no imputed relationships
        input_df = pl.DataFrame(
            data=Data.related_location_flag_with_no_related_locations,
            schema=Schemas.related_location_flag_input_schema,
        )

        # WHEN
        output_df = job.add_related_location_flag(
            input_df,
        )

        # THEN
        #   All the rows should be flagged as N for related location
        expected_df = pl.DataFrame(
            data=Data.expected_related_location_flag_with_no_related_locations,
            schema=Schemas.expected_related_location_flag_schema,
        )
        pl_testing.assert_frame_equal(expected_df, output_df)
        self.assertEqual(
            [
                "N",
                "N",
            ],
            output_df["related_location"].to_list(),
        )


@patch(f"{PATCH_PATH}.remove_rows", return_value=["a", "b"])
class RemoveSpecialistCollegesTests(unittest.TestCase):
    def setUp(self):
        self.input_fact_df = pl.DataFrame(
            data=Data.remove_specialist_colleges_fact,
            schema=Schemas.remove_specialist_colleges_fact_input_schema,
        )

    def test_removes_rows_where_specialist_college_is_only_service(
        self, mock_remove_rows
    ):
        # GIVEN
        #   Input where all rows have specialist colleges as their only service offered
        input_dim_df = pl.DataFrame(
            data=Data.remove_specialist_colleges_dim_only_specialist_college,
            schema=Schemas.remove_specialist_colleges_dim_input_schema,
        )

        # WHEN
        job.remove_specialist_colleges(
            self.input_fact_df,
            input_dim_df,
        )

        # THEN
        expected_to_remove_df = pl.DataFrame(
            data=Data.expected_remove_specialist_colleges_dim_only_specialist_college,
            schema=Schemas.expected_remove_specialist_colleges_schema,
        )
        #   The row removal function should have been called once
        mock_remove_rows.assert_called_once()
        mock_call_args = mock_remove_rows.call_args.kwargs
        #   The input fact and dimension df should be unchanged
        pl_testing.assert_frame_equal(
            self.input_fact_df, mock_call_args["target_dfs"][0]
        )
        pl_testing.assert_frame_equal(input_dim_df, mock_call_args["target_dfs"][1])
        #   All the rows should be passed in to the mock function as to be removed
        pl_testing.assert_frame_equal(
            expected_to_remove_df, mock_call_args["to_remove_df"]
        )

    def test_does_not_remove_rows_where_specialist_college_is_one_of_many_services(
        self, mock_remove_rows
    ):
        # GIVEN
        #   Input where all rows have services offered that include specialist college and at least one other service
        input_dim_df = pl.DataFrame(
            data=Data.remove_specialist_colleges_dim_specialist_college_plus_other,
            schema=Schemas.remove_specialist_colleges_dim_input_schema,
        )

        # WHEN
        job.remove_specialist_colleges(
            self.input_fact_df,
            input_dim_df,
        )

        # THEN
        expected_to_remove_df = pl.DataFrame(
            data=Data.expected_remove_specialist_colleges_remove_none,
            schema=Schemas.expected_remove_specialist_colleges_schema,
        )
        #   The row removal function should have been called once
        mock_remove_rows.assert_called_once()
        mock_call_args = mock_remove_rows.call_args.kwargs
        #   The input fact and dimension df should be unchanged
        pl_testing.assert_frame_equal(
            self.input_fact_df, mock_call_args["target_dfs"][0]
        )
        pl_testing.assert_frame_equal(input_dim_df, mock_call_args["target_dfs"][1])
        #   No rows should be passed in to the mock function as to be removed
        pl_testing.assert_frame_equal(
            expected_to_remove_df, mock_call_args["to_remove_df"]
        )

    def test_does_not_remove_rows_where_specialist_college_is_not_a_service(
        self, mock_remove_rows
    ):
        # GIVEN
        #   Input where no rows have specialist college in their services offered
        input_dim_df = pl.DataFrame(
            data=Data.remove_specialist_colleges_dim_no_specialist_college,
            schema=Schemas.remove_specialist_colleges_dim_input_schema,
        )

        # WHEN
        job.remove_specialist_colleges(
            self.input_fact_df,
            input_dim_df,
        )

        # THEN
        expected_to_remove_df = pl.DataFrame(
            data=Data.expected_remove_specialist_colleges_remove_none,
            schema=Schemas.expected_remove_specialist_colleges_schema,
        )
        #   The row removal function should have been called once
        mock_remove_rows.assert_called_once()
        mock_call_args = mock_remove_rows.call_args.kwargs
        #   The input fact and dimension df should be unchanged
        pl_testing.assert_frame_equal(
            self.input_fact_df, mock_call_args["target_dfs"][0]
        )
        pl_testing.assert_frame_equal(input_dim_df, mock_call_args["target_dfs"][1])
        #   No rows should be passed in to the mock function as to be removed
        pl_testing.assert_frame_equal(
            expected_to_remove_df, mock_call_args["to_remove_df"]
        )

    def test_does_not_removes_rows_where_there_is_no_service_offered(
        self, mock_remove_rows
    ):
        # GIVEN
        #   Input where no rows have services offered (services offered is null or empty list)
        input_dim_df = pl.DataFrame(
            data=Data.remove_specialist_colleges_dim_no_services_offered,
            schema=Schemas.remove_specialist_colleges_dim_input_schema,
        )

        # WHEN
        job.remove_specialist_colleges(
            self.input_fact_df,
            input_dim_df,
        )

        # THEN
        expected_to_remove_df = pl.DataFrame(
            data=Data.expected_remove_specialist_colleges_remove_none,
            schema=Schemas.expected_remove_specialist_colleges_schema,
        )
        #   The row removal function should have been called once
        mock_remove_rows.assert_called_once()
        mock_call_args = mock_remove_rows.call_args.kwargs
        #   The input fact and dimension df should be unchanged
        pl_testing.assert_frame_equal(
            self.input_fact_df, mock_call_args["target_dfs"][0]
        )
        pl_testing.assert_frame_equal(input_dim_df, mock_call_args["target_dfs"][1])
        #   No rows should be passed in to the mock function as to be removed
        pl_testing.assert_frame_equal(
            expected_to_remove_df, mock_call_args["to_remove_df"]
        )


@patch(f"{PATCH_PATH}.remove_rows", return_value=["a", "b"])
class RemoveLocationsWithoutRegulatedActivitiesTests(unittest.TestCase):
    def setUp(self):
        self.input_fact_df = pl.DataFrame(
            data=Data.remove_locations_without_ra_fact,
            schema=Schemas.remove_locations_without_ra_fact_schema,
        )

    def test_removes_locations_without_regulated_activities(self, mock_remove_rows):
        # GIVEN
        #   Input where all rows have no regulated activities
        input_dim_df = pl.DataFrame(
            data=Data.remove_locations_without_ra_dim_without_ra,
            schema=Schemas.remove_locations_without_ra_dim_schema,
        )

        # WHEN
        job.remove_locations_without_regulated_activities(
            self.input_fact_df,
            input_dim_df,
        )

        # THEN
        expected_to_remove_df = pl.DataFrame(
            data=Data.expected_remove_locations_without_ra_dim_without_ra_to_remove,
            schema=Schemas.expected_remove_locations_without_ra_to_remove_schema,
        )
        #   The row removal function should have been called once
        mock_remove_rows.assert_called_once()
        mock_call_args = mock_remove_rows.call_args.kwargs
        #   The input fact and dimension df should be unchanged
        pl_testing.assert_frame_equal(
            self.input_fact_df, mock_call_args["target_dfs"][0]
        )
        pl_testing.assert_frame_equal(input_dim_df, mock_call_args["target_dfs"][1])
        #   All the rows should be passed in to the mock function as to be removed
        pl_testing.assert_frame_equal(
            expected_to_remove_df, mock_call_args["to_remove_df"]
        )

    def test_does_not_remove_rows_with_regulated_activities(self, mock_remove_rows):
        # GIVEN
        #   Input where all rows have regulated activities
        input_dim_df = pl.DataFrame(
            data=Data.remove_locations_without_ra_dim_with_ra,
            schema=Schemas.remove_locations_without_ra_dim_schema,
        )

        # WHEN
        job.remove_locations_without_regulated_activities(
            self.input_fact_df,
            input_dim_df,
        )

        # THEN
        expected_to_remove_df = pl.DataFrame(
            data=Data.expected_remove_locations_without_ra_dim_with_ra_to_remove,
            schema=Schemas.expected_remove_locations_without_ra_to_remove_schema,
        )
        #   The row removal function should have been called once
        mock_remove_rows.assert_called_once()
        mock_call_args = mock_remove_rows.call_args.kwargs
        #   The input fact and dimension df should be unchanged
        pl_testing.assert_frame_equal(
            self.input_fact_df, mock_call_args["target_dfs"][0]
        )
        pl_testing.assert_frame_equal(input_dim_df, mock_call_args["target_dfs"][1])
        #   No rows should be passed in to the mock function as to be removed
        pl_testing.assert_frame_equal(
            expected_to_remove_df, mock_call_args["to_remove_df"]
        )

    def test_returns_empty_df_when_empty_input_df(self, mock_remove_rows):
        # GIVEN
        #   Input where there are no rows
        input_dim_df = pl.DataFrame(
            data=Data.remove_locations_without_ra_empty,
            schema=Schemas.remove_locations_without_ra_dim_schema,
        )

        # WHEN
        job.remove_locations_without_regulated_activities(
            self.input_fact_df,
            input_dim_df,
        )

        # THEN
        expected_to_remove_df = pl.DataFrame(
            data=Data.expected_remove_locations_without_ra_empty_to_remove,
            schema=Schemas.expected_remove_locations_without_ra_to_remove_schema,
        )
        #   The row removal function should have been called once
        mock_remove_rows.assert_called_once()
        mock_call_args = mock_remove_rows.call_args.kwargs
        #   The input fact and dimension df should be unchanged
        pl_testing.assert_frame_equal(
            self.input_fact_df, mock_call_args["target_dfs"][0]
        )
        pl_testing.assert_frame_equal(input_dim_df, mock_call_args["target_dfs"][1])
        #   No rows should be passed in to the mock function as to be removed
        pl_testing.assert_frame_equal(
            expected_to_remove_df, mock_call_args["to_remove_df"]
        )

    def test_warns_when_not_all_rows_for_location_have_no_regulated_activities(
        self, mock_remove_rows
    ):
        # GIVEN
        #   Input where all rows have no regulated activities
        input_dim_df = pl.DataFrame(
            data=Data.remove_locations_without_ra_dim_some_dates_without_ra,
            schema=Schemas.remove_locations_without_ra_dim_schema,
        )

        # WHEN
        with self.assertWarns(UserWarning) as cm:
            job.remove_locations_without_regulated_activities(
                self.input_fact_df,
                input_dim_df,
            )

        # THEN
        expected_to_remove_df = pl.DataFrame(
            data=Data.expected_remove_locations_without_ra_dim_some_dates_without_ra_to_remove,
            schema=Schemas.expected_remove_locations_without_ra_to_remove_schema,
        )
        #   A single warning should have been raised
        self.assertIn(
            (
                "The following locations have some dates with imputed regulated activities, and others do not: ['loc_1']. "
                "Please check that the imputation has been carried out correctly."
            ),
            str(cm.warnings[0].message),
        )
        self.assertEqual(1, len(cm.warnings))
        #   The row removal function should have been called once
        mock_remove_rows.assert_called_once()
        mock_call_args = mock_remove_rows.call_args.kwargs
        #   The input fact and dimension df should be unchanged
        pl_testing.assert_frame_equal(
            self.input_fact_df, mock_call_args["target_dfs"][0]
        )
        pl_testing.assert_frame_equal(input_dim_df, mock_call_args["target_dfs"][1])
        #   All the rows should be passed in to the mock function as to be removed
        pl_testing.assert_frame_equal(
            expected_to_remove_df, mock_call_args["to_remove_df"]
        )


class SelectRegisteredLocationsTests(unittest.TestCase):
    def test_selects_all_registered(self):
        # GIVEN
        #   Input where all rows have registration status of 'Y'
        input_df = pl.DataFrame(
            data=Data.select_registered_locations_all_registered,
            schema=Schemas.select_registered_locations_schema,
        )

        # WHEN
        result_df = job.select_registered_locations(input_df)

        # THEN
        #   All rows should be selected (returned) unchanged
        pl_testing.assert_frame_equal(input_df, result_df)
        self.assertEqual(2, result_df.shape[0])

    def test_selects_no_deregistered(self):
        # GIVEN
        #   Input where all rows have registration status of 'N'
        input_df = pl.DataFrame(
            data=Data.select_registered_locations_none_registered,
            schema=Schemas.select_registered_locations_schema,
        )

        # WHEN
        result_df = job.select_registered_locations(input_df)

        # THEN
        #   No rows should be selected (returned)
        expected_df = pl.DataFrame(
            data=Data.expected_select_registered_locations_empty,
            schema=Schemas.select_registered_locations_schema,
        )
        pl_testing.assert_frame_equal(expected_df, result_df)
        self.assertEqual(0, result_df.shape[0])

    def test_selects_registered_when_status_is_mixed(self):
        # GIVEN
        #   Input where some rows have registration status of 'Y', and some have registration status of 'N'
        input_df = pl.DataFrame(
            data=Data.select_registered_locations_all_registered,
            schema=Schemas.select_registered_locations_schema,
        )

        # WHEN
        result_df = job.select_registered_locations(input_df)

        # THEN
        #   Only rows with a registration status of 'Y' should be returned
        expected_df = pl.DataFrame(
            data=Data.expected_select_registered_locations_registered,
            schema=Schemas.select_registered_locations_schema,
        )
        pl_testing.assert_frame_equal(expected_df, result_df)
        self.assertEqual(2, result_df.shape[0])

    def test_handles_empty_input_df(self):
        # GIVEN
        #   Input with no rows
        input_df = pl.DataFrame(
            data=Data.select_registered_locations_empty_input,
            schema=Schemas.select_registered_locations_schema,
        )

        # WHEN
        result_df = job.select_registered_locations(input_df)

        # THEN
        #   Should return an empty dataframe
        expected_df = pl.DataFrame(
            data=Data.expected_select_registered_locations_empty,
            schema=Schemas.select_registered_locations_schema,
        )
        pl_testing.assert_frame_equal(expected_df, result_df)
        self.assertEqual(0, result_df.shape[0])

    def test_warns_on_invalid_registration_status(self):
        # GIVEN
        #   Input where all rows have a registration status which is invalid
        input_df = pl.DataFrame(
            data=Data.select_registered_locations_invalid_status,
            schema=Schemas.select_registered_locations_schema,
        )

        # WHEN
        with self.assertWarns(UserWarning) as cm:
            result_df = job.select_registered_locations(input_df)

        # THEN
        #   A single warning should have been raised
        self.assertIn(
            "2 row(s) had an invalid registration status and have been dropped.",
            str(cm.warnings[0].message),
        )
        self.assertEqual(1, len(cm.warnings))

        #   And it should return an empty dataframe (with the invalid rows removed)
        expected_df = pl.DataFrame(
            data=Data.expected_select_registered_locations_empty,
            schema=Schemas.select_registered_locations_schema,
        )
        pl_testing.assert_frame_equal(expected_df, result_df)
        self.assertEqual(0, result_df.shape[0])


class AssignCqcSectorTests(TestCase):
    def test_assigns_local_authority(self):
        # GIVEN
        #   Input where rows have provider IDs in the list of LA provider ids
        input_df = pl.DataFrame(
            data=Data.assign_cqc_sector,
            schema=Schemas.assign_cqc_sector_input_schema,
        )
        local_authority_provider_ids = input_df["providerId"].to_list()

        # WHEN
        result_df = job.assign_cqc_sector(input_df, local_authority_provider_ids)

        # THEN
        #   Each row should be assigned the CQC sector of "Local authority"
        expected_df = pl.DataFrame(
            data=Data.expected_assign_cqc_sector_local_authority,
            schema=Schemas.expected_assign_cqc_sector_schema,
        )
        pl_testing.assert_frame_equal(expected_df, result_df)
        self.assertEqual(
            ["Local authority", "Local authority"], expected_df["cqc_sector"].to_list()
        )

    def test_assigns_independent(self):
        # GIVEN
        #   Input where rows have provider IDs in the list of LA provider ids
        input_df = pl.DataFrame(
            data=Data.assign_cqc_sector,
            schema=Schemas.assign_cqc_sector_input_schema,
        )
        local_authority_provider_ids = [
            s + "change" for s in input_df["providerId"].to_list()
        ]

        # WHEN
        result_df = job.assign_cqc_sector(input_df, local_authority_provider_ids)

        # THEN
        #   Each row should be assigned the CQC sector of "Local authority"
        expected_df = pl.DataFrame(
            data=Data.expected_assign_cqc_sector_independent,
            schema=Schemas.expected_assign_cqc_sector_schema,
        )
        pl_testing.assert_frame_equal(expected_df, result_df)
        self.assertEqual(
            ["Independent", "Independent"], expected_df["cqc_sector"].to_list()
        )


if __name__ == "__main__":
    unittest.main()
