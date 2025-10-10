import unittest
import warnings
from dataclasses import asdict
from unittest.mock import ANY, Mock, patch

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import projects._01_ingest.cqc_api.jobs.delta_clean_cqc_location_data as job
import utils.cleaning_utils as cUtils
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    CQCLocationsData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    CQCLocationsSchema as Schemas,
)
from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLCleaned,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    DimensionPartitionKeys as DimensionKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import Sector

PATCH_PATH = "projects._01_ingest.cqc_api.jobs.delta_clean_cqc_location_data"


class CleanCQCLocationDatasetTests(unittest.TestCase):
    TEST_LOC_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    TEST_ONS_POSTCODE_DIRECTORY_SOURCE = "some/other/directory"
    TEST_GAC_SERVICE_DIMENSION_SOURCE = "dimension/some/other/directory"
    TEST_REGULATED_ACTIVITY_DIMENSION_SOURCE = "dimension/some/other/directory2"
    TEST_SPECIALISM_DIMENSION_SOURCE = "dimension/some/other/directory3"
    TEST_POSTCODE_DIMENSION_SOURCE = "dimension/some/other/directory4"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.sample_rows, schema=Schemas.detailed_schema
        )
        self.test_location_df = self.spark.createDataFrame(
            Data.small_location_rows, Schemas.small_location_schema
        )
        self.test_ons_postcode_directory_df = self.spark.createDataFrame(
            Data.ons_postcode_directory_rows, Schemas.ons_postcode_directory_schema
        )


class MainTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.create_postcode_matching_dimension")
    @patch(f"{PATCH_PATH}.add_cqc_sector_column_to_cqc_locations_dataframe")
    @patch(f"{PATCH_PATH}.add_related_location_column")
    @patch(f"{PATCH_PATH}.extract_registered_manager_names")
    @patch(f"{PATCH_PATH}.realign_carehome_column_with_primary_service")
    @patch(f"{PATCH_PATH}.allocate_primary_service_type")
    @patch(f"{PATCH_PATH}.remove_specialist_colleges")
    @patch(f"{PATCH_PATH}.classify_specialisms")
    @patch(f"{PATCH_PATH}.extract_from_struct")
    @patch(f"{PATCH_PATH}.remove_locations_that_never_had_regulated_activities")
    @patch(f"{PATCH_PATH}.impute_missing_struct_column")
    @patch(f"{PATCH_PATH}.create_dimension_from_missing_struct_column")
    @patch(f"{PATCH_PATH}.select_registered_locations_only")
    @patch(f"{PATCH_PATH}.impute_historic_relationships")
    @patch(f"{PATCH_PATH}.utils.format_date_fields", wraps=utils.format_date_fields)
    @patch(f"{PATCH_PATH}.remove_records_from_locations_data")
    @patch(f"{PATCH_PATH}.remove_non_social_care_locations")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_non_null_value")
    @patch(f"{PATCH_PATH}.clean_provider_id_column")
    @patch(f"{PATCH_PATH}.cUtils.column_to_date", wraps=cUtils.column_to_date)
    @patch(f"{PATCH_PATH}.create_cleaned_registration_date_column")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_mock: Mock,
        create_cleaned_registration_date_column_mock: Mock,
        column_to_date_mock: Mock,
        clean_provider_id_column_mock: Mock,
        select_rows_with_non_null_value_mock: Mock,
        remove_non_social_care_locations_mock: Mock,
        remove_records_from_locations_data_mock: Mock,
        format_date_fields_mock: Mock,
        impute_historic_relationships_mock: Mock,
        select_registered_locations_only_mock: Mock,
        create_dimension_from_missing_struct_column_mock: Mock,
        impute_missing_struct_column_mock: Mock,
        remove_locations_that_never_had_regulated_activities_mock: Mock,
        extract_from_struct_mock: Mock,
        classify_specialisms_mock: Mock,
        remove_specialist_colleges_mock: Mock,
        allocate_primary_service_type_mock: Mock,
        realign_carehome_column_with_primary_service_mock: Mock,
        extract_registered_manager_names_mock: Mock,
        add_related_location_column_mock: Mock,
        add_cqc_sector_column_to_cqc_locations_dataframe: Mock,
        create_postcode_matching_dim_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_ons_postcode_directory_df,
        ]
        remove_locations_that_never_had_regulated_activities_mock.return_value = (
            Mock(),
            Mock(),
        )
        remove_specialist_colleges_mock.return_value = (
            Mock(),
            Mock(),
        )

        job.main(
            self.TEST_LOC_SOURCE,
            self.TEST_ONS_POSTCODE_DIRECTORY_SOURCE,
            self.TEST_DESTINATION,
            self.TEST_GAC_SERVICE_DIMENSION_SOURCE,
            self.TEST_REGULATED_ACTIVITY_DIMENSION_SOURCE,
            self.TEST_SPECIALISM_DIMENSION_SOURCE,
            self.TEST_POSTCODE_DIMENSION_SOURCE,
        )

        self.assertEqual(read_from_parquet_mock.call_count, 2)
        create_cleaned_registration_date_column_mock.assert_called_once()
        self.assertEqual(column_to_date_mock.call_count, 2)
        clean_provider_id_column_mock.assert_called_once()
        select_rows_with_non_null_value_mock.assert_called_once()
        remove_non_social_care_locations_mock.assert_called_once()
        remove_records_from_locations_data_mock.assert_called_once()
        format_date_fields_mock.assert_called_once()
        impute_historic_relationships_mock.assert_called_once()
        select_registered_locations_only_mock.assert_called_once()
        self.assertEqual(create_dimension_from_missing_struct_column_mock.call_count, 3)
        impute_missing_struct_column_mock.assert_not_called()
        remove_locations_that_never_had_regulated_activities_mock.assert_called_once()
        self.assertEqual(extract_from_struct_mock.call_count, 2)
        self.assertEqual(classify_specialisms_mock.call_count, 3)
        remove_specialist_colleges_mock.assert_called_once()
        allocate_primary_service_type_mock.assert_called_once()
        realign_carehome_column_with_primary_service_mock.assert_called_once()
        extract_registered_manager_names_mock.assert_called_once()
        add_related_location_column_mock.assert_called_once()
        add_cqc_sector_column_to_cqc_locations_dataframe.assert_called_once()
        create_postcode_matching_dim_mock.assert_called_once()
        self.assertEqual(5, write_to_parquet_mock.call_count)
        write_to_parquet_mock.assert_called_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

        final_df: DataFrame = write_to_parquet_mock.call_args[0][0]
        expected_cols = list(asdict(CQCLCleaned()).values())
        for col in final_df.columns:
            self.assertIn(col, expected_cols)


class CreateDimensionTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.impute_missing_struct_column")
    def test_create_dimension_from_missing_struct_column_when_import_date_already_exists_in_dimension(
        self, mock_impute_missing_struct_column, mock_read_from_parquet
    ):
        # GIVEN
        #   Historic data:
        mock_read_from_parquet.return_value = self.spark.createDataFrame(
            Data.previous_gac_service_dimension_when_import_date_already_exists_in_dimension_rows,
            Schemas.gac_service_dimension_schema,
        )
        #   Current data has some updates to old rows and a new row
        mock_impute_missing_struct_column.return_value = self.spark.createDataFrame(
            Data.create_gac_service_dimension_when_import_date_already_exists_in_dimension_rows,
            Schemas.gac_service_dimension_input_schema,
        )
        max_import_date = "20240201"

        # WHEN
        #   the function is run
        returned_df = job.create_dimension_from_missing_struct_column(
            df=Mock(),
            missing_struct_column=CQCL.gac_service_types,
            dimension_location=self.TEST_GAC_SERVICE_DIMENSION_SOURCE,
            dimension_update_date=max_import_date,
        )
        returned_df.show()

        # THEN
        #   The updated rows and the new row should be returned
        expected_df = self.spark.createDataFrame(
            Data.expected_gac_service_delta_when_import_date_already_exists_in_dimension_rows,
            Schemas.gac_service_dimension_return_schema,
        )
        expected_df.show()
        mock_read_from_parquet.assert_called_once_with(
            self.TEST_GAC_SERVICE_DIMENSION_SOURCE
        )
        mock_impute_missing_struct_column.assert_called_once()
        self.assertEqual(len(returned_df.columns), len(expected_df.columns))
        column_order = [
            CQCL.location_id,
            CQCL.gac_service_types,
            CQCLCleaned.imputed_gac_service_types,
            Keys.import_date,
            DimensionKeys.last_updated,
        ]
        self.assertEqual(
            expected_df.select(column_order).collect(),
            returned_df.select(column_order).collect(),
        )

    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.impute_missing_struct_column")
    def test_create_dimension_from_missing_struct_column(
        self, mock_impute_missing_struct_column, mock_read_from_parquet
    ):
        # GIVEN
        #   Historic data:
        mock_read_from_parquet.return_value = self.spark.createDataFrame(
            Data.previous_gac_service_dimension_rows,
            Schemas.gac_service_dimension_schema,
        )
        #   Current data has some updates to old rows and a new row
        mock_impute_missing_struct_column.return_value = self.spark.createDataFrame(
            Data.create_gac_service_dimension_rows,
            Schemas.gac_service_dimension_input_schema,
        )

        # WHEN
        #   the function is run
        returned_df = job.create_dimension_from_missing_struct_column(
            df=Mock(),
            missing_struct_column=CQCL.gac_service_types,
            dimension_location=self.TEST_GAC_SERVICE_DIMENSION_SOURCE,
            dimension_update_date="20240201",
        )
        returned_df.show()
        # THEN
        #   The updated rows and the new row should be returned
        expected_df = self.spark.createDataFrame(
            Data.expected_gac_service_delta_rows,
            Schemas.gac_service_dimension_return_schema,
        )
        expected_df.show()
        mock_read_from_parquet.assert_called_once_with(
            self.TEST_GAC_SERVICE_DIMENSION_SOURCE
        )
        mock_impute_missing_struct_column.assert_called_once()
        self.assertEqual(len(returned_df.columns), len(expected_df.columns))
        column_order = [
            CQCL.location_id,
            CQCL.gac_service_types,
            CQCLCleaned.imputed_gac_service_types,
            Keys.import_date,
            DimensionKeys.last_updated,
        ]
        self.assertEqual(
            expected_df.select(column_order).collect(),
            returned_df.select(column_order).collect(),
        )

    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.impute_missing_struct_column")
    def test_create_dimension_when_no_historic_data(
        self, mock_impute_missing_struct_column, mock_read_from_parquet
    ):
        # GIVEN
        #   Trying to read historic data creates an analysis exception
        mock_read_from_parquet.side_effect = AnalysisException(
            "The file does not exist"
        )
        mock_impute_missing_struct_column.return_value = self.spark.createDataFrame(
            Data.create_gac_service_dimension_rows,
            Schemas.gac_service_dimension_input_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_gac_service_delta_when_no_history_rows,
            Schemas.gac_service_dimension_return_schema,
        )

        # WHEN
        #   the function is run
        returned_df = job.create_dimension_from_missing_struct_column(
            df=Mock(),
            missing_struct_column=CQCL.gac_service_types,
            dimension_location=self.TEST_GAC_SERVICE_DIMENSION_SOURCE,
            dimension_update_date="20240201",
        )
        returned_df.show()
        expected_df.show()
        # THEN
        #   The original data should be returned in full, with new columns for the update date
        mock_read_from_parquet.assert_called_once()
        mock_impute_missing_struct_column.assert_called_once()
        column_order = [
            CQCL.location_id,
            CQCL.gac_service_types,
            CQCLCleaned.imputed_gac_service_types,
            Keys.import_date,
            DimensionKeys.year,
            DimensionKeys.month,
            DimensionKeys.day,
            DimensionKeys.last_updated,
        ]
        self.assertEqual(
            returned_df.select(column_order).collect(),
            expected_df.select(column_order).collect(),
        )


class CreatePostcodeMatchingDimensionTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.run_postcode_matching")
    def test_postcode_matching_dimension(
        self, mock_run_postcode_matching, mock_read_from_parquet
    ):
        # GIVEN
        #   Historic data:
        mock_read_from_parquet.return_value = self.spark.createDataFrame(
            Data.postcode_matching_dimension_historic_rows,
            Schemas.postcode_matching_dimension_schema,
        )
        #   Current data:
        mock_run_postcode_matching.return_value = self.spark.createDataFrame(
            Data.postcode_matching_dimension_current_rows,
            Schemas.postcode_matching_dimension_input_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_postcode_matching_dimension_rows,
            Schemas.postcode_matching_dimension_schema,
        )

        # WHEN
        returned_df = job.create_postcode_matching_dimension(
            cqc_df=Mock(),
            postcode_df=Mock(),
            dimension_location=self.TEST_POSTCODE_DIMENSION_SOURCE,
            dimension_update_date="20240201",
        )

        # THEN
        mock_read_from_parquet.assert_called_once_with(
            self.TEST_POSTCODE_DIMENSION_SOURCE
        )
        mock_run_postcode_matching.assert_called_once()
        self.assertEqual(len(returned_df.columns), len(expected_df.columns))
        column_order = [
            CQCLCleaned.location_id,
            CQCLCleaned.cqc_location_import_date,
            CQCLCleaned.postal_address_line1,
            CQCLCleaned.postcode,
            CQCLCleaned.postcode_cleaned,
            DimensionKeys.year,
            DimensionKeys.month,
            DimensionKeys.day,
            DimensionKeys.import_date,
            DimensionKeys.last_updated,
        ]
        self.assertEqual(
            expected_df.select(column_order).collect(),
            returned_df.select(column_order).collect(),
        )


class CleanRegistrationDateTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_create_cleaned_registration_date_column(self):
        test_df = self.spark.createDataFrame(
            Data.clean_registration_date_column_rows,
            Schemas.clean_registration_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_registration_date_column_rows,
            Schemas.expected_clean_registration_column_schema,
        )
        returned_df = job.create_cleaned_registration_date_column(test_df)
        self.assertEqual(expected_df.collect(), returned_df.collect())

    def test_remove_time_from_date_column(self):
        test_df = self.spark.createDataFrame(
            Data.remove_time_from_date_column_rows,
            Schemas.expected_clean_registration_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_remove_time_from_date_column_rows,
            Schemas.expected_clean_registration_column_schema,
        )
        returned_df = job.remove_time_from_date_column(
            test_df, CQCLCleaned.imputed_registration_date
        )
        self.assertEqual(expected_df.collect(), returned_df.collect())

    def test_remove_registration_dates_that_are_later_than_import_date(self):
        test_df = self.spark.createDataFrame(
            Data.remove_late_registration_dates_rows,
            Schemas.remove_late_registration_dates_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_remove_late_registration_dates_rows,
            Schemas.remove_late_registration_dates_schema,
        )
        returned_df = job.remove_registration_dates_that_are_later_than_import_date(
            test_df
        )
        self.assertEqual(expected_df.collect(), returned_df.collect())

    def test_impute_missing_registration_dates_where_dates_are_the_same(self):
        test_df = self.spark.createDataFrame(
            Data.impute_missing_registration_dates_rows,
            Schemas.expected_clean_registration_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_missing_registration_dates_rows,
            Schemas.expected_clean_registration_column_schema,
        )
        returned_df = job.impute_missing_registration_dates(test_df)
        self.assertEqual(expected_df.collect(), returned_df.collect())

    def test_impute_missing_registration_dates_where_dates_are_different_return_min_registration_date(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.impute_missing_registration_dates_different_rows,
            Schemas.expected_clean_registration_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_missing_registration_dates_different_rows,
            Schemas.expected_clean_registration_column_schema,
        )
        returned_df = job.impute_missing_registration_dates(test_df)
        self.assertEqual(expected_df.collect(), returned_df.collect())

    def test_impute_missing_registration_dates_where_dates_are_all_missing_returns_min_import_date(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.impute_missing_registration_dates_missing_rows,
            Schemas.expected_clean_registration_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_missing_registration_dates_missing_rows,
            Schemas.expected_clean_registration_column_schema,
        )
        returned_df = job.impute_missing_registration_dates(test_df)
        self.assertEqual(expected_df.collect(), returned_df.collect())


class RemovedNonSocialCareLocationsTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_remove_non_social_care_locations_only_keeps_social_care_orgs(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.social_care_org_rows, Schemas.social_care_org_schema
        )

        returned_social_care_df = job.remove_non_social_care_locations(test_df)
        returned_social_care_data = returned_social_care_df.collect()

        expected_social_care_data = self.spark.createDataFrame(
            Data.expected_social_care_org_rows, Schemas.social_care_org_schema
        ).collect()

        self.assertEqual(returned_social_care_data, expected_social_care_data)


class ImputeHistoricRelationshipsTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_impute_historic_relationships_when_type_is_none_returns_none(self):
        test_df = self.spark.createDataFrame(
            Data.impute_historic_relationships_when_type_is_none_returns_none_rows,
            Schemas.impute_historic_relationships_schema,
        )
        returned_df = job.impute_historic_relationships(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_historic_relationships_when_type_is_none_returns_none_rows,
            Schemas.expected_impute_historic_relationships_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_impute_historic_relationships_when_type_is_predecessor_returns_predecessor(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.impute_historic_relationships_when_type_is_predecessor_returns_predecessor_rows,
            Schemas.impute_historic_relationships_schema,
        )
        returned_df = job.impute_historic_relationships(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_historic_relationships_when_type_is_predecessor_returns_predecessor_rows,
            Schemas.expected_impute_historic_relationships_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_impute_historic_relationships_when_type_is_successor_returns_none_when_registered(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.impute_historic_relationships_when_type_is_successor_returns_none_when_registered_rows,
            Schemas.impute_historic_relationships_schema,
        )
        returned_df = job.impute_historic_relationships(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_historic_relationships_when_type_is_successor_returns_none_when_registered_rows,
            Schemas.expected_impute_historic_relationships_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_impute_historic_relationships_when_type_is_successor_returns_successor_when_deregistered(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.impute_historic_relationships_when_type_is_successor_returns_successor_when_deregistered_rows,
            Schemas.impute_historic_relationships_schema,
        )
        returned_df = job.impute_historic_relationships(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_historic_relationships_when_type_is_successor_returns_successor_when_deregistered_rows,
            Schemas.expected_impute_historic_relationships_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_impute_historic_relationships_when_type_has_both_types_only_returns_predecessors_when_registered(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.impute_historic_relationships_when_type_has_both_types_only_returns_predecessors_when_registered_rows,
            Schemas.impute_historic_relationships_schema,
        )
        returned_df = job.impute_historic_relationships(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_historic_relationships_when_type_has_both_types_only_returns_predecessors_when_registered_rows,
            Schemas.expected_impute_historic_relationships_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_impute_historic_relationships_when_type_has_both_types_returns_original_values_when_deregistered(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.impute_historic_relationships_when_type_has_both_types_returns_original_values_when_deregistered_rows,
            Schemas.impute_historic_relationships_schema,
        )
        returned_df = job.impute_historic_relationships(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_historic_relationships_when_type_has_both_types_returns_original_values_when_deregistered_rows,
            Schemas.expected_impute_historic_relationships_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_impute_historic_relationships_where_different_relationships_over_time_returns_first_found(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.impute_historic_relationships_where_different_relationships_over_time_returns_first_found_rows,
            Schemas.impute_historic_relationships_schema,
        )
        returned_df = job.impute_historic_relationships(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_historic_relationships_where_different_relationships_over_time_returns_first_found_rows,
            Schemas.expected_impute_historic_relationships_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class GetRelationshipsWhereTypeIsPredecessorTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_get_relationships_where_type_is_none_returns_none(self):
        test_df = self.spark.createDataFrame(
            Data.get_relationships_where_type_is_none_returns_none_rows,
            Schemas.get_relationships_where_type_is_predecessor_schema,
        )
        returned_df = job.get_relationships_where_type_is_predecessor(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_get_relationships_where_type_is_none_returns_none_rows,
            Schemas.expected_get_relationships_where_type_is_predecessor_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_get_relationships_where_type_is_successor_returns_none(self):
        test_df = self.spark.createDataFrame(
            Data.get_relationships_where_type_is_successor_returns_none_rows,
            Schemas.get_relationships_where_type_is_predecessor_schema,
        )
        returned_df = job.get_relationships_where_type_is_predecessor(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_get_relationships_where_type_is_successor_returns_none_rows,
            Schemas.expected_get_relationships_where_type_is_predecessor_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_get_relationships_where_type_is_predecessor_returns_predecessor(self):
        test_df = self.spark.createDataFrame(
            Data.get_relationships_where_type_is_predecessor_returns_predecessor_rows,
            Schemas.get_relationships_where_type_is_predecessor_schema,
        )
        returned_df = job.get_relationships_where_type_is_predecessor(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_get_relationships_where_type_is_predecessor_returns_predecessor_rows,
            Schemas.expected_get_relationships_where_type_is_predecessor_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_get_relationships_where_type_has_both_types_only_returns_predecessor(self):
        test_df = self.spark.createDataFrame(
            Data.get_relationships_where_type_has_both_types_only_returns_predecessor_rows,
            Schemas.get_relationships_where_type_is_predecessor_schema,
        )
        returned_df = job.get_relationships_where_type_is_predecessor(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_get_relationships_where_type_has_both_types_only_returns_predecessor_rows,
            Schemas.expected_get_relationships_where_type_is_predecessor_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class ImputeMissingStructColumnTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_impute_missing_struct_column_df = self.spark.createDataFrame(
            Data.impute_missing_struct_column_rows,
            Schemas.impute_missing_struct_column_schema,
        )
        self.returned_df = job.impute_missing_struct_column(
            self.test_impute_missing_struct_column_df, CQCLCleaned.gac_service_types
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_impute_missing_struct_column_rows,
            Schemas.expected_impute_missing_struct_column_schema,
        )

        self.returned_data = self.returned_df.sort(
            CQCL.location_id, CQCLCleaned.cqc_location_import_date
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_impute_missing_struct_column_returns_expected_columns(self):
        self.assertTrue(self.returned_df.columns, self.expected_df.columns)

    def test_original_column_remains_unchanged(self):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][CQCL.gac_service_types],
                self.expected_data[i][CQCL.gac_service_types],
                f"Returned value in row {i} does not match original",
            )

    def test_impute_missing_struct_column_returns_expected_imputed_data(self):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][CQCLCleaned.imputed_gac_service_types],
                self.expected_data[i][CQCLCleaned.imputed_gac_service_types],
                f"Returned value in row {i} does not match expected",
            )


class RemoveLocationsThatNeverHadRegulatedActivitesTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_cqc_df = self.spark.createDataFrame(
            Data.remove_locations_that_never_had_regulated_activities_cqc_rows,
            Schemas.remove_locations_that_never_had_regulated_activities_cqc_schema,
        )
        self.test_dim_df = self.spark.createDataFrame(
            Data.remove_locations_that_never_had_regulated_activities_dim_rows,
            Schemas.remove_locations_that_never_had_regulated_activities_dim_schema,
        )
        self.returned_cqc_df, self.returned_dim_df = (
            job.remove_locations_that_never_had_regulated_activities(
                self.test_cqc_df, self.test_dim_df
            )
        )
        self.expected_cqc_df = self.spark.createDataFrame(
            Data.expected_remove_locations_that_never_had_regulated_activities_cqc_rows,
            Schemas.remove_locations_that_never_had_regulated_activities_cqc_schema,
        )
        self.expected_dim_df = self.spark.createDataFrame(
            Data.expected_remove_locations_that_never_had_regulated_activities_dim_rows,
            Schemas.remove_locations_that_never_had_regulated_activities_dim_schema,
        )

    def test_remove_locations_that_never_had_regulated_activities_returns_expected_data(
        self,
    ):
        self.assertEqual(self.expected_cqc_df.collect(), self.returned_cqc_df.collect())
        self.assertEqual(self.expected_dim_df.collect(), self.returned_dim_df.collect())


class ExtractFromStructTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()
        test_df = self.spark.createDataFrame(
            Data.extract_from_struct_rows,
            schema=Schemas.extract_from_struct_schema,
        )
        self.returned_df = job.extract_from_struct(
            test_df,
            test_df[CQCL.gac_service_types][CQCL.description],
            CQCLCleaned.services_offered,
        )

    def test_extract_from_struct_adds_column(self):
        self.assertTrue(CQCLCleaned.services_offered in self.returned_df.columns)

    def test_extract_from_struct_returns_correct_data(self):
        expected_df = self.spark.createDataFrame(
            Data.expected_extract_from_struct_rows,
            Schemas.expected_extract_from_struct_schema,
        )
        returned_data = self.returned_df.sort(CQCLCleaned.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class AllocatePrimaryServiceTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_allocate_primary_service_type(self):
        PRIMARY_SERVICE_TYPE_COLUMN_NAME = "primary_service_type"

        test_primary_service_df = self.spark.createDataFrame(
            Data.primary_service_type_rows,
            schema=Schemas.primary_service_type_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_type_rows,
            schema=Schemas.expected_primary_service_type_schema,
        )

        output_df = job.allocate_primary_service_type(test_primary_service_df)

        self.assertTrue(PRIMARY_SERVICE_TYPE_COLUMN_NAME in output_df.columns)

        primary_service_values = output_df.select(
            F.collect_list(PRIMARY_SERVICE_TYPE_COLUMN_NAME)
        ).first()[0]

        self.assertEqual(len(primary_service_values), 10)

        self.assertEqual(output_df.collect(), expected_df.collect())


class RealignCareHomeColumnWthPrimaryServiceTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()
        test_realign_carehome_column_df = self.spark.createDataFrame(
            Data.realign_carehome_column_rows,
            Schemas.realign_carehome_column_schema,
        )
        returned_df = job.realign_carehome_column_with_primary_service(
            test_realign_carehome_column_df
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_realign_carehome_column_rows,
            Schemas.realign_carehome_column_schema,
        )

        self.returned_data = returned_df.sort(CQCL.location_id).collect()
        self.expected_data = expected_df.collect()

    def test_care_home_values_match_expected_data(self):
        self.assertEqual(self.returned_data, self.expected_data)


class RemoveSpecialistCollegesTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()
        self.mock_cqc_df = Mock()
        self.mock_cqc_df.join.return_value = Mock()

    def test_remove_specialist_colleges_removes_rows_where_specialist_college_is_only_service(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_only_service_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        _, returned_df = job.remove_specialist_colleges(self.mock_cqc_df, test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_only_service_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_remove_specialist_colleges_does_not_remove_rows_where_specialist_college_is_one_of_multiple_services(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_multiple_services_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        _, returned_df = job.remove_specialist_colleges(self.mock_cqc_df, test_df)
        expected_df = self.spark.createDataFrame(
            Data.test_multiple_services_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_remove_specialist_colleges_does_not_remove_rows_where_specialist_college_is_not_listed_in_services(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_without_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        _, returned_df = job.remove_specialist_colleges(self.mock_cqc_df, test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_without_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_remove_specialist_colleges_does_not_remove_rows_where_gac_service_type_struct_is_empty(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_empty_array_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        _, returned_df = job.remove_specialist_colleges(self.mock_cqc_df, test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_empty_array_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_remove_specialist_colleges_does_not_remove_rows_where_gac_service_type_column_is_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_null_row_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        _, returned_df = job.remove_specialist_colleges(self.mock_cqc_df, test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_row_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class SelectRegisteredLocationsOnlyTest(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_select_registered_locations_only_splits_data_correctly(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.registration_status_rows, Schemas.registration_status_schema
        )

        returned_registered_df = job.select_registered_locations_only(test_df)
        returned_registered_data = returned_registered_df.collect()

        expected_registered_data = self.spark.createDataFrame(
            Data.expected_registered_rows, Schemas.registration_status_schema
        ).collect()

        self.assertEqual(returned_registered_data, expected_registered_data)

    def test_select_registered_locations_only_raises_a_warning_if_any_rows_are_invalid(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.registration_status_with_missing_data_rows,
            Schemas.registration_status_schema,
        )
        returned_registered_df = job.select_registered_locations_only(test_df)
        returned_registered_data = returned_registered_df.collect()

        expected_registered_data = self.spark.createDataFrame(
            Data.expected_registered_rows, Schemas.registration_status_schema
        ).collect()

        self.assertEqual(returned_registered_data, expected_registered_data)

        self.assertWarns(Warning)

    def test_select_registered_locations_only_does_not_raise_a_warning_if_all_rows_are_valid(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.registration_status_rows, Schemas.registration_status_schema
        )
        with warnings.catch_warnings(record=True) as warnings_log:
            returned_registered_df = job.select_registered_locations_only(test_df)

            self.assertEqual(warnings_log, [])


class CleanProviderIdColumn(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_clean_provider_id_column(self):
        test_df = self.spark.createDataFrame(
            Data.clean_provider_id_column_rows, Schemas.clean_provider_id_column_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_provider_id_column_rows,
            Schemas.clean_provider_id_column_schema,
        )
        returned_df = job.clean_provider_id_column(test_df)
        self.assertEqual(expected_df.collect(), returned_df.collect())

    def test_remove_values_with_too_many_characters(self):
        test_df = self.spark.createDataFrame(
            Data.long_provider_id_column_rows, Schemas.clean_provider_id_column_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_long_provider_id_column_rows,
            Schemas.clean_provider_id_column_schema,
        )
        returned_df = job.remove_provider_ids_with_too_many_characters(test_df)
        self.assertEqual(expected_df.collect(), returned_df.collect())

    def test_fill_missing_values_if_present_in_other_rows(self):
        test_df = self.spark.createDataFrame(
            Data.fill_missing_provider_id_column_rows,
            Schemas.clean_provider_id_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_fill_missing_provider_id_column_rows,
            Schemas.clean_provider_id_column_schema,
        )
        returned_df = job.fill_missing_provider_ids_from_other_rows(test_df)
        self.assertEqual(expected_df.collect(), returned_df.collect())


class AddColumnRelatedLocation(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_related_location_column_returns_correct_values(self):
        test_df = self.spark.createDataFrame(
            Data.add_related_location_column_rows,
            Schemas.add_related_location_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_add_related_location_column_rows,
            Schemas.expected_add_related_location_column_schema,
        )
        returned_df = job.add_related_location_column(test_df)
        self.assertEqual(
            expected_df.collect(), returned_df.sort(CQCL.location_id).collect()
        )


class CreateLaCqcProviderDataframeTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_create_dataframe_from_la_cqc_provider_list_creates_a_dataframe_with_a_column_of_providerids_and_a_column_of_strings(
        self,
    ):
        test_la_cqc_dataframe = job.create_dataframe_from_la_cqc_location_list(
            Data.sector_rows
        )
        self.assertEqual(
            test_la_cqc_dataframe.columns, [CQCL.provider_id, CQCLCleaned.cqc_sector]
        )

        test_cqc_sector_list = test_la_cqc_dataframe.select(
            F.collect_list(CQCLCleaned.cqc_sector)
        ).first()[0]
        self.assertEqual(
            test_cqc_sector_list,
            [
                Sector.local_authority,
                Sector.local_authority,
                Sector.local_authority,
                Sector.local_authority,
            ],
        )


class AddCqcSectorColumnTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        test_cqc_provider_df = self.spark.createDataFrame(
            Data.rows_without_cqc_sector,
            Schemas.rows_without_cqc_sector_schema,
        )
        self.test_cqc_provider_with_sector = (
            job.add_cqc_sector_column_to_cqc_locations_dataframe(
                test_cqc_provider_df, Data.sector_rows
            )
        )
        self.test_expected_dataframe = self.spark.createDataFrame(
            Data.expected_rows_with_cqc_sector,
            Schemas.expected_rows_with_cqc_sector_schema,
        )

    def test_add_cqc_sector_column_to_cqc_provider_dataframe_adds_cqc_sector_column(
        self,
    ):
        self.assertTrue(
            CQCLCleaned.cqc_sector in self.test_cqc_provider_with_sector.columns
        )

    def test_add_cqc_sector_column_to_cqc_provider_dataframe_returns_expected_df(
        self,
    ):
        expected_data = self.test_expected_dataframe.sort(CQCL.provider_id).collect()
        returned_data = self.test_cqc_provider_with_sector.sort(
            CQCL.provider_id
        ).collect()

        self.assertEqual(
            returned_data,
            expected_data,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
