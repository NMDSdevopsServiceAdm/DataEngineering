import unittest
import warnings
from dataclasses import asdict
from unittest.mock import ANY, Mock, patch

from pyspark.sql import DataFrame, functions as F

import projects._01_ingest.cqc_api.jobs.clean_cqc_location_data as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    CQCLocationsData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    CQCLocationsSchema as Schemas,
)
from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLCleaned,
)

PATCH_PATH = "projects._01_ingest.cqc_api.jobs.clean_cqc_location_data"


class CleanCQCLocationDatasetTests(unittest.TestCase):
    TEST_LOC_SOURCE = "some/directory"
    TEST_PROV_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/directory"
    TEST_ONS_POSTCODE_DIRECTORY_SOURCE = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.sample_rows, schema=Schemas.detailed_schema
        )
        self.test_location_df = self.spark.createDataFrame(
            Data.small_location_rows, Schemas.small_location_schema
        )
        self.test_provider_df = self.spark.createDataFrame(
            Data.join_provider_rows, Schemas.join_provider_schema
        )
        self.test_ons_postcode_directory_df = self.spark.createDataFrame(
            Data.ons_postcode_directory_rows, Schemas.ons_postcode_directory_schema
        )


class MainTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.run_postcode_matching")
    @patch(f"{PATCH_PATH}.impute_missing_data_from_provider_dataset")
    @patch(f"{PATCH_PATH}.join_cqc_provider_data")
    @patch(f"{PATCH_PATH}.add_related_location_column")
    @patch(
        f"{PATCH_PATH}.extract_registered_manager_names_from_imputed_regulated_activities_column"
    )
    @patch(f"{PATCH_PATH}.realign_carehome_column_with_primary_service")
    @patch(f"{PATCH_PATH}.allocate_primary_service_type")
    @patch(f"{PATCH_PATH}.remove_specialist_colleges")
    @patch(f"{PATCH_PATH}.classify_specialisms")
    @patch(f"{PATCH_PATH}.extract_from_struct")
    @patch(f"{PATCH_PATH}.remove_locations_that_never_had_regulated_activities")
    @patch(f"{PATCH_PATH}.impute_missing_struct_column")
    @patch(f"{PATCH_PATH}.select_registered_locations_only")
    @patch(f"{PATCH_PATH}.impute_historic_relationships")
    @patch(f"{PATCH_PATH}.calculate_time_registered_for")
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
        calculate_time_registered_for_mock: Mock,
        impute_historic_relationships_mock: Mock,
        select_registered_locations_only_mock: Mock,
        impute_missing_struct_column_mock: Mock,
        remove_locations_that_never_had_regulated_activities_mock: Mock,
        extract_from_struct_mock: Mock,
        classify_specialisms_mock: Mock,
        remove_specialist_colleges_mock: Mock,
        allocate_primary_service_type_mock: Mock,
        realign_carehome_column_with_primary_service_mock: Mock,
        extract_registered_manager_names_from_imputed_regulated_activities_column_mock: Mock,
        add_related_location_column_mock: Mock,
        join_cqc_provider_data_mock: Mock,
        impute_missing_data_from_provider_dataset_mock: Mock,
        run_postcode_matching_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_provider_df,
            self.test_ons_postcode_directory_df,
        ]

        job.main(
            self.TEST_LOC_SOURCE,
            self.TEST_PROV_SOURCE,
            self.TEST_ONS_POSTCODE_DIRECTORY_SOURCE,
            self.TEST_DESTINATION,
        )

        self.assertEqual(read_from_parquet_mock.call_count, 3)
        create_cleaned_registration_date_column_mock.assert_called_once()
        self.assertEqual(column_to_date_mock.call_count, 2)
        clean_provider_id_column_mock.assert_called_once()
        select_rows_with_non_null_value_mock.assert_called_once()
        remove_non_social_care_locations_mock.assert_called_once()
        remove_records_from_locations_data_mock.assert_called_once()
        format_date_fields_mock.assert_called_once()
        calculate_time_registered_for_mock.assert_called_once()
        impute_historic_relationships_mock.assert_called_once()
        select_registered_locations_only_mock.assert_called_once()
        self.assertEqual(impute_missing_struct_column_mock.call_count, 3)
        remove_locations_that_never_had_regulated_activities_mock.assert_called_once()
        self.assertEqual(extract_from_struct_mock.call_count, 2)
        self.assertEqual(classify_specialisms_mock.call_count, 3)
        remove_specialist_colleges_mock.assert_called_once()
        allocate_primary_service_type_mock.assert_called_once()
        realign_carehome_column_with_primary_service_mock.assert_called_once()
        extract_registered_manager_names_from_imputed_regulated_activities_column_mock.assert_called_once()
        add_related_location_column_mock.assert_called_once()
        join_cqc_provider_data_mock.assert_called_once()
        self.assertEqual(impute_missing_data_from_provider_dataset_mock.call_count, 2)
        run_postcode_matching_mock.assert_called_once()
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

        final_df: DataFrame = write_to_parquet_mock.call_args[0][0]
        expected_cols = list(asdict(CQCLCleaned()).values())
        for col in final_df.columns:
            self.assertIn(col, expected_cols)


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


class CalculateTimeRegisteredForTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_time_registered_returns_one_when_dates_are_on_the_same_day(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_same_day_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_same_day_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_exact_months_apart(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_exact_months_apart_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_exact_months_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.sort(CQCLCleaned.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_one_day_less_than_a_full_month_apart(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_one_day_less_than_a_full_month_apart_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_one_day_less_than_a_full_month_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.sort(CQCLCleaned.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_one_day_more_than_a_full_month_apart(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_one_day_more_than_a_full_month_apart_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_one_day_more_than_a_full_month_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.sort(CQCLCleaned.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)


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
        self.test_df = self.spark.createDataFrame(
            Data.remove_locations_that_never_had_regulated_activities_rows,
            Schemas.remove_locations_that_never_had_regulated_activities_schema,
        )
        self.returned_df = job.remove_locations_that_never_had_regulated_activities(
            self.test_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_remove_locations_that_never_had_regulated_activities_rows,
            Schemas.remove_locations_that_never_had_regulated_activities_schema,
        )

        self.returned_data = self.returned_df.sort(CQCL.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_remove_locations_that_never_had_regulated_activities_returns_expected_data(
        self,
    ):
        self.assertEqual(self.returned_data, self.expected_data)


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

    def test_remove_specialist_colleges_removes_rows_where_specialist_college_is_only_service(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_only_service_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        returned_df = job.remove_specialist_colleges(test_df)
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
        returned_df = job.remove_specialist_colleges(test_df)
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
        returned_df = job.remove_specialist_colleges(test_df)
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
        returned_df = job.remove_specialist_colleges(test_df)
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
        returned_df = job.remove_specialist_colleges(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_row_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class JoinCqcProviderDataTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_location_df = cUtils.column_to_date(
            self.test_location_df,
            Keys.import_date,
            CQCLCleaned.cqc_location_import_date,
        ).drop(CQCLCleaned.import_date)

    def test_join_cqc_provider_data_adds_three_columns(self):
        returned_df = job.join_cqc_provider_data(
            self.test_location_df, self.test_provider_df
        )
        new_columns = 3
        expected_columns = len(self.test_location_df.columns) + new_columns

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_join_cqc_provider_data_correctly_joins_data(self):
        returned_df = job.join_cqc_provider_data(
            self.test_location_df, self.test_provider_df
        )
        returned_data = (
            returned_df.select(sorted(returned_df.columns))
            .sort(CQCL.location_id)
            .collect()
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_joined_rows, Schemas.expected_joined_schema
        )
        expected_data = (
            expected_df.select(sorted(expected_df.columns))
            .sort(CQCL.location_id)
            .collect()
        )

        self.assertCountEqual(returned_data, expected_data)


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


class ImputeMissingDataFromProviderDataset(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()
        self.column_to_impute = CQCLCleaned.cqc_sector

    def test_impute_missing_data_from_provider_dataset_returns_correct_values_when_column_has_the_same_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.impute_missing_data_from_provider_dataset_single_value_rows,
            Schemas.impute_missing_data_from_provider_dataset_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_missing_data_from_provider_dataset_rows,
            Schemas.impute_missing_data_from_provider_dataset_schema,
        )
        returned_df = job.impute_missing_data_from_provider_dataset(
            test_df, self.column_to_impute
        )
        self.assertEqual(expected_df.collect(), returned_df.collect())

    def test_impute_missing_data_from_provider_dataset_returns_correct_values_when_column_values_change_over_time(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.impute_missing_data_from_provider_dataset_multiple_values_rows,
            Schemas.impute_missing_data_from_provider_dataset_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_impute_missing_data_from_provider_dataset_multiple_values_rows,
            Schemas.impute_missing_data_from_provider_dataset_schema,
        ).sort(CQCLCleaned.cqc_location_import_date)
        returned_df = job.impute_missing_data_from_provider_dataset(
            test_df, self.column_to_impute
        ).sort(CQCLCleaned.cqc_location_import_date)
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


class CalculateTimeSinceDormant(CleanCQCLocationDatasetTests):
    def setUp(self):
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.calculate_time_since_dormant_rows,
            Schemas.calculate_time_since_dormant_schema,
        )
        self.returned_df = job.calculate_time_since_dormant(self.test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_since_dormant_rows,
            Schemas.expected_calculate_time_since_dormant_schema,
        )

        self.columns_added_by_function = [
            column
            for column in self.returned_df.columns
            if column not in self.test_df.columns
        ]

    def test_calculate_time_since_dormant_returns_new_column(self):
        self.assertEqual(len(self.columns_added_by_function), 1)
        self.assertEqual(
            self.columns_added_by_function[0], CQCLCleaned.time_since_dormant
        )

    def test_calculate_time_since_dormant_returns_expected_values(self):
        returned_data = self.returned_df.sort(
            CQCLCleaned.cqc_location_import_date
        ).collect()
        expected_data = self.expected_df.collect()

        self.assertEqual(returned_data, expected_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
