import unittest
import warnings
from unittest.mock import ANY, Mock, patch
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

import jobs.clean_cqc_location_data as job


from tests.test_file_data import CQCLocationsData as Data
from tests.test_file_schemas import CQCLocationsSchema as Schemas

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)


class CleanCQCLocationDatasetTests(unittest.TestCase):
    TEST_LOC_SOURCE = "some/directory"
    TEST_PROV_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/directory"
    TEST_ONS_POSTCODE_DIRECTORY_SOURCE = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.sample_rows_full, schema=Schemas.full_schema
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

    @patch("utils.cleaning_utils.column_to_date", wraps=cUtils.column_to_date)
    @patch("utils.utils.format_date_fields", wraps=utils.format_date_fields)
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
        format_date_fields_mock: Mock,
        column_to_date_mock: Mock,
    ):
        read_from_parquet_patch.side_effect = [
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

        self.assertEqual(read_from_parquet_patch.call_count, 3)
        format_date_fields_mock.assert_called_once()
        self.assertEqual(column_to_date_mock.call_count, 2)
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


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


class InvalidPostCodesTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_remove_invalid_postcodes(self):
        test_invalid_postcode_df = self.spark.createDataFrame(
            Data.test_invalid_postcode_data, Schemas.invalid_postcode_schema
        )

        df_with_invalid_postcodes_removed = job.remove_invalid_postcodes(
            test_invalid_postcode_df
        )

        expected_postcode_df = self.spark.createDataFrame(
            Data.expected_invalid_postcode_data, Schemas.invalid_postcode_schema
        )

        self.assertEqual(
            df_with_invalid_postcodes_removed.columns, expected_postcode_df.columns
        )

        self.assertEqual(
            df_with_invalid_postcodes_removed.sort(CQCL.location_id).collect(),
            expected_postcode_df.sort(CQCL.location_id).collect(),
        )


class AllocatePrimaryServiceTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_allocate_primary_service_type(self):
        PRIMARY_SERVICE_TYPE_COLUMN_NAME = "primary_service_type"

        test_primary_service_df = self.spark.createDataFrame(
            Data.primary_service_type_rows,
            schema=Schemas.primary_service_type_schema,
        )

        output_df = job.allocate_primary_service_type(test_primary_service_df)

        self.assertTrue(PRIMARY_SERVICE_TYPE_COLUMN_NAME in output_df.columns)

        primary_service_values = output_df.select(
            F.collect_list(PRIMARY_SERVICE_TYPE_COLUMN_NAME)
        ).first()[0]

        self.assertEqual(len(primary_service_values), 5)
        self.assertEqual(primary_service_values[0], job.NONE_RESIDENTIAL_IDENTIFIER)
        self.assertEqual(primary_service_values[1], job.NURSING_HOME_IDENTIFIER)
        self.assertEqual(primary_service_values[2], job.NONE_NURSING_HOME_IDENTIFIER)
        self.assertEqual(primary_service_values[3], job.NURSING_HOME_IDENTIFIER)
        self.assertEqual(primary_service_values[4], job.NONE_NURSING_HOME_IDENTIFIER)


class JoinCqcProviderDataTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_join_cqc_provider_data_adds_two_columns(self):
        returned_df = job.join_cqc_provider_data(
            self.test_location_df, self.test_provider_df
        )
        new_columns = 2
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


class SplitDataframeIntoRegAndDeRegTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_split_dataframe_into_registered_and_deregistered_rows_splits_data_correctly(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.registration_status_rows, Schemas.registration_status_schema
        )

        (
            returned_registered_df,
            returned_deregistered_df,
        ) = job.split_dataframe_into_registered_and_deregistered_rows(test_df)
        returned_registered_data = returned_registered_df.collect()
        returned_deregistered_data = returned_deregistered_df.collect()

        expected_registered_data = self.spark.createDataFrame(
            Data.expected_registered_rows, Schemas.registration_status_schema
        ).collect()
        expected_deregistered_data = self.spark.createDataFrame(
            Data.expected_deregistered_rows, Schemas.registration_status_schema
        ).collect()

        self.assertEqual(returned_registered_data, expected_registered_data)
        self.assertEqual(returned_deregistered_data, expected_deregistered_data)

    def test_split_dataframe_into_registered_and_deregistered_rows_raises_a_warning_if_any_rows_are_invalid(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.registration_status_with_missing_data_rows,
            Schemas.registration_status_schema,
        )
        (
            returned_registered_df,
            returned_deregistered_df,
        ) = job.split_dataframe_into_registered_and_deregistered_rows(test_df)
        returned_registered_data = returned_registered_df.collect()
        returned_deregistered_data = returned_deregistered_df.collect()

        expected_registered_data = self.spark.createDataFrame(
            Data.expected_registered_rows, Schemas.registration_status_schema
        ).collect()
        expected_deregistered_data = self.spark.createDataFrame(
            Data.expected_deregistered_rows, Schemas.registration_status_schema
        ).collect()

        self.assertEqual(returned_registered_data, expected_registered_data)
        self.assertEqual(returned_deregistered_data, expected_deregistered_data)

        self.assertWarns(Warning)

    def test_split_dataframe_into_registered_and_deregistered_rows_does_not_raise_a_warning_if_all_rows_are_valid(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.registration_status_rows, Schemas.registration_status_schema
        )
        with warnings.catch_warnings(record=True) as warnings_log:
            (
                returned_registered_df,
                returned_deregistered_df,
            ) = job.split_dataframe_into_registered_and_deregistered_rows(test_df)

            self.assertEqual(warnings_log, [])


class RemoveLocationsWithDuplicatesTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()
        self.returned_df = job.remove_locations_with_duplicates(self.test_location_df)

        self.test_duplicate_loc_df = self.spark.createDataFrame(
            Data.location_rows_with_duplicates, Schemas.small_location_schema
        )

    def test_returns_a_dataframe(self):
        self.assertEqual(type(self.returned_df), DataFrame)

    def test_returns_the_correct_columns(self):
        self.assertCountEqual(
            self.returned_df.columns, ["locationId", "providerId", "import_date"]
        )

    def test_does_not_remove_rows_if_no_duplicates(self):
        self.assertEqual(self.returned_df.count(), self.test_location_df.count())
        self.assertEqual(self.returned_df.collect(), self.test_location_df.collect())

    def test_removes_duplicate_location_id_with_same_import_date(self):
        filtered_df = job.remove_locations_with_duplicates(self.test_duplicate_loc_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_filtered_location_rows, Schemas.small_location_schema
        )
        self.assertEqual(filtered_df.collect(), expected_df.collect())

    def test_does_not_remove_duplicate_location_id_with_different_import_dates(self):
        locations_with_different_import_dates_df = self.spark.createDataFrame(
            Data.location_rows_with_different_import_dates,
            Schemas.small_location_schema,
        ).orderBy(CQCL.location_id)

        filtered_df = job.remove_locations_with_duplicates(
            locations_with_different_import_dates_df
        ).orderBy(CQCL.location_id)

        self.assertEqual(
            filtered_df.collect(), locations_with_different_import_dates_df.collect()
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
