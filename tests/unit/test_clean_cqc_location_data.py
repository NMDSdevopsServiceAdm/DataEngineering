import unittest
import warnings
from unittest.mock import ANY, Mock, patch
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from dataclasses import asdict

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
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLCleaned,
    CqcLocationCleanedValues as CQCLValues,
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

    @patch(
        "jobs.clean_cqc_location_data.raise_error_if_cqc_postcode_was_not_found_in_ons_dataset"
    )
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
        raise_error_if_cqc_postcode_was_not_found_in_ons_dataset: Mock,
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
        self.assertEqual(column_to_date_mock.call_count, 1)
        self.assertEqual(
            raise_error_if_cqc_postcode_was_not_found_in_ons_dataset.call_count, 1
        )

        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

        final_df: DataFrame = write_to_parquet_patch.call_args[0][0]
        expected_cols = list(asdict(CQCLCleaned()).values())
        for col in final_df.columns:
            self.assertIn(col, expected_cols)


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

    def test_amend_invalid_postcodes(self):
        test_invalid_postcode_df = self.spark.createDataFrame(
            Data.test_invalid_postcode_data, Schemas.invalid_postcode_schema
        )

        df_with_invalid_postcodes_removed = job.amend_invalid_postcodes(
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


class ListServicesOfferedTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_services_offered_df = self.spark.createDataFrame(
            Data.primary_service_type_rows,
            schema=Schemas.primary_service_type_schema,
        )

    def test_allocate_primary_service_type_add_column(self):
        returned_df = job.add_list_of_services_offered(self.test_services_offered_df)

        self.assertTrue(CQCLCleaned.services_offered in returned_df.columns)

    def test_allocate_primary_service_type_returns_correct_data(self):
        returned_df = job.add_list_of_services_offered(self.test_services_offered_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_services_offered_rows,
            Schemas.expected_services_offered_schema,
        )

        returned_data = (
            returned_df.select(sorted(returned_df.columns))
            .sort(CQCLCleaned.location_id)
            .collect()
        )
        expected_data = (
            expected_df.select(sorted(expected_df.columns))
            .sort(CQCLCleaned.location_id)
            .collect()
        )
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

        output_df = job.allocate_primary_service_type(test_primary_service_df)

        self.assertTrue(PRIMARY_SERVICE_TYPE_COLUMN_NAME in output_df.columns)

        primary_service_values = output_df.select(
            F.collect_list(PRIMARY_SERVICE_TYPE_COLUMN_NAME)
        ).first()[0]

        self.assertEqual(len(primary_service_values), 5)
        self.assertEqual(primary_service_values[0], CQCLValues.non_residential)
        self.assertEqual(primary_service_values[1], CQCLValues.care_home_with_nursing)
        self.assertEqual(primary_service_values[2], CQCLValues.care_home_only)
        self.assertEqual(primary_service_values[3], CQCLValues.care_home_with_nursing)
        self.assertEqual(primary_service_values[4], CQCLValues.care_home_only)


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


class JoinONSDataTests(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_location_for_ons_join_df = self.spark.createDataFrame(
            Data.locations_for_ons_join_rows,
            Schemas.locations_for_ons_join_schema,
        )

    def test_join_ons_postcode_data_completes(self):
        returned_df = job.join_ons_postcode_data_into_cqc_df(
            self.test_location_for_ons_join_df,
            self.test_ons_postcode_directory_df,
        )

        self.assertIsInstance(returned_df, DataFrame)

    def test_join_ons_postcode_data_correctly_joins_data(self):
        returned_df = job.join_ons_postcode_data_into_cqc_df(
            self.test_location_for_ons_join_df,
            self.test_ons_postcode_directory_df,
        )
        returned_data = (
            returned_df.select(sorted(returned_df.columns))
            .sort(CQCL.location_id)
            .collect()
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_ons_join_with_null_rows,
            Schemas.expected_ons_join_schema,
        )
        expected_data = (
            expected_df.select(sorted(expected_df.columns))
            .sort(CQCL.location_id)
            .collect()
        )

        self.assertCountEqual(returned_data, expected_data)


class RaiseErrorIfCQCPostcodeWasNotFoundInONSDataset(CleanCQCLocationDatasetTests):
    def setUp(self) -> None:
        super().setUp()
        self.expected_split_registered_df = self.spark.createDataFrame(
            Data.expected_split_registered_no_nulls_rows,
            Schemas.expected_split_registered_schema,
        )

    def test_raise_error_if_cqc_postcode_was_not_found_in_ons_dataset_returns_original_df(
        self,
    ):
        test_df = job.raise_error_if_cqc_postcode_was_not_found_in_ons_dataset(
            self.expected_split_registered_df
        )
        self.assertEqual(test_df, self.expected_split_registered_df)

    def test_raise_error_if_cqc_postcode_was_not_found_in_ons_dataset_exits_program_when_check_fails(
        self,
    ):
        expected_ons_join_df_with_nulls = self.spark.createDataFrame(
            Data.expected_ons_join_with_null_rows,
            Schemas.expected_ons_join_schema,
        )
        input_registered_df = job.split_dataframe_into_registered_and_deregistered_rows(
            expected_ons_join_df_with_nulls
        )[0]
        # At this point, PR19AB has a null curent_ons_import_date emulating a failed join.
        expected_tuple = ("PR19AB", "loc-1", "count: 1")
        with self.assertRaises(TypeError) as context:
            job.raise_error_if_cqc_postcode_was_not_found_in_ons_dataset(
                input_registered_df
            )

        self.assertTrue(
            f"Error: The following {CQCL.postcode}(s) and their corresponding {CQCL.location_id}(s) were not found in the ONS postcode data:"
            in str(context.exception),
            "Error text is missing correct description of Error",
        )
        self.assertTrue(
            f"{expected_tuple}" in str(context.exception),
            "Exception does not contain the postcode, locationId and number of rows",
        )

    def test_raise_error_if_cqc_postcode_was_not_found_in_ons_dataset_only_runs_when_provided_column_is_in_dataset(
        self,
    ):
        COLUMN_NOT_IN_DF = "not_a_column"
        with self.assertRaises(ValueError) as context:
            job.raise_error_if_cqc_postcode_was_not_found_in_ons_dataset(
                self.expected_split_registered_df, COLUMN_NOT_IN_DF
            )

        self.assertTrue(
            f"ERROR: A column or function parameter with name {COLUMN_NOT_IN_DF} cannot be found in the dataframe."
            in str(context.exception),
            "Exception does not contain the correct error message",
        )

    def test_raise_error_if_cqc_postcode_was_not_found_in_ons_dataset_only_runs_when_contains_appropriate_columns(
        self,
    ):
        no_postcode_df = self.expected_split_registered_df.drop(CQCL.postcode)
        no_location_df = self.expected_split_registered_df.drop(CQCL.location_id)
        no_current_date_df = self.expected_split_registered_df.drop(
            CQCLCleaned.current_ons_import_date
        )

        list_of_test_tuples = [
            (no_postcode_df, CQCL.postcode),
            (no_location_df, CQCL.location_id),
            (no_current_date_df, CQCLCleaned.current_ons_import_date),
        ]

        for test_data in list_of_test_tuples:
            with self.assertRaises(ValueError) as context:
                job.raise_error_if_cqc_postcode_was_not_found_in_ons_dataset(
                    test_data[0]
                )
            self.assertTrue(
                f"ERROR: A column or function parameter with name {test_data[1]} cannot be found in the dataframe."
                in str(context.exception),
                f"Error in {test_data} loop, exception does not contain the correct error message",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
