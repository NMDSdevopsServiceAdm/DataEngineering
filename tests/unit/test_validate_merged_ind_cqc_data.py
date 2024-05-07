import unittest

from unittest.mock import Mock, patch

import jobs.validate_merged_ind_cqc_data as job

from tests.test_file_data import ValidateMergedIndCqcData as Data
from tests.test_file_schemas import ValidateMergedIndCqcData as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


class ValidateMergedIndCQCDatasetTests(unittest.TestCase):
    TEST_CQC_LOCATION_SOURCE = "some/directory"
    TEST_MERGED_IND_CQC_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.cqc_locations_rows,
            Schemas.cqc_locations_schema,
        )
        self.test_merged_ind_cqc_df = self.spark.createDataFrame(
            Data.merged_ind_cqc_rows, Schemas.merged_ind_cqc_schema
        )
        self.test_merged_ind_cqc_extra_row_df = self.spark.createDataFrame(
            Data.merged_ind_cqc_extra_row_rows, Schemas.merged_ind_cqc_schema
        )
        self.test_merged_ind_cqc_missing_row_df = self.spark.createDataFrame(
            Data.merged_ind_cqc_missing_row_rows, Schemas.merged_ind_cqc_schema
        )
        self.test_merged_ind_cqc_with_cqc_sector_null_df = self.spark.createDataFrame(
            Data.merged_ind_cqc_with_cqc_sector_null_rows, Schemas.merged_ind_cqc_schema
        )
        self.test_merged_ind_cqc_with_duplicate_data_df = self.spark.createDataFrame(
            Data.merged_ind_cqc_with_duplicate_data_rows, Schemas.merged_ind_cqc_schema
        )
        self.constraint_status = "constraint_status"
        self.constraint_message = "constraint_message"
        self.failure_value = "Failure"

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()

    @patch("jobs.validate_merged_ind_cqc_data.parse_data_quality_errors")
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
        parse_data_quality_errors_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_merged_ind_cqc_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_MERGED_IND_CQC_SOURCE,
            self.TEST_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 2)
        self.assertEqual(write_to_parquet_patch.call_count, 1)
        self.assertEqual(parse_data_quality_errors_patch.call_count, 1)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_returns_only_successes_when_given_valid_data(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_merged_ind_cqc_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_MERGED_IND_CQC_SOURCE,
            self.TEST_DESTINATION,
        )
        validation_results = write_to_parquet_patch.call_args[0][0]
        failure_count = validation_results.where(
            validation_results[self.constraint_status] == self.failure_value
        ).count()
        expected_failure_count = 0

        self.assertEqual(failure_count, expected_failure_count)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_returns_failure_when_given_dataframe_with_extra_row(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_merged_ind_cqc_extra_row_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_MERGED_IND_CQC_SOURCE,
            self.TEST_DESTINATION,
        )
        validation_results = write_to_parquet_patch.call_args[0][0]
        failure_count = validation_results.where(
            validation_results[self.constraint_status] == self.failure_value
        ).count()
        expected_failure_count = 1
        failure_message = (
            validation_results.where(
                validation_results[self.constraint_status] == self.failure_value
            )
            .select(self.constraint_message)
            .collect()[0][0]
        )
        expected_failure_message = "Value: 5 does not meet the constraint requirement! DataFrame row count should be 4."

        self.assertEqual(failure_count, expected_failure_count)
        self.assertEqual(failure_message, expected_failure_message)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_returns_failure_when_given_dataframe_with_missing_row(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_merged_ind_cqc_missing_row_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_MERGED_IND_CQC_SOURCE,
            self.TEST_DESTINATION,
        )
        validation_results = write_to_parquet_patch.call_args[0][0]
        failure_count = validation_results.where(
            validation_results[self.constraint_status] == self.failure_value
        ).count()
        expected_failure_count = 1
        failure_message = (
            validation_results.where(
                validation_results[self.constraint_status] == self.failure_value
            )
            .select(self.constraint_message)
            .collect()[0][0]
        )
        expected_failure_message = "Value: 3 does not meet the constraint requirement! DataFrame row count should be 4."

        self.assertEqual(failure_count, expected_failure_count)
        self.assertEqual(failure_message, expected_failure_message)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_returns_failure_when_given_null_care_home_value(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_merged_ind_cqc_with_cqc_sector_null_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_MERGED_IND_CQC_SOURCE,
            self.TEST_DESTINATION,
        )
        validation_results = write_to_parquet_patch.call_args[0][0]
        failure_count = validation_results.where(
            validation_results[self.constraint_status] == self.failure_value
        ).count()
        expected_failure_count = 1
        failure_message = (
            validation_results.where(
                validation_results[self.constraint_status] == self.failure_value
            )
            .select(self.constraint_message)
            .collect()[0][0]
        )
        expected_failure_message = "Value: 0.75 does not meet the constraint requirement! Completeness of cqc_sector should be 1."

        self.assertEqual(failure_count, expected_failure_count)
        self.assertEqual(failure_message, expected_failure_message)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_returns_failure_when_given_duplicate_data(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_merged_ind_cqc_with_duplicate_data_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_MERGED_IND_CQC_SOURCE,
            self.TEST_DESTINATION,
        )
        validation_results = write_to_parquet_patch.call_args[0][0]

        failure_count = validation_results.where(
            validation_results[self.constraint_status] == self.failure_value
        ).count()
        expected_failure_count = 1
        failure_message = (
            validation_results.where(
                validation_results[self.constraint_status] == self.failure_value
            )
            .select(self.constraint_message)
            .collect()[0][0]
        )
        expected_failure_message = "Value: 0.5 does not meet the constraint requirement! Uniqueness should be 1."

        self.assertEqual(failure_count, expected_failure_count)
        self.assertEqual(failure_message, expected_failure_message)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
