import unittest

from unittest.mock import Mock, patch

import projects._01_ingest.ons_pd.jobs.validate_postcode_directory_cleaned_data as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    ValidatePostcodeDirectoryCleanedData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    ValidatePostcodeDirectoryCleanedData as Schemas,
)
from utils import utils

PATCH_PATH = "projects._01_ingest.ons_pd.jobs.validate_postcode_directory_cleaned_data"


class ValidatePostcodeDirectoryCleanedDatasetTests(unittest.TestCase):
    TEST_RAW_POSTCODE_DIRECTORY_SOURCE = "some/directory"
    TEST_POSTCODE_DIRECTORY_CLEANED_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_raw_postcode_directory_df = self.spark.createDataFrame(
            Data.raw_postcode_directory_rows,
            Schemas.raw_postcode_directory_schema,
        )
        self.test_cleaned_postcode_directory_df = self.spark.createDataFrame(
            Data.cleaned_postcode_directory_rows,
            Schemas.cleaned_postcode_directory_schema,
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class MainTests(ValidatePostcodeDirectoryCleanedDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_raw_postcode_directory_df,
            self.test_cleaned_postcode_directory_df,
        ]

        with self.assertRaises(ValueError):
            job.main(
                self.TEST_RAW_POSTCODE_DIRECTORY_SOURCE,
                self.TEST_POSTCODE_DIRECTORY_CLEANED_SOURCE,
                self.TEST_DESTINATION,
            )

            self.assertEqual(read_from_parquet_patch.call_count, 2)
            self.assertEqual(write_to_parquet_patch.call_count, 1)


class CalculateExpectedSizeofDataset(ValidatePostcodeDirectoryCleanedDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_calculate_expected_size_of_cleaned_postcode_directory_dataset_returns_correct_row_count(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_expected_size_rows, Schemas.calculate_expected_size_schema
        )
        expected_row_count = 4
        returned_row_count = (
            job.calculate_expected_size_of_cleaned_postcode_directory_dataset(test_df)
        )
        self.assertEqual(returned_row_count, expected_row_count)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
