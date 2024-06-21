import unittest

from unittest.mock import Mock, patch

import jobs.validate_providers_api_cleaned_data as job

from tests.test_file_data import ValidateProvidersAPICleanedData as Data
from tests.test_file_schemas import ValidateProvidersAPICleanedData as Schemas

from utils import utils


class ValidateProvidersAPICleanedDatasetTests(unittest.TestCase):
    TEST_RAW_CQC_PROVIDER_SOURCE = "some/directory"
    TEST_CQC_PROVIDERS_API_CLEANED_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_raw_cqc_provider_df = self.spark.createDataFrame(
            Data.raw_cqc_providers_rows,
            Schemas.raw_cqc_providers_schema,
        )
        self.test_cleaned_cqc_providers_df = self.spark.createDataFrame(
            Data.cleaned_cqc_providers_rows, Schemas.cleaned_cqc_providers_schema
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class MainTests(ValidateProvidersAPICleanedDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_raw_cqc_provider_df,
            self.test_cleaned_cqc_providers_df,
        ]

        with self.assertRaises(ValueError):
            job.main(
                self.TEST_RAW_CQC_PROVIDER_SOURCE,
                self.TEST_CQC_PROVIDERS_API_CLEANED_SOURCE,
                self.TEST_DESTINATION,
            )

            self.assertEqual(read_from_parquet_patch.call_count, 2)
            self.assertEqual(write_to_parquet_patch.call_count, 1)


class CalculateExpectedSizeofDataset(ValidateProvidersAPICleanedDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_calculate_expected_size_of_cleaned_cqc_providers_dataset_returns_correct_row_count(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_expected_size_rows, Schemas.calculate_expected_size_schema
        )
        expected_row_count = 4
        returned_row_count = (
            job.calculate_expected_size_of_cleaned_cqc_providers_dataset(test_df)
        )
        self.assertEqual(returned_row_count, expected_row_count)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
