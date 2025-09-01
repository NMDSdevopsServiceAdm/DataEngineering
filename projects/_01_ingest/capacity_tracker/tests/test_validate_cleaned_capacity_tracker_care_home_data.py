import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.capacity_tracker.jobs.validate_cleaned_capacity_tracker_care_home_data as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    ValidateCleanedCapacityTrackerCareHomeData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    ValidateCleanedCapacityTrackerCareHomeData as Schemas,
)
from utils import utils

PATCH_PATH: str = "projects._01_ingest.capacity_tracker.jobs.validate_cleaned_capacity_tracker_care_home_data"


class ValidateCleanedCapacityTrackerCareHomeDatasetTests(unittest.TestCase):
    TEST_CT_CARE_HOME_SOURCE = "some/directory"
    TEST_CLEANED_CT_CARE_HOME_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_ct_care_home_df = self.spark.createDataFrame(
            Data.ct_care_home_rows,
            Schemas.ct_care_home_schema,
        )
        self.test_cleaned_ct_care_home_df = self.spark.createDataFrame(
            Data.cleaned_ct_care_home_rows, Schemas.cleaned_ct_care_home_schema
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()
        self.spark.stop()


class MainTests(ValidateCleanedCapacityTrackerCareHomeDatasetTests):
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
            self.test_ct_care_home_df,
            self.test_cleaned_ct_care_home_df,
        ]

        with self.assertRaises(ValueError):
            job.main(
                self.TEST_CT_CARE_HOME_SOURCE,
                self.TEST_CLEANED_CT_CARE_HOME_SOURCE,
                self.TEST_DESTINATION,
            )

            self.assertEqual(read_from_parquet_patch.call_count, 2)
            self.assertEqual(write_to_parquet_patch.call_count, 1)


class CalculateExpectedSizeofDataset(
    ValidateCleanedCapacityTrackerCareHomeDatasetTests
):
    def setUp(self) -> None:
        return super().setUp()

    def test_calculate_expected_size_of_cleaned_ct_care_home_dataset_returns_correct_row_count(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_expected_size_rows, Schemas.calculate_expected_size_schema
        )
        expected_row_count = 1
        returned_row_count = (
            job.calculate_expected_size_of_cleaned_capacity_tracker_care_home_dataset(
                test_df
            )
        )
        self.assertEqual(returned_row_count, expected_row_count)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
