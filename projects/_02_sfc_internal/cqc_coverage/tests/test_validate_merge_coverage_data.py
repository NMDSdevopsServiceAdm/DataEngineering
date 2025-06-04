import unittest
from unittest.mock import Mock, patch

import projects._02_sfc_internal.cqc_coverage.jobs.validate_merge_coverage_data as job
from projects._02_sfc_internal.unittest_data.sfc_test_file_data import (
    ValidateMergedCoverageData as Data,
)
from projects._02_sfc_internal.unittest_data.sfc_test_file_schemas import (
    ValidateMergedCoverageData as Schemas,
)

from utils import utils


PATCH_PATH = "projects._02_sfc_internal.cqc_coverage.jobs.validate_merge_coverage_data"


class ValidateMergedCoverageDatasetTests(unittest.TestCase):
    TEST_CQC_LOCATION_SOURCE = "some/directory"
    TEST_MERGED_COVERAGE_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.cqc_locations_rows,
            Schemas.cqc_locations_schema,
        )
        self.test_merged_coverage_df = self.spark.createDataFrame(
            Data.merged_coverage_rows, Schemas.merged_coverage_schema
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class MainTests(ValidateMergedCoverageDatasetTests):
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
            self.test_clean_cqc_location_df,
            self.test_merged_coverage_df,
        ]

        with self.assertRaises(ValueError):
            job.main(
                self.TEST_CQC_LOCATION_SOURCE,
                self.TEST_MERGED_COVERAGE_SOURCE,
                self.TEST_DESTINATION,
            )

            self.assertEqual(read_from_parquet_patch.call_count, 2)
            self.assertEqual(write_to_parquet_patch.call_count, 1)


class CalculateExpectedSizeofDataset(ValidateMergedCoverageDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_calculate_expected_size_of_merged_coverage_dataset_returns_correct_row_count(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_expected_size_rows, Schemas.calculate_expected_size_schema
        )
        expected_row_count = 1
        returned_row_count = job.calculate_expected_size_of_merged_coverage_dataset(
            test_df
        )
        self.assertEqual(returned_row_count, expected_row_count)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
