import unittest

from unittest.mock import Mock, patch

import jobs.validate_merged_ind_cqc_data as job

from tests.test_file_data import ValidateMergedIndCqcData as Data
from tests.test_file_schemas import ValidateMergedIndCqcData as Schemas

from utils import utils


class ValidateMergedIndCQCDatasetTests(unittest.TestCase):
    TEST_CQC_LOCATION_SOURCE = "some/directory"
    TEST_MERGED_IND_CQC_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.cqc_locations_rows,
            Schemas.cqc_locations_schema,
        )
        self.test_merged_ind_cqc_df = self.spark.createDataFrame(
            Data.merged_ind_cqc_rows, Schemas.merged_ind_cqc_schema
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class MainTests(ValidateMergedIndCQCDatasetTests):
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


class CalculateExpectedSizeofDataset(ValidateMergedIndCQCDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_calculate_expected_size_of_merged_ind_cqc_dataset_returns_correct_row_count(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_expected_size_rows, Schemas.calculate_expected_size_schema
        )
        expected_row_count = 1
        returned_row_count = job.calculate_expected_size_of_merged_ind_cqc_dataset(
            test_df
        )
        self.assertEqual(returned_row_count, expected_row_count)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
