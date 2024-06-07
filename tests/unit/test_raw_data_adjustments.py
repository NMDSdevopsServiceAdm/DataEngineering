import unittest

from utils import utils

import utils.raw_data_adjustments as job

from tests.test_file_schemas import RawDataAdjustments as Schemas
from tests.test_file_data import RawDataAdjustments as Data


class TestRawDataAdjustments(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()


class RemoveDuplicateWorkerTests(TestRawDataAdjustments):
    def setUp(self) -> None:
        super().setUp()
        self.test_with_multiple_rows_to_remove_df = self.spark.createDataFrame(
            Data.worker_data_with_multiple_rows_to_remove, Schemas.worker_data_schema
        )
        self.test_with_single_row_to_remove_df = self.spark.createDataFrame(
            Data.worker_data_with_single_row_to_remove, Schemas.worker_data_schema
        )
        self.test_without_rows_to_remove_df = self.spark.createDataFrame(
            Data.worker_data_without_rows_to_remove, Schemas.worker_data_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_worker_data, Schemas.worker_data_schema
        )

    def test_remove_duplicate_worker_in_worker_data_removes_multiple_rows_when_they_match_the_criteria(
        self,
    ):
        returned_df = job.remove_duplicate_worker_in_raw_worker_data(
            self.test_with_multiple_rows_to_remove_df,
        )

        self.assertIsNotNone(returned_df)
        self.assertEqual(self.expected_df.collect(), returned_df.collect())

    def test_remove_duplicate_worker_in_worker_data_removes_single_row_when_it_matches_the_criteria(
        self,
    ):
        returned_df = job.remove_duplicate_worker_in_raw_worker_data(
            self.test_with_single_row_to_remove_df,
        )

        self.assertIsNotNone(returned_df)
        self.assertEqual(self.expected_df.collect(), returned_df.collect())

    def test_remove_duplicate_worker_in_worker_data_does_not_remove_rows_when_they_do_not_match_the_criteria(
        self,
    ):
        returned_df = job.remove_duplicate_worker_in_raw_worker_data(
            self.test_without_rows_to_remove_df,
        )

        self.assertIsNotNone(returned_df)
        self.assertEqual(self.expected_df.collect(), returned_df.collect())
