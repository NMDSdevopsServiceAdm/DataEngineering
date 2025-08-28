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


class RemoveDuplicateWorkplacesTests(TestRawDataAdjustments):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.workplace_data_with_duplicates_rows, Schemas.workplace_data_schema
        )
        self.returned_df = job.remove_duplicate_workplaces_in_raw_workplace_data(
            self.test_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_workplace_data_with_duplicates_rows,
            Schemas.workplace_data_schema,
        )

    def test_remove_duplicate_workplaces_in_raw_workplace_data_returns_original_columns(
        self,
    ):
        self.assertEqual(sorted(self.returned_df.columns), sorted(self.test_df.columns))

    def test_remove_duplicate_workplaces_in_raw_workplace_data_removes_rows_identified_as_duplicates(
        self,
    ):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(expected_data, returned_data)


class RemoveRecordsFromLocationsDataTests(TestRawDataAdjustments):
    def setUp(self) -> None:
        super().setUp()
        self.test_with_multiple_rows_to_remove_df = self.spark.createDataFrame(
            Data.locations_data_with_multiple_rows_to_remove,
            Schemas.locations_data_schema,
        )
        self.test_with_single_rows_to_remove_df = self.spark.createDataFrame(
            Data.locations_data_with_single_rows_to_remove,
            Schemas.locations_data_schema,
        )
        self.test_without_rows_to_remove_df = self.spark.createDataFrame(
            Data.locations_data_without_rows_to_remove, Schemas.locations_data_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_locations_data, Schemas.locations_data_schema
        )

    def test_remove_records_from_locations_data_removes_multiple_rows_when_they_match_the_criteria(
        self,
    ):
        returned_df = job.remove_records_from_locations_data(
            self.test_with_multiple_rows_to_remove_df,
        )

        self.assertIsNotNone(returned_df)
        self.assertEqual(self.expected_df.collect(), returned_df.collect())

    def test_remove_records_from_locations_data_removes_single_rows_when_it_matches_the_criteria(
        self,
    ):
        returned_df = job.remove_records_from_locations_data(
            self.test_with_single_rows_to_remove_df,
        )

        self.assertIsNotNone(returned_df)
        self.assertEqual(self.expected_df.collect(), returned_df.collect())

    def test_remove_records_from_locations_data_does_not_remove_rows_when_they_do_not_match_the_criteria(
        self,
    ):
        returned_df = job.remove_records_from_locations_data(
            self.test_without_rows_to_remove_df,
        )

        self.assertIsNotNone(returned_df)
        self.assertEqual(self.expected_df.collect(), returned_df.collect())
