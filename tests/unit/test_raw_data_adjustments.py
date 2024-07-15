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


class RemoveDuplicatePIRTests(TestRawDataAdjustments):
    def setUp(self) -> None:
        super().setUp()
        self.test_with_multiple_rows_to_remove_df = self.spark.createDataFrame(
            Data.pir_data_with_multiple_rows_to_remove, Schemas.pir_data_schema
        )
        self.test_with_single_row_to_remove_df = self.spark.createDataFrame(
            Data.pir_data_with_single_row_to_remove, Schemas.pir_data_schema
        )
        self.test_without_rows_to_remove_df = self.spark.createDataFrame(
            Data.pir_data_without_rows_to_remove, Schemas.pir_data_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_pir_data, Schemas.pir_data_schema
        )

    def test_remove_duplicate_record_in_pir_data_removes_multiple_rows_when_they_match_the_criteria(
        self,
    ):
        returned_df = job.remove_duplicate_record_in_raw_pir_data(
            self.test_with_multiple_rows_to_remove_df,
        )

        self.assertIsNotNone(returned_df)
        self.assertEqual(self.expected_df.collect(), returned_df.collect())

    def test_remove_duplicate_record_in_pir_data_removes_single_row_when_it_matches_the_criteria(
        self,
    ):
        returned_df = job.remove_duplicate_record_in_raw_pir_data(
            self.test_with_single_row_to_remove_df,
        )

        self.assertIsNotNone(returned_df)
        self.assertEqual(self.expected_df.collect(), returned_df.collect())

    def test_remove_duplicate_record_in_pir_data_does_not_remove_rows_when_they_do_not_match_the_criteria(
        self,
    ):
        returned_df = job.remove_duplicate_record_in_raw_pir_data(
            self.test_without_rows_to_remove_df,
        )

        self.assertIsNotNone(returned_df)
        self.assertEqual(self.expected_df.collect(), returned_df.collect())


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


class ReplaceIncorrectPostcodeinLocationdDataTests(TestRawDataAdjustments):
    def setUp(self) -> None:
        super().setUp()
        self.test_one_location_df = self.spark.createDataFrame(
            Data.replace_incorrect_postcodes_one_location_rows,
            Schemas.replace_incorrect_postcodes_schema,
        )
        self.test_multiple_locations_df = self.spark.createDataFrame(
            Data.replace_incorrect_postcodes_multiple_locations_rows,
            Schemas.replace_incorrect_postcodes_schema,
        )
        self.returned_one_location_df = (
            job.replace_incorrect_postcode_in_locations_data(self.test_one_location_df)
        )
        self.returned_multiple_locations_df = (
            job.replace_incorrect_postcode_in_locations_data(
                self.test_multiple_locations_df
            )
        )
        self.expected_one_location_df = self.spark.createDataFrame(
            Data.expected_replace_incorrect_postcodes_one_location_rows,
            Schemas.replace_incorrect_postcodes_schema,
        )
        self.expected_multiple_locations_df = self.spark.createDataFrame(
            Data.expected_replace_incorrect_postcodes_multiple_locations_rows,
            Schemas.replace_incorrect_postcodes_schema,
        )

    def test_replace_incorrect_postcode_in_locations_data_changes_incorrect_postcode_for_relevant_location_id(
        self,
    ):
        self.assertEqual(
            self.returned_one_location_df.collect(),
            self.expected_one_location_df.collect(),
        )

    def test_replace_incorrect_postcode_in_locations_data_does_not_change_only_incorrect_postcodes_for_other_location_ids(
        self,
    ):
        self.assertEqual(
            self.returned_multiple_locations_df.collect(),
            self.expected_multiple_locations_df.collect(),
        )

    def test_replace_incorrect_postcode_in_locations_data_does_not_change_row_count(
        self,
    ):
        self.assertEqual(
            self.returned_one_location_df.count(), self.expected_one_location_df.count()
        )
        self.assertEqual(
            self.returned_multiple_locations_df.count(),
            self.expected_multiple_locations_df.count(),
        )

    def test_replace_incorrect_postcode_in_locations_data_does_not_change_columns(self):
        self.assertEqual(
            self.returned_one_location_df.columns, self.expected_one_location_df.columns
        )
        self.assertEqual(
            self.returned_multiple_locations_df.columns,
            self.expected_multiple_locations_df.columns,
        )
