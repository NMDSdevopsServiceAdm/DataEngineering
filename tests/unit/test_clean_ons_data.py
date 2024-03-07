import unittest
from unittest.mock import ANY, patch
from pyspark.sql.dataframe import DataFrame
from datetime import date

from utils import utils
import utils.cleaning_utils as cUtils

import jobs.clean_ons_data as job

from tests.test_file_data import ONSData as Data
from tests.test_file_schemas import ONSData as Schema
from utils.column_names.raw_data_files.ons_columns import (
    ONSPartitionKeys as Keys,
)
from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)


class CleanONSDatasetTests(unittest.TestCase):
    TEST_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    onsPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_ons_parquet = self.spark.createDataFrame(
            Data.ons_sample_rows_full, schema=Schema.full_schema
        )
        self.test_ons_postcode_directory_with_date_df = cUtils.column_to_date(
            self.test_ons_parquet,
            Keys.import_date,
            ONSClean.contemporary_ons_import_date,
        )


class MainTests(CleanONSDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_patch, write_to_parquet_patch):
        read_from_parquet_patch.return_value = self.test_ons_parquet
        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.onsPartitionKeys,
        )


class RefactorColumnsWithPrefixTests(CleanONSDatasetTests):
    def setUp(self):
        super().setUp()
        self.returned_df = job.refactor_columns_with_prefix(
            self.test_ons_postcode_directory_with_date_df, job.CONTEMPORARY_PREFIX
        )

    def test_refactor_columns_as_with_prefix_returns_df_with_correct_number_of_rows(self):
        self.assertIsInstance(self.returned_df, DataFrame)
        self.assertEqual(self.returned_df.count(), 5)

    def test_refactored_schema_matches_expected_columns(self):
        returned_schema = sorted(self.returned_df.columns)
        expected_df = self.spark.createDataFrame([], Schema.expected_refactored_contemporary_schema)
        expected_schema = sorted(expected_df.columns)
        self.assertEqual(returned_schema, expected_schema)


class PrepareCurrentONSTests(CleanONSDatasetTests):
    def setUp(self):
        super().setUp()

    def test_only_most_recent_rows_for_max_date_are_kept(self):
        returned_df = job.prepare_current_ons_data(
            self.test_ons_postcode_directory_with_date_df
        )
        self.assertEqual(returned_df.count(), 3)

        returned_data = returned_df.collect()
        self.assertEqual(
            returned_data[0][ONSClean.current_ons_import_date], date(2023, 1, 1)
        )
        self.assertEqual(
            returned_data[1][ONSClean.current_ons_import_date], date(2023, 1, 1)
        )
        self.assertEqual(
            returned_data[2][ONSClean.current_ons_import_date], date(2023, 1, 1)
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
