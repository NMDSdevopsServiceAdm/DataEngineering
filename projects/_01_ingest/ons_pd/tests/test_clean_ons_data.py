import unittest
from unittest.mock import ANY, Mock, patch
from pyspark.sql.dataframe import DataFrame
from datetime import date

from utils import utils
import utils.cleaning_utils as cUtils

import projects._01_ingest.ons_pd.jobs.clean_ons_data as job
from projects._01_ingest.unittest_data.data import CleanONSData as Data
from projects._01_ingest.unittest_data.schemas import CleanONSData as Schema
from utils.column_names.raw_data_files.ons_columns import ONSPartitionKeys as Keys
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)

PATCH_PATH = "projects._01_ingest.ons_pd.jobs.clean_ons_data"


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

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main(self, read_from_parquet_patch: Mock, write_to_parquet_patch: Mock):
        read_from_parquet_patch.return_value = self.test_ons_parquet
        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.onsPartitionKeys,
        )


class PrepareContemporaryOnsDataTests(CleanONSDatasetTests):
    def setUp(self):
        super().setUp()
        self.returned_df = job.prepare_contemporary_ons_data(
            self.test_ons_postcode_directory_with_date_df
        )

    def test_prepare_contemporary_ons_data_returns_df_with_correct_number_of_rows(
        self,
    ):
        self.assertIsInstance(self.returned_df, DataFrame)
        self.assertEqual(self.returned_df.count(), 5)

    def test_prepare_contemporary_ons_data_returns_expected_columns(self):
        returned_schema = sorted(self.returned_df.columns)
        expected_df = self.spark.createDataFrame(
            [], Schema.expected_refactored_contemporary_schema
        )
        expected_schema = sorted(expected_df.columns)
        self.assertEqual(returned_schema, expected_schema)


class PrepareCurrentOnsDataTests(CleanONSDatasetTests):
    def setUp(self):
        super().setUp()
        self.returned_df = job.prepare_current_ons_data(
            self.test_ons_postcode_directory_with_date_df
        )

    def test_only_most_recent_rows_for_max_date_are_kept(self):
        returned_data = self.returned_df.collect()
        self.assertEqual(
            returned_data[0][ONSClean.current_ons_import_date], date(2023, 1, 1)
        )
        self.assertEqual(
            returned_data[1][ONSClean.current_ons_import_date], date(2023, 1, 1)
        )
        self.assertEqual(
            returned_data[2][ONSClean.current_ons_import_date], date(2023, 1, 1)
        )

    def test_prepare_current_ons_data_returns_df_with_correct_number_of_rows(
        self,
    ):
        self.assertIsInstance(self.returned_df, DataFrame)
        self.assertEqual(self.returned_df.count(), 3)

    def test_prepare_current_ons_data_returns_expected_columns(self):
        returned_schema = sorted(self.returned_df.columns)
        expected_df = self.spark.createDataFrame(
            [], Schema.expected_refactored_current_schema
        )
        expected_schema = sorted(expected_df.columns)
        self.assertEqual(returned_schema, expected_schema)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
