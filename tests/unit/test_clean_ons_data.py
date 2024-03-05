import unittest
from unittest.mock import ANY, patch
from pyspark.sql.dataframe import DataFrame

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


class RefactorColumnsAsStructTests(CleanONSDatasetTests):
    def setUp(self):
        super().setUp()
        self.test_ons_postcode_directory_with_date_df = cUtils.column_to_date(
            self.test_ons_parquet,
            Keys.import_date,
            ONSClean.ons_import_date,
        ).drop(Keys.import_date)
        self.returned_df = job.refactor_columns_as_struct_with_alias(
            self.test_ons_postcode_directory_with_date_df, ONSClean.contemporary
        )

    def test_refactor_columns_as_struct_returns_df_with_correct_number_of_rows(self):
        self.assertIsInstance(self.returned_df, DataFrame)
        self.assertEqual(self.returned_df.count(), 5)

    def test_refactored_schema_matches_expected_schema(self):
        self.assertEqual(self.returned_df.schema, Schema.expected_refactored_schema)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
