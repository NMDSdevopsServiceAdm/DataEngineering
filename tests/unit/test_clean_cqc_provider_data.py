import unittest
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

import jobs.clean_cqc_provider_data as job

import tests.test_helpers as helpers
from tests.test_file_schemas import CQCProviderSchemas as Schemas
from tests.test_file_data import CQCProviderData as Data


class CleanCQCProviderDatasetTests(unittest.TestCase):
    TEST_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = ["year", "month", "day", "import_date"]

    def setUp(self) -> None:
        self.spark = SparkSession.builder.appName(
            "test_clean_cqc_provider_data"
        ).getOrCreate()
        self.test_cqc_providers_parquet = self.spark.createDataFrame(
            Data.sample_rows_full, schema=Schemas.full_parquet_schema
        )
        self.spark_mock = helpers.create_spark_mock()
        self.spark_mock.parquet.return_value = self.test_cqc_providers_parquet

    def test_get_cqc_provider_df(self):
        df = job.get_cqc_provider_df(self.TEST_SOURCE, self.spark_mock)

        # Ensure specific functionality is called once
        self.spark_mock.read.option.assert_called_once_with(
            "basePath", self.TEST_SOURCE
        )
        self.spark_mock.parquet.assert_called_once_with(self.TEST_SOURCE)

        # Test data read doesn't lose any data
        self.assertEqual(df.count(), self.test_cqc_providers_parquet.count())

        # Schema structure preserved

    def test_clean_cqc_provider_df(self):
        # Test returned df is the same as the one passed in
        self.assertTrue(False)

    @patch("utils.utils.write_to_parquet")
    def test_write_cleaned_provider_df_to_parquet(self, write_to_parquet_mock):
        job.write_cleaned_provider_df_to_parquet(
            self.test_cqc_providers_parquet, self.TEST_DESTINATION
        )
        write_to_parquet_mock.assert_called_once_with(
            self.test_cqc_providers_parquet,
            self.TEST_DESTINATION,
            append=True,
            partitionKeys=self.partition_keys,
        )

    def test_main(self):
        self.assertTrue(False)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
