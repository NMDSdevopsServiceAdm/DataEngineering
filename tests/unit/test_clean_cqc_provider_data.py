import unittest
from unittest.mock import Mock, patch
import shutil
import warnings

import jobs.clean_cqc_provider_data as job

from pyspark.sql import SparkSession

from tests.test_file_generator import generate_cqc_providers_full_file


class CleanCQCProviderDatasetTests(unittest.TestCase):
    TEST_FULL_CQC_PROVIDERS_FILE = (
        "tests/test_data/domain=cqc/dataset=providers/format=parquet"
    )

    def setUp(self) -> None:
        self.test_cqc_providers_parquet = generate_cqc_providers_full_file(None)
        self.spark_mock = Mock()
        type(self.spark_mock).read = self.spark_mock
        self.spark_mock.option.return_value = self.spark_mock
        self.spark_mock.parquet.return_value = self.test_cqc_providers_parquet

    def test_get_cqc_provider_df(self):
        df = job.get_cqc_provider_df(self.TEST_FULL_CQC_PROVIDERS_FILE, self.spark_mock)

        self.spark_mock.read.option.assert_called_once_with(
            "basePath", self.TEST_FULL_CQC_PROVIDERS_FILE
        )
        self.spark_mock.parquet.assert_called_once_with(
            self.TEST_FULL_CQC_PROVIDERS_FILE
        )

        self.assertEqual(df.count(), self.test_cqc_providers_parquet.count())

        # is it retrieving the file
        # is the file empty or not?
        # Are particular columns present?
        # Does it return a dataframe?

    def test_clean_cqc_provider_df(self):
        self.assertTrue(False)

    def test_write_cleaned_provider_df_to_parquet(self):
        self.assertTrue(False)

    def test_main(self):
        self.assertTrue(False)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
