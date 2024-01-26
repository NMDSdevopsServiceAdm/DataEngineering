import unittest
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession
from utils import utils

import jobs.clean_cqc_provider_data as job

import tests.test_helpers as helpers
from schemas.cqc_provider_schema import PROVIDER_SCHEMA
from tests.test_file_data import CQCProviderData as Data


class CleanCQCProviderDatasetTests(unittest.TestCase):
    TEST_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = ["year", "month", "day", "import_date"]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_providers_parquet = self.spark.createDataFrame(
            Data.sample_rows_full, schema=PROVIDER_SCHEMA
        )

    def test_clean_cqc_provider_df(self):
        # Test returned df is the same as the one passed in
        returned_df = job.clean_cqc_provider_df(self.test_clean_cqc_provider_df)

        self.assertEqual(self.test_clean_cqc_provider_df, returned_df)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_patch, write_to_parquet_patch):
        read_from_parquet_patch.return_value = self.test_cqc_providers_parquet
        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)
        write_to_parquet_patch.assert_called_once_with(
            self.test_cqc_providers_parquet,
            self.TEST_DESTINATION,
            append=True,
            partitionKeys=self.partition_keys,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
