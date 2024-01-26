import unittest
from unittest.mock import patch

from utils import utils

import jobs.clean_cqc_provider_data as job

from schemas.cqc_provider_schema import PROVIDER_SCHEMA
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from tests.test_file_data import CQCProviderData as Data


class CleanCQCProviderDatasetTests(unittest.TestCase):
    TEST_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_providers_parquet = self.spark.createDataFrame(
            Data.sample_rows_full, schema=PROVIDER_SCHEMA
        )

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
