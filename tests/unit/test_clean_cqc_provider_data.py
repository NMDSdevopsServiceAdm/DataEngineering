import unittest
from unittest.mock import patch

from utils import utils

import jobs.clean_cqc_provider_data as job

from schemas.cqc_provider_schema import PROVIDER_SCHEMA
from tests.test_file_schemas import CleanCqcProviderData as Schema
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
        self.test_sector_df = self.spark.createDataFrame(
            Data.sector_rows, schema=Schema.sector_schema
        )

    def test_add_sector_column_adds_column_to_dataframe(self):
        # dataframe that the function returns.
        returned_df = job.add_sector_column(self.test_sector_df)

        # dataframe that shows the expected output.
        col_count_expected = len(self.test_sector_df.columns) + 1

        # assert that one column has been addded to output.
        self.assertEqual(len(returned_df.columns), col_count_expected)

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
