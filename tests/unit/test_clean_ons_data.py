import unittest
from unittest.mock import ANY, patch
import pyspark.sql.functions as F

from utils import utils

import jobs.clean_ons_data as job

from tests.test_file_data import CQCProviderData as Data
from tests.test_file_schemas import CQCProviderSchema as Schema
from utils.column_names.raw_data_files.ons_columns import (
    ONSPartitionKeys as Keys,
)
from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)


class CleanCQCProviderDatasetTests(unittest.TestCase):
    TEST_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    onsPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_ons_parquet = self.spark.createDataFrame(
            Data.sample_rows_full, schema=Schema.full_schema
        )

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


if __name__ == "__main__":
    unittest.main(warnings="ignore")
