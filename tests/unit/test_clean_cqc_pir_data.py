import unittest
from unittest.mock import ANY, patch

from utils import utils

import jobs.clean_cqc_pir_data as job

from schemas.cqc_pir_schema import PIR_SCHEMA
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from tests.test_file_data import CQCpirData as Data


class CleanCQCpirDatasetTests(unittest.TestCase):
    TEST_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_pir_parquet = self.spark.createDataFrame(
            Data.sample_rows_full, schema=PIR_SCHEMA
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_patch, write_to_parquet_patch):
        read_from_parquet_patch.return_value = self.test_cqc_pir_parquet
        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            append=True,
            partitionKeys=self.partition_keys,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
