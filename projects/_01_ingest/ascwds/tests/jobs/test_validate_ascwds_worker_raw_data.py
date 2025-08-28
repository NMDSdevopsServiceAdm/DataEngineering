import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.ascwds.jobs.validate_ascwds_worker_raw_data as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    ValidateASCWDSWorkerRawData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    ValidateASCWDSWorkerRawData as Schemas,
)
from utils import utils


class ValidateASCWDSWorkerRawDatasetTests(unittest.TestCase):
    TEST_ASCWDS_WORKER_RAW_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_raw_ascwds_worker_df = self.spark.createDataFrame(
            Data.raw_ascwds_worker_rows, Schemas.raw_ascwds_worker_schema
        )

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class MainTests(ValidateASCWDSWorkerRawDatasetTests):
    def setUp(self) -> None:
        return super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_raw_ascwds_worker_df,
        ]

        job.main(
            self.TEST_ASCWDS_WORKER_RAW_SOURCE,
            self.TEST_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)
        self.assertEqual(write_to_parquet_patch.call_count, 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
