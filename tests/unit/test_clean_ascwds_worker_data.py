import unittest
from unittest.mock import patch

import jobs.clean_ascwds_worker_data as job

from tests.test_file_data import ASCWDSWorkerData
from tests.test_file_schemas import ASCWDSWorkerSchemas
from utils.utils import get_spark


class IngestASCWDSWorkerDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.TEST_SOURCE = "s3://some_bucket/some_source_key"
        self.TEST_DESTINATION = "s3://some_bucket/some_destination_key"

        spark = get_spark()
        self.test_ascwds_worker_df = spark.createDataFrame(
            ASCWDSWorkerData.worker_rows, ASCWDSWorkerSchemas.worker_schema
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_mock, write_to_parquet_mock):
        read_from_parquet_mock.return_value = self.test_ascwds_worker_df

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        read_from_parquet_mock.assert_called_once_with(self.TEST_SOURCE)
        write_to_parquet_mock.assert_called_once_with(
            self.test_ascwds_worker_df, "s3://some_bucket/"
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
