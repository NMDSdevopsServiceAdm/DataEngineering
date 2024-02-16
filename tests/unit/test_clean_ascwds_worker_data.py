import unittest
from unittest.mock import ANY, Mock, patch

from pyspark.sql.dataframe import DataFrame

import jobs.clean_ascwds_worker_data as job

from tests.test_file_data import ASCWDSWorkerData, ASCWDSWorkplaceData
from tests.test_file_schemas import ASCWDSWorkerSchemas, ASCWDSWorkplaceSchemas
from utils.column_names.raw_data_files.ascwds_worker_columns import PartitionKeys
from utils.utils import get_spark


class IngestASCWDSWorkerDatasetTests(unittest.TestCase):
    TEST_WORKER_SOURCE = "s3://some_bucket/some_source_key"
    TEST_WORKPLACE_SOURCE = "s3://some_bucket/some_source_key"
    TEST_DESTINATION = "s3://some_bucket/some_destination_key"
    partition_keys = [
        PartitionKeys.year,
        PartitionKeys.month,
        PartitionKeys.day,
        PartitionKeys.import_date,
    ]

    def setUp(self) -> None:
        spark = get_spark()
        self.test_ascwds_worker_df = spark.createDataFrame(
            ASCWDSWorkerData.worker_rows, ASCWDSWorkerSchemas.worker_schema
        )
        self.test_ascwds_workplace_df = spark.createDataFrame(
            ASCWDSWorkplaceData.workplace_rows, ASCWDSWorkplaceSchemas.workplace_schema
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock):
        read_from_parquet_mock.return_value = self.test_ascwds_worker_df

        job.main(
            self.TEST_WORKER_SOURCE, self.TEST_WORKPLACE_SOURCE, self.TEST_DESTINATION
        )

        read_from_parquet_mock.assert_called_with(self.TEST_WORKER_SOURCE)
        read_from_parquet_mock.assert_called_with(self.TEST_WORKPLACE_SOURCE)
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            True,
            self.partition_keys,
        )

    def test_remove_invalid_worker_records_returns_df(self):
        returned_df = job.remove_invalid_worker_records(
            self.test_ascwds_worker_df, self.test_ascwds_workplace_df
        )

        self.assertIsInstance(returned_df, DataFrame)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
