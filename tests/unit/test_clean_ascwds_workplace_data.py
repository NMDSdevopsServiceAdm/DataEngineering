import unittest
from unittest.mock import patch

import jobs.clean_ascwds_workplace_data as job

from tests.test_file_data import ASCWDSWorkplaceData
from tests.test_file_schemas import ASCWDSWorkplaceSchemas
from utils.column_names.raw_data_files.ascwds_workplace_columns import PartitionKeys
from utils.utils import get_spark


class IngestASCWDSWorkerDatasetTests(unittest.TestCase):
    TEST_SOURCE = "s3://some_bucket/some_source_key"
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
            ASCWDSWorkplaceData.workplace_rows, ASCWDSWorkplaceSchemas.workplace_schema
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_mock, write_to_parquet_mock):
        read_from_parquet_mock.return_value = self.test_ascwds_worker_df

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        read_from_parquet_mock.assert_called_once_with(self.TEST_SOURCE)
        write_to_parquet_mock.assert_called_once_with(
            self.test_ascwds_worker_df,
            self.TEST_DESTINATION,
            True,
            self.partition_keys,
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
