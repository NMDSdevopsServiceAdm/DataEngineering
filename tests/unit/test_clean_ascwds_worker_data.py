import unittest
from unittest.mock import ANY, Mock, patch

from pyspark.sql.dataframe import DataFrame

import jobs.clean_ascwds_worker_data as job
from utils.utils import get_spark
from tests.test_file_data import ASCWDSWorkerData, ASCWDSWorkplaceData
from tests.test_file_schemas import ASCWDSWorkerSchemas, ASCWDSWorkplaceSchemas
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
    PartitionKeys,
)


class IngestASCWDSWorkerDatasetTests(unittest.TestCase):
    TEST_WORKER_SOURCE = "s3://some_bucket/some_worker_source_key"
    TEST_WORKPLACE_SOURCE = "s3://some_bucket/some_workplace_source_key"
    TEST_DESTINATION = "s3://some_bucket/some_destination_key"
    partition_keys = [
        PartitionKeys.year,
        PartitionKeys.month,
        PartitionKeys.day,
        PartitionKeys.import_date,
    ]

    def setUp(self) -> None:
        self.spark = get_spark()
        self.test_ascwds_worker_df = self.spark.createDataFrame(
            ASCWDSWorkerData.worker_rows, ASCWDSWorkerSchemas.worker_schema
        )
        self.test_ascwds_workplace_df = self.spark.createDataFrame(
            ASCWDSWorkplaceData.workplace_rows, ASCWDSWorkplaceSchemas.workplace_schema
        )


class MainTests(IngestASCWDSWorkerDatasetTests):
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock):
        read_from_parquet_mock.return_value = self.test_ascwds_worker_df

        job.main(
            self.TEST_WORKER_SOURCE, self.TEST_WORKPLACE_SOURCE, self.TEST_DESTINATION
        )

        read_from_parquet_mock.assert_any_call(
            self.TEST_WORKER_SOURCE, job.WORKER_COLUMNS
        )
        read_from_parquet_mock.assert_any_call(
            self.TEST_WORKPLACE_SOURCE, job.WORKPLACE_COLUMNS
        )
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


class RemoveWorkersWithoutWorkplacesTests(IngestASCWDSWorkerDatasetTests):
    def test_remove_invalid_worker_records_returns_df(self):
        returned_df = job.remove_workers_without_workplaces(
            self.test_ascwds_worker_df, self.test_ascwds_workplace_df
        )

        self.assertIsInstance(returned_df, DataFrame)

    def test_remove_invalid_worker_records_removed_expected_workers(self):
        returned_df = job.remove_workers_without_workplaces(
            self.test_ascwds_worker_df, self.test_ascwds_workplace_df
        )

        expected_df = self.spark.createDataFrame(
            ASCWDSWorkerData.expected_remove_workers_without_workplaces_rows,
            ASCWDSWorkerSchemas.worker_schema,
        )

        expected_rows = expected_df.orderBy(AWK.location_id).collect()
        returned_rows = (
            returned_df.select(*expected_df.columns).orderBy(AWK.location_id).collect()
        )

        self.assertCountEqual(returned_df.columns, self.test_ascwds_worker_df.columns)
        self.assertEqual(expected_rows, returned_rows)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
