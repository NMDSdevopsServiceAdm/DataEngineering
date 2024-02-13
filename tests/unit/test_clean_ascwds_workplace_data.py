import unittest
from unittest.mock import patch

import jobs.clean_ascwds_workplace_data as job

from tests.test_file_data import ASCWDSWorkplaceData as Data
from tests.test_file_schemas import ASCWDSWorkplaceSchemas as Schemas
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys,
    AscwdsWorkplaceColumns as AWP,
)
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
            Data.workplace_rows, Schemas.workplace_schema
        )
        self.cast_to_int_df = spark.createDataFrame(Data.cast_to_int_rows, Schemas.cast_to_int_schema)
        self.cast_to_int_with_errors_df = spark.createDataFrame(Data.cast_to_int_errors_rows, Schemas.cast_to_int_schema)
        self.cast_to_int_expected_df = spark.createDataFrame(Data.cast_to_int_expected_rows, Schemas.cast_to_int_schema)
        self.cast_to_int_with_errors_expected_df = spark.createDataFrame(Data.cast_to_int_errors_expected_rows, Schemas.cast_to_int_schema)
        self.filled_posts_columns = [AWP.total_staff, AWP.worker_records]

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
    
    def test_cast_to_int_returns_strings_formatted_as_ints_to_ints(self):
        returned_df = job.cast_to_int(self.cast_to_int_df, self.filled_posts_columns)
        returned_data = returned_df.sort(AWP.location_id).collect()
        expected_data = self.cast_to_int_expected_df.sort(AWP.location_id).collect()

        self.assertEqual(expected_data, returned_data)

    def test_cast_to_int_returns_strings_not_formatted_as_ints_as_none(self):
        returned_df = job.cast_to_int(self.cast_to_int_with_errors_df, self.filled_posts_columns)
        returned_data = returned_df.sort(AWP.location_id).collect()
        expected_data = self.cast_to_int_with_errors_expected_df.sort(AWP.location_id).collect()

        self.assertEqual(expected_data, returned_data)



if __name__ == "__main__":
    unittest.main(warnings="ignore")
