import unittest
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession

import jobs.ingest_ascwds_worker_data as job


class IngestASCWDSWorkerDatasetTests(unittest.TestCase):
    TEST_ASCWDS_WORKER_FILE = "tests/test_data/domain=ascwds/dataset=worker"

    def setUp(self) -> None:
        self.spark = SparkSession.builder.appName(
            "sfc_data_engineering_test_ingest_ascwds_dataset"
        ).getOrCreate()

        columns = ["locationid", "workerid", "mainjrid", "import_date"]

        rows = [
            ("1-000000001", "100", 1, "20220101"),
            ("1-000000001", "101", 1, "20220101"),
        ]

        self.test_ascwds_worker_df = self.spark.createDataFrame(rows, columns)

        self.spark_mock = Mock()
        type(self.spark_mock).read = self.spark_mock
        self.spark_mock.option.return_value = self.spark_mock

    @patch("jobs.ingest_ascwds_worker_data.get_delimiter_for_csv")
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.get_spark")
    def test_main(
        self, get_spark_mock, write_to_parquet_mock, get_delimiter_for_csv_mock
    ):
        get_spark_mock.return_value = self.spark_mock
        self.spark_mock.csv.return_value = self.test_ascwds_worker_df
        get_delimiter_for_csv_mock.return_value = ","

        job.main("some source", "some destination")

        get_delimiter_for_csv_mock.assert_called_once_with("some source")
        write_to_parquet_mock.assert_called_once_with(
            self.test_ascwds_worker_df, "some destination"
        )

    @patch("utils.utils.read_partial_csv_content")
    def test_get_delimiter_for_csv(self, read_partial_csv_content_mock):
        read_partial_csv_content_mock.return_value = (
            "locationid,workerid,mainjrid,import_date"
        )

        self.assertEqual(
            job.get_delimiter_for_csv(
                "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=worker/"
            ),
            ",",
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
