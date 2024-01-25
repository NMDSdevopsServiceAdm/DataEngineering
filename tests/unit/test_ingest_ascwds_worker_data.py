import unittest
from unittest.mock import patch

import jobs.ingest_ascwds_worker_data as job

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
    @patch("utils.utils.read_csv")
    @patch("jobs.ingest_ascwds_worker_data.get_delimiter_for_csv")
    def test_main(
        self, get_delimiter_for_csv_mock, read_csv_mock, write_to_parquet_mock
    ):
        read_csv_mock.return_value = self.test_ascwds_worker_df
        get_delimiter_for_csv_mock.return_value = ","

        job.main(self.TEST_SOURCE, self.TEST_DESTINATION)

        get_delimiter_for_csv_mock.assert_called_once_with(
            "some_bucket", "some_source_key"
        )
        read_csv_mock.assert_called_once_with(self.TEST_SOURCE, ",")
        write_to_parquet_mock.assert_called_once_with(
            self.test_ascwds_worker_df, "s3://some_bucket/"
        )

    @patch("utils.utils.read_partial_csv_content")
    def test_get_delimiter_for_csv(self, read_partial_csv_content_mock):
        read_partial_csv_content_mock.return_value = (
            "locationid,workerid,mainjrid,import_date"
        )

        self.assertEqual(
            job.get_delimiter_for_csv(
                "sfc-data-engineering-raw", "/domain=ASCWDS/dataset=worker/"
            ),
            ",",
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
