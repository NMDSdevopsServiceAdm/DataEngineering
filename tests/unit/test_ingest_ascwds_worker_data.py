import unittest
from unittest.mock import Mock, patch

import jobs.ingest_ascwds_worker_data as job

from tests.test_file_generator import generate_worker_parquet


class IngestASCWDSWorkerDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_ascwds_worker_df = generate_worker_parquet(None)

        self.spark_mock = Mock()
        self.spark_mock.read = self.spark_mock
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

    def test_write_cleaned_provider_df_to_parquet(self):
        mock_data_frame = Mock()
        mock_data_frame_writer = Mock()
        mock_data_frame.write = mock_data_frame_writer
        mock_data_frame_writer.partitionBy.return_value = mock_data_frame_writer

        job.write_cleaned_provider_df_to_parquet(mock_data_frame, "some destination")

        mock_data_frame_writer.partitionBy.assert_called_once_with()
        mock_data_frame_writer.parquet.assert_called_once_with("some destination")

    @patch("utils.utils.get_spark")
    def test_read_ascwds_worker_csv(self, get_spark_mock):
        get_spark_mock.return_value = self.spark_mock

        job.read_ascwds_worker_csv("some source", "some delimiter")

        self.spark_mock.option.assert_called_once_with("delimiter", "some delimiter")
        self.spark_mock.csv.assert_called_once_with("some source", header=True)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
