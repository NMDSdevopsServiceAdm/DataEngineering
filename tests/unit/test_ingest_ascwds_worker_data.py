import unittest
from unittest.mock import patch

import jobs.ingest_ascwds_worker_data as job

from tests.test_file_generator import generate_worker_parquet


class IngestASCWDSWorkerDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_ascwds_worker_df = generate_worker_parquet(None)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_csv")
    @patch("jobs.ingest_ascwds_worker_data.get_delimiter_for_csv")
    def test_main(
        self, get_delimiter_for_csv_mock, read_csv_mock, write_to_parquet_mock
    ):
        read_csv_mock.return_value = self.test_ascwds_worker_df
        get_delimiter_for_csv_mock.return_value = ","

        job.main("some source", "some destination")

        get_delimiter_for_csv_mock.assert_called_once_with("some source")
        read_csv_mock.assert_called_once_with("some source", ",")
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
