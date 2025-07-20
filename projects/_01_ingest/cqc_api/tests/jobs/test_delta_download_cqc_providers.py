import unittest
from tempfile import TemporaryDirectory

import projects._01_ingest.cqc_api.jobs.delta_download_cqc_providers as job
from utils import utils


class MainTests(unittest.TestCase):

    def setUp(self) -> None:
        self.spark = utils.get_spark()


    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


    def test_main(self):
        with self.subTest("Known changes"):
            # Given
            known_changes_size = 3
            start_time = "2025-06-02T08:00:00Z"
            end_time = "2025-06-02T09:00:00Z"
            
            with TemporaryDirectory() as tempdir:
                # When
                job.main(f"{tempdir}/test.parquet", start_time, end_time)
                result = self.spark.read.parquet(f"{tempdir}/test.parquet")
                # Then
                self.assertEqual(result.count(), known_changes_size)

        with self.subTest("Invalid dates"):
            # Given
            start_time = "2025-06-01T16:00:00Z"
            end_time = "2025-06-01T00:00:00Z"

            with TemporaryDirectory() as tempdir:
                # Then
                with self.assertRaises(ValueError):
                    # When
                    job.main(f"{tempdir}/test.parquet", start_time, end_time)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
