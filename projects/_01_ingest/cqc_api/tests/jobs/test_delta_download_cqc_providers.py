import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

import projects._01_ingest.cqc_api.jobs.delta_download_cqc_providers as job

from utils import utils

PATCH_PATH = "projects._01_ingest.cqc_api.jobs.delta_download_cqc_providers"


class MainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


    @patch(f"{PATCH_PATH}.ars.get_secret")
    @patch(f"{PATCH_PATH}.cqc.get_updated_objects")
    def test_main_returns_expected_row_count(self, mock_get_updated_objects, mock_get_secret):
        # Given
        mock_get_secret.return_value = '{"Ocp-Apim-Subscription-Key": "test"}'
        mock_get_updated_objects.return_value = [
            {"providerId": 1},
            {"providerId": 2},
            {"providerId": 3}
        ]
        start_time = "2025-06-02T08:00:00Z"
        end_time = "2025-06-02T09:00:00Z"

        # Results only needed temporarily to verify count
        with TemporaryDirectory() as tempdir:
            # When
            job.main(f"{tempdir}/test.parquet", start_time, end_time)
            result = self.spark.read.parquet(f"{tempdir}/test.parquet")
            # Then
            self.assertEqual(result.count(), 3)

    def test_main_raises_error_when_end_time_before_start_time(self):
        # Given
        start_time = "2025-06-01T16:00:00.123Z"
        end_time = "2025-06-01T00:00:00.123Z"

        # TempDir not strictly needed but prevents accidental write if error isn't thrown
        with TemporaryDirectory() as tempdir:
            # Then
            with self.assertRaises(ValueError):
                # When
                job.main(f"{tempdir}/test.parquet", start_time, end_time)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
