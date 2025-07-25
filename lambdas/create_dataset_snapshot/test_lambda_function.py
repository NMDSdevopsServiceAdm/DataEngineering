import unittest
from unittest.mock import patch, MagicMock, Mock
import sys

import polars as pl

mock_s3fs_module = MagicMock()
sys.modules["s3fs"] = mock_s3fs_module

mock_utils_module = MagicMock()
sys.modules["utils"] = mock_utils_module

from lambda_function import lambda_handler
import projects.tools.delta_data_remodel.jobs.utils as job

PATCH_PATH = "projects.tools.delta_data_remodel.jobs.utils"


class TestLambdaFunction(unittest.TestCase):
    def setUp(self):
        self.snapshot_df = pl.DataFrame(
            {
                "import_date": [
                    20130301,
                    20130301,
                    20130301,
                    20130301,
                    20130301,
                ],
                "providerId": ["a", "b", "c", "d", "e"],
                "value": ["same", "same", "same", "same", "same"],
                "deregistrationDate": ["", "", "", "", ""],
                "year": [2013, 2013, 2013, 2013, 2013],
                "month": [1, 2, 3, 4, 5],
                "day": [6, 7, 8, 9, 10],
            }
        )

    @patch("lambda_function.build_snapshot_table_from_delta")
    @patch("lambda_function.S3FileSystem")
    @patch("polars.DataFrame.write_parquet")
    def test_lambda_function(
        self,
        mock_write_parquet,
        mock_s3fs,
        mock_build_snapshot_table_from_delta,
    ):
        mock_build_snapshot_table_from_delta.return_value = self.snapshot_df

        mock_file_dest = MagicMock()
        mock_s3fs.return_value.open.return_value = mock_file_dest

        self.snapshot_df.write_parquet = mock_write_parquet

        lambda_handler(
            event={
                "input_uri": "s3://test-bucket/some/filepath",
                "output_uri": "output_uri/",
                "snapshot_date": "2025-07-23T17:59:33.737Z",
            },
            context={},
        )

        mock_build_snapshot_table_from_delta.assert_called_once_with(
            bucket="test-bucket", read_folder="some/filepath", timepoint=20250723
        )
        mock_s3fs.assert_called_once()
        mock_s3fs.return_value.open.assert_called_with(
            "output_uri/import_date=20250723/file.parquet", mode="wb"
        )
        mock_write_parquet.assert_called_once()


class TestUtilDependencies(unittest.TestCase):
    def setUp(self):
        self.base_snapshot = pl.DataFrame(
            {
                "import_date": [
                    20130301,
                    20130301,
                    20130301,
                    20130301,
                    20130301,
                ],
                "providerId": ["a", "b", "c", "d", "e"],
                "value": ["same", "same", "same", "same", "same"],
                "deregistrationDate": ["", "", "", "", ""],
            }
        )

    @patch(f"{PATCH_PATH}.snapshots")
    def test_build_snapshot_table_from_delta(self, mock_snapshots: Mock):
        # Given
        mock_snapshots.return_value = [
            self.base_snapshot,
        ]

        expected = self.base_snapshot

        # When
        result = job.build_snapshot_table_from_delta(
            "bucket", "read_folder", timepoint=20130301
        )

        # Then
        pl.testing.assert_frame_equal(result, expected)
        mock_snapshots.assert_called_once()
