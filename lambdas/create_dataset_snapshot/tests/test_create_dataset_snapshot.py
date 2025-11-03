import unittest
from unittest.mock import MagicMock, patch

import polars as pl

import lambdas.create_dataset_snapshot.create_dataset_snapshot as job

PATCH_PATH = "lambdas.create_dataset_snapshot"


class TestCreateDatasetSnapshotLambda(unittest.TestCase):
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

    @unittest.skip("TO DO - ticket in trello")
    @patch(f"{PATCH_PATH}.utils.snapshots.build_snapshot_table_from_delta")
    @patch(f"{PATCH_PATH}.create_dataset_snapshot.S3FileSystem")
    @patch("polars.DataFrame.write_parquet")
    def test_create_dataset_snapshot_lambda(
        self,
        mock_write_parquet,
        mock_s3fs,
        mock_build_snapshot_table_from_delta,
    ):
        mock_build_snapshot_table_from_delta.return_value = self.snapshot_df

        mock_file_dest = MagicMock()
        mock_s3fs.return_value.open.return_value = mock_file_dest

        self.snapshot_df.write_parquet = mock_write_parquet

        job.lambda_handler(
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
