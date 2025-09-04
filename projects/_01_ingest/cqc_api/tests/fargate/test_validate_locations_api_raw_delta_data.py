import unittest
from unittest.mock import patch, ANY

import polars as pl

from projects._01_ingest.cqc_api.fargate.validate_locations_api_raw_delta_data import (
    main,
)

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.validate_locations_api_raw_delta_data"


class ValidateDatasetsTests(unittest.TestCase):

    def setUp(self) -> None:
        self.source_path = "some/directory"
        self.destination = "some/other/other/directory"
        self.raw_df = pl.DataFrame(
            [
                ("1-00001", "20240101", "a"),
                ("1-00002", "20240101", "b"),
                ("1-00001", "20240201", "b"),
                ("1-00002", "20240201", "c"),
                ("1-00002", "20240201", "d"),
            ],
            schema=pl.Schema(
                [
                    ("locationId", pl.String),
                    ("import_date", pl.String),
                    ("name", pl.String),
                ]
            ),
        )

    @patch("polars.scan_parquet", autospec=True)
    @patch("boto3.client", autospec=True)
    def test_distinct_rows_validation(self, mock_s3_client, mock_scan):
        # Given
        mock_scan.return_value.collect.return_value = self.raw_df
        # When
        with self.assertRaises(AssertionError) as context:
            main("bucket", "source", "destination")
        # Then
        self.assertIn(
            "Expect entirely distinct rows across `locationId`, `import_date`.", str(context.exception)
        )
        mock_scan.assert_called_once_with(
            "s3://bucket/source/",
            cast_options=ANY,
            extra_columns=ANY,
        )
        mock_s3_client.assert_called_once()


if __name__ == "__main__":
    unittest.main(warnings="ignore")
