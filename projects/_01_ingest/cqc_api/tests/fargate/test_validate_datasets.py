import unittest
from unittest.mock import ANY, patch

import polars as pl
import yaml

from projects._01_ingest.cqc_api.fargate.validate_datasets import (
    main,
)


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
        self.yaml = yaml.safe_load(
            """
            tbl: null
            tbl_name: "delta_locations_api"
            label: "Basic data quality checks"
            brief: 
            thresholds:
                warning: 1
            actions:
                warning: "{LEVEL}: {type} validation failed with {n_failed} records for column {col}."
            steps:
            - rows_distinct:
                columns_subset: [locationId, import_date]
                brief: "Ensure all {col} values are distinct"
            - col_vals_not_null:
                columns: [locationId, import_date, name]
                brief: "Ensure {col} columns are fully populated"
        """
        )

    @patch("boto3.client", autospec=True)
    @patch("polars.scan_parquet", autospec=True)
    @patch("pointblank.yaml.YAMLValidator.load_config", autospec=True)
    def test_distinct_rows_validation(self, mock_yaml, mock_scan, mock_s3_client):
        # Given
        mock_scan.return_value.collect.return_value = self.raw_df
        mock_yaml.return_value = self.yaml

        # When
        with self.assertRaises(AssertionError) as context:
            main("bucket", "domain", "dataset", "version", "report")

        # Then
        self.assertIn(
            "Expect entirely distinct rows across `locationId`, `import_date`.",
            str(context.exception),
        )
        mock_scan.assert_called_once_with(
            "s3://bucket/domain=domain/dataset=dataset/version=version/",
            cast_options=ANY,
            extra_columns=ANY,
        )
        mock_s3_client.assert_called_once()


if __name__ == "__main__":
    unittest.main(warnings="ignore")
