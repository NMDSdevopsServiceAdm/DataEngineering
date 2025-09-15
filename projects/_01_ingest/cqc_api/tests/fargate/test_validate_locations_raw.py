import json
import unittest
from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.cqc_api.fargate.validate_locations_raw as job

UTILS_PATH = "polars_utils.validate"


class ValidateLocationsRawTests(unittest.TestCase):
    def setUp(self) -> None:
        self.raw_df = pl.DataFrame(
            [
                ("1-00001", "20240101", "a"),
                ("1-00002", "20240101", "b"),
                ("1-00001", "20240201", "b"),
                ("1-00002", "20240201", "c"),
                ("1-00002", "20240201", None),
            ],
            schema=pl.Schema(
                [
                    ("locationId", pl.String),
                    ("import_date", pl.String),
                    ("name", pl.String),
                ]
            ),
            orient="row",
        )

    @patch(f"{UTILS_PATH}.write_reports")
    @patch(f"{UTILS_PATH}.read_parquet")
    def test_invalid_dataset(self, mock_read_parquet: Mock, mock_write_reports: Mock):
        # Given
        mock_read_parquet.return_value = self.raw_df

        # When
        job.main("bucket", "/my/dataset/", "my/reports/")

        # Then
        mock_read_parquet.assert_called_once_with("s3://bucket/my/dataset/")
        mock_write_reports.assert_called_once()

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        self.assertDictContainsSubset(
            {
                "i": 1,
                "assertion_type": "col_vals_not_null",
                "column": "locationId",
                "all_passed": True,
            },
            report_json[0],
        )
        self.assertDictContainsSubset(
            {
                "i": 2,
                "assertion_type": "col_vals_not_null",
                "column": "import_date",
                "all_passed": True,
            },
            report_json[1],
        )
        self.assertDictContainsSubset(
            {
                "i": 3,
                "assertion_type": "col_vals_not_null",
                "column": "name",
                "all_passed": False,
            },
            report_json[2],
        )
        self.assertDictContainsSubset(
            {
                "i": 4,
                "assertion_type": "rows_distinct",
                "column": ["locationId", "import_date"],
                "all_passed": False,
            },
            report_json[3],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
