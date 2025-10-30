import json
import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._01_ingest.cqc_api.fargate.validate_cqc_locations_2_delta_flatten as job

PATCH_PATH = (
    "projects._01_ingest.cqc_api.fargate.validate_cqc_locations_2_delta_flatten"
)


class ValidateLocationsFlattenTests(unittest.TestCase):
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

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_invalid_dataset(self, mock_read_parquet: Mock, mock_write_reports: Mock):
        # Given
        mock_read_parquet.return_value = self.raw_df

        # When
        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        # Then
        mock_read_parquet.assert_has_calls(
            [
                call("s3://bucket/my/dataset/", exclude_complex_types=True),
                call("s3://bucket/other/dataset/", selected_columns=ANY),
            ]
        )
        mock_write_reports.assert_called_once()

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        self.assertDictContainsSubset(
            {
                "assertion_type": "row_count_match",
                "all_passed": True,
            },
            report_json[0],
        )

        self.assertDictContainsSubset(
            {
                "assertion_type": "col_vals_not_null",
                "column": "locationId",
                "all_passed": True,
            },
            report_json[1],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "col_vals_not_null",
                "column": "import_date",
                "all_passed": True,
            },
            report_json[2],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "rows_distinct",
                "column": ["locationId", "import_date"],
                "all_passed": False,
            },
            report_json[3],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "col_vals_between",
                "column": "locationId_length",
                "all_passed": True,
            },
            report_json[4],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
