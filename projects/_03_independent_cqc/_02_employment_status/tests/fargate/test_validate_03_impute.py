import json
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate.validate_03_impute as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns

PATCH_PATH = (
    "projects._03_independent_cqc._02_employment_status.fargate.validate_03_impute"
)


class TestMain:
    def setup_method(self):
        source_schema = {
            IndCqcColumns.location_id: pl.String,
            IndCqcColumns.published_job_role_label: pl.String,
            IndCqcColumns.cqc_location_import_date: pl.Date,
            **{col: pl.Float32 for col in job.IMPUTED_PERCENTAGE_COLUMNS},
            **{col: pl.Float32 for col in job.ROLLING_AVERAGE_PERCENTAGE_COLUMNS},
        }
        source_rows = [
            ("1-001", "Care worker", date(2024, 1, 1), *[0.2] * 5, *[0.2] * 5),
        ]
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")
        self.compare_df = self.source_df.select([IndCqcColumns.location_id])

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        assert mock_read_parquet.call_count == 2
        mock_read_parquet.assert_has_calls(
            [
                call(
                    source="s3://bucket/my/source/",
                    selected_columns=job.SOURCE_COLS_TO_IMPORT,
                ),
                call(
                    source="s3://bucket/my/compare/",
                    selected_columns=job.COMPARE_COLS_TO_IMPORT,
                ),
            ]
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        assert assertion_types_present == {
            "row_count_match",
            "col_vals_between",
            "col_vals_expr",
        }
