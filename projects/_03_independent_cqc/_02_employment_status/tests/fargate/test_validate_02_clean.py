import json
from unittest.mock import Mock, call, patch

import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate.validate_02_clean as job
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_column_values import EmploymentStatusFilteringRule

PATCH_PATH = (
    "projects._03_independent_cqc._02_employment_status.fargate.validate_02_clean"
)


class TestMain:
    def setup_method(self):
        source_schema = {
            IndCqcColumns.location_id: pl.String,
            EmpStatus.permanent_count_dedup: pl.Int64,
            EmpStatus.temporary_count_dedup: pl.Int64,
            EmpStatus.bank_or_pool_count_dedup: pl.Int64,
            EmpStatus.agency_count_dedup: pl.Int64,
            EmpStatus.other_count_dedup: pl.Int64,
            EmpStatus.permanent_percentage: pl.Float32,
            EmpStatus.temporary_percentage: pl.Float32,
            EmpStatus.bank_or_pool_percentage: pl.Float32,
            EmpStatus.agency_percentage: pl.Float32,
            EmpStatus.other_percentage: pl.Float32,
            EmpStatus.permanent_count_clean: pl.Int64,
            EmpStatus.temporary_count_clean: pl.Int64,
            EmpStatus.bank_or_pool_count_clean: pl.Int64,
            EmpStatus.agency_count_clean: pl.Int64,
            EmpStatus.other_count_clean: pl.Int64,
            EmpStatus.permanent_percentage_clean: pl.Float32,
            EmpStatus.temporary_percentage_clean: pl.Float32,
            EmpStatus.bank_or_pool_percentage_clean: pl.Float32,
            EmpStatus.agency_percentage_clean: pl.Float32,
            EmpStatus.other_percentage_clean: pl.Float32,
            EmpStatus.filtering_rule: pl.String,
        }
        source_rows = [
            (
                "1-001",
                2,
                1,
                0,
                1,
                0,
                0.5,
                0.25,
                0.0,
                0.25,
                0.0,
                2,
                1,
                0,
                1,
                0,
                0.5,
                0.25,
                0.0,
                0.25,
                0.0,
                EmploymentStatusFilteringRule.populated,
            ),
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
                    exclude_complex_types=True,
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
    def test_validation_report_includes_expected_checks(
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
            "col_vals_ge",
            "col_vals_between",
            "col_vals_not_null",
            "col_vals_in_set",
            "col_vals_expr",
        }
