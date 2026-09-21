import json
from unittest.mock import Mock, call, patch

import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate.validate_04_estimate as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols

PATCH_PATH = (
    "projects._03_independent_cqc._02_employment_status.fargate.validate_04_estimate"
)


class TestMain:
    def setup_method(self):
        source_schema = {
            IndCqcColumns.location_id: pl.String,
            job.METRIC: pl.Float64,
            SLVCols.estimated_emp_stat_perm: pl.Float64,
            SLVCols.estimated_emp_stat_temp: pl.Float64,
            SLVCols.estimated_emp_stat_bank_or_pool: pl.Float64,
            SLVCols.estimated_emp_stat_agency: pl.Float64,
            SLVCols.estimated_emp_stat_other: pl.Float64,
        }
        source_rows = [
            ("1-001", 10.0, 5.0, 2.0, 1.5, 1.0, 0.5),
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
                call(source="s3://bucket/my/source/"),
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

        assert "row_count_match" in assertion_types_present
        assert "col_vals_ge" in assertion_types_present
        assert "col_vals_expr" in assertion_types_present
