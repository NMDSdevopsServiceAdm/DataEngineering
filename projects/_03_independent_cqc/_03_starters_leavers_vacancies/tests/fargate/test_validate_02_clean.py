import json
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.validate_02_clean as job
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)
from utils.column_values.categorical_column_values import SLVFilteringRule

PATCH_PATH = "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.validate_02_clean"


class TestMain:
    @pytest.fixture(autouse=True)
    def setup(self):
        source_schema = {
            IndCqcColumns.location_id: pl.String,
            EmpStatus.employee_count: pl.Int64,
            SLVCols.starters_cleaned: pl.Int64,
            SLVCols.leavers_cleaned: pl.Int64,
            SLVCols.vacancies_cleaned: pl.Int64,
            SLVCols.starters_cleaned_dedup: pl.Int64,
            SLVCols.leavers_cleaned_dedup: pl.Int64,
            SLVCols.vacancies_cleaned_dedup: pl.Int64,
            SLVCols.turnover_rate: pl.Float32,
            SLVCols.starter_rate: pl.Float32,
            SLVCols.vacancy_rate: pl.Float32,
            SLVCols.starters_filtering_rule: pl.String,
            SLVCols.leavers_filtering_rule: pl.String,
            SLVCols.vacancies_filtering_rule: pl.String,
        }
        source_rows = [
            (
                "1-001",
                2,
                1,
                1,
                1,
                1,
                1,
                1,
                0.1,
                0.2,
                0.3,
                SLVFilteringRule.populated,
                SLVFilteringRule.populated,
                SLVFilteringRule.populated,
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

        expected_assertions = {
            "row_count_match",
            "col_vals_ge",
            "col_vals_between",
            "col_vals_in_set",
        }

        for assertion in expected_assertions:
            assert (
                assertion in assertion_types_present
            ), f"{assertion} not found in validation report"
