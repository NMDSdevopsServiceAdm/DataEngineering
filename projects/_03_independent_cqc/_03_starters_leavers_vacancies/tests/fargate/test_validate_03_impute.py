import json
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.validate_03_impute as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)

PATCH_PATH = "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.validate_03_impute"

SOURCE_SCHEMA = {
    IndCqcColumns.location_id: pl.String,
    IndCqcColumns.published_job_role_label: pl.String,
    IndCqcColumns.cqc_location_import_date: pl.Date,
    SLVCols.ascwds_turnover_rate: pl.Float32,
    SLVCols.ascwds_turnover_rate_imputed_trendline: pl.Float32,
    SLVCols.ascwds_turnover_rate_rolling_avg: pl.Float32,
    SLVCols.ascwds_turnover_rate_imputed: pl.Float32,
}


def build_source_df(known: float | None, imputed: float | None) -> pl.DataFrame:
    """A location with a known rate in January and a second row with the given values."""
    return pl.DataFrame(
        [
            ("1-001", "Care worker", date(2024, 1, 1), 0.2, 0.2, 0.1, 0.2),
            ("1-001", "Care worker", date(2024, 2, 1), known, known, 0.1, imputed),
        ],
        SOURCE_SCHEMA,
        orient="row",
    )


def run_validation(source_df: pl.DataFrame) -> list[dict]:
    compare_df = source_df.select([IndCqcColumns.location_id])
    with (
        patch(f"{PATCH_PATH}.vl.write_reports") as mock_write_reports,
        patch(f"{PATCH_PATH}.utils.read_parquet") as mock_read_parquet,
    ):
        mock_read_parquet.side_effect = [source_df, compare_df]
        job.main("bucket", "my/source/", "my/compare/", "my/reports/")
    return json.loads(mock_write_reports.call_args[0][0].get_json_report())


def failed_briefs(report: list[dict]) -> list[str]:
    return [step["brief"] for step in report if step["n_failed"]]


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        source_df = build_source_df(known=0.3, imputed=0.3)
        compare_df = source_df.select([IndCqcColumns.location_id])
        mock_read_parquet.side_effect = [source_df, compare_df]

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

    def test_validation_report_includes_expected_validations(self):
        report = run_validation(build_source_df(known=0.3, imputed=0.3))

        assert {step["assertion_type"] for step in report} == {
            "row_count_match",
            "col_vals_ge",
            "col_vals_expr",
        }

    def test_validation_passes_for_valid_data(self):
        report = run_validation(build_source_df(known=0.3, imputed=0.3))

        assert failed_briefs(report) == []


class TestValidate03ImputeRules:
    def test_validate_03_impute_flags_changed_known_rate(self):
        report = run_validation(build_source_df(known=0.3, imputed=0.4))

        assert failed_briefs(report) == [job.KNOWN_RATE_KEPT_BRIEF]

    def test_validate_03_impute_flags_negative_imputed_rate(self):
        report = run_validation(build_source_df(known=None, imputed=-0.1))

        assert failed_briefs(report) == [job.IMPUTED_NOT_NEGATIVE_BRIEF]

    def test_validate_03_impute_flags_null_imputed_without_group_data(self):
        report = run_validation(build_source_df(known=None, imputed=None))

        assert failed_briefs(report) == [job.IMPUTED_POPULATED_BRIEF]

    def test_validate_03_impute_allows_null_imputed_when_location_has_no_known_rate(
        self,
    ):
        source_df = pl.DataFrame(
            [("1-001", "Care worker", date(2024, 1, 1), None, None, 0.1, None)],
            SOURCE_SCHEMA,
            orient="row",
        )

        report = run_validation(source_df)

        assert failed_briefs(report) == []
