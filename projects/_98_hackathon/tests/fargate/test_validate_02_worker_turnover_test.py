import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._98_hackathon.fargate.validate_02_worker_turnover_test as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import (
    HackathonOutputColumns as OutCols,
)
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat

PATCH_PATH = "projects._98_hackathon.fargate.validate_02_worker_turnover_test"


def build_answers_df() -> pl.DataFrame:
    """Two rows of answers: key columns, is_leaver and turnover_rate."""
    return pl.DataFrame(
        {
            OutCols.establishmentid: ["100", "100"],
            OutCols.workerid: ["1", "2"],
            OutCols.snapshot_date: [date(2025, 11, 1)] * 2,
            OutCols.is_leaver: [True, False],
            OutCols.turnover_rate: [0.5, 0.5],
        }
    )


def build_compare_lf() -> pl.LazyFrame:
    """Cleaned worker data with two rows kept by the job's filter."""
    return pl.LazyFrame(
        {
            AWKClean.location_id: ["1-001"] * 2,
            AWKClean.ascwds_worker_import_date: [date(2025, 11, 1)] * 2,
            AWKClean.employment_status_clean_labelled: [EmpStat.permanent] * 2,
        },
        schema={
            AWKClean.location_id: pl.String,
            AWKClean.ascwds_worker_import_date: pl.Date,
            AWKClean.employment_status_clean_labelled: CatColType.EmploymentStatusCatType,
        },
    )


def run_main(
    mock_read_parquet: Mock,
    mock_scan_parquet: Mock,
    answers_df: pl.DataFrame,
) -> None:
    mock_read_parquet.return_value = answers_df
    mock_scan_parquet.return_value = build_compare_lf()

    job.main("bucket", "my/source/", "my/compare/", "my/reports/")


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        run_main(mock_read_parquet, mock_scan_parquet, build_answers_df())

        mock_read_parquet.assert_called_once_with(
            source="s3://bucket/my/source/", selected_columns=job.SOURCE_COLUMNS
        )
        mock_write_reports.assert_called_once()

        report_json = json.loads(mock_write_reports.call_args[0][0].get_json_report())
        assert {item["assertion_type"] for item in report_json} == {
            "row_count_match",
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_between",
            "col_vals_expr",
        }
        row_count_step = next(
            item for item in report_json if item["assertion_type"] == "row_count_match"
        )
        assert row_count_step["values"]["count"] == 2

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_fails_when_turnover_rate_is_populated_without_is_leaver(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        answers_df = build_answers_df().with_columns(
            pl.Series(OutCols.is_leaver, [True, None])
        )
        run_main(mock_read_parquet, mock_scan_parquet, answers_df)

        report_json = json.loads(mock_write_reports.call_args[0][0].get_json_report())
        populated_step = next(
            item
            for item in report_json
            if item["assertion_type"] == "col_vals_expr"
            and "populated exactly" in item["brief"]
        )
        assert populated_step["all_passed"] is False
