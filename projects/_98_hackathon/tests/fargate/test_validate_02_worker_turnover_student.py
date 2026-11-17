import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._98_hackathon.fargate.validate_02_worker_turnover_student as job
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols

PATCH_PATH = "projects._98_hackathon.fargate.validate_02_worker_turnover_student"


def build_student_df() -> pl.DataFrame:
    """Two rows of the student version of the test split: no is_leaver or
    turnover_rate."""
    return pl.DataFrame(
        {
            AWKClean.establishment_id: ["100", "100"],
            AWKClean.worker_id: ["1", "2"],
            AWKClean.ascwds_worker_import_date: [date(2025, 11, 1)] * 2,
            HackCols.split: ["test"] * 2,
            HackCols.valid_for_analysis: [True, False],
            HackCols.hourly_pay: [12.0, None],
        }
    )


def build_test_lf(row_count: int = 2) -> pl.LazyFrame:
    """The test split the student version is compared with."""
    return pl.LazyFrame({HackCols.split: ["test"] * row_count})


def run_main(
    mock_read_parquet: Mock,
    mock_scan_parquet: Mock,
    student_df: pl.DataFrame,
    test_lf: pl.LazyFrame,
) -> None:
    mock_read_parquet.return_value = student_df
    mock_scan_parquet.return_value = test_lf

    job.main("bucket", "my/source/", "my/test/", "my/reports/")


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
        run_main(
            mock_read_parquet, mock_scan_parquet, build_student_df(), build_test_lf()
        )

        mock_read_parquet.assert_called_once_with(source="s3://bucket/my/source/")
        mock_scan_parquet.assert_called_once_with(
            "s3://bucket/my/test/", selected_columns=[HackCols.split]
        )
        mock_write_reports.assert_called_once()

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "row_count_match",
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_in_set",
            "specially",
        }
        assert assertion_types_present == expected_assertions

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_expected_row_count_comes_from_test_dataset(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        run_main(
            mock_read_parquet,
            mock_scan_parquet,
            build_student_df(),
            build_test_lf(row_count=5),
        )

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_step = next(
            item for item in report_json if item["assertion_type"] == "row_count_match"
        )
        assert row_count_step["values"]["count"] == 5
