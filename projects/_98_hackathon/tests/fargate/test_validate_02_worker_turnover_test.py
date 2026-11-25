import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._98_hackathon.fargate.validate_02_worker_turnover_test as job
from utils.column_names.hackathon_columns import HackathonOutputColumns as OutCols

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


def run_main(mock_read_parquet: Mock, answers_df: pl.DataFrame) -> None:
    mock_read_parquet.return_value = answers_df

    job.main("bucket", "my/source/", "my/reports/")


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        run_main(mock_read_parquet, build_answers_df())

        mock_read_parquet.assert_called_once_with(
            source="s3://bucket/my/source/", selected_columns=job.SOURCE_COLUMNS
        )
        mock_write_reports.assert_called_once()

        report_json = json.loads(mock_write_reports.call_args[0][0].get_json_report())
        assert {item["assertion_type"] for item in report_json} == {
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_between",
            "col_vals_expr",
        }

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_fails_when_turnover_rate_is_populated_without_is_leaver(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        answers_df = build_answers_df().with_columns(
            pl.Series(OutCols.is_leaver, [True, None])
        )
        run_main(mock_read_parquet, answers_df)

        report_json = json.loads(mock_write_reports.call_args[0][0].get_json_report())
        populated_step = next(
            item
            for item in report_json
            if item["assertion_type"] == "col_vals_expr"
            and "populated exactly" in item["brief"]
        )
        assert populated_step["all_passed"] is False
