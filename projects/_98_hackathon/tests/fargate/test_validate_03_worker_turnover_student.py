import json
from unittest.mock import Mock, patch

import polars as pl

import projects._98_hackathon.fargate.validate_03_worker_turnover_student as job
from projects._98_hackathon.tests.fargate.test_validate_01_worker_turnover import (
    build_source_df as build_worker_df,
)
from utils.column_names.hackathon_columns import (
    HackathonOutputColumns as OutCols,
)

PATCH_PATH = "projects._98_hackathon.fargate.validate_03_worker_turnover_student"


def build_source_df() -> pl.DataFrame:
    """Two rows of the student data, which has no is_leaver or turnover_rate."""
    return build_worker_df().drop(OutCols.is_leaver, OutCols.turnover_rate)


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_passes_when_answer_columns_are_absent(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = build_source_df()
        mock_scan_parquet.return_value = pl.LazyFrame()

        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once_with(
            source="s3://bucket/my/source/",
            selected_columns=job.vUtils.WORKER_DATA_COLUMNS,
        )
        report_json = json.loads(mock_write_reports.call_args[0][0].get_json_report())
        assert next(
            item
            for item in report_json
            if item["assertion_type"] == "specially" and "no is_leaver" in item["brief"]
        )["all_passed"]

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_fails_when_answer_columns_are_in_the_file(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        source_df = build_source_df().with_columns(
            pl.lit(True).alias(OutCols.is_leaver),
            pl.lit(0.5).alias(OutCols.turnover_rate),
        )
        mock_read_parquet.return_value = source_df
        mock_scan_parquet.return_value = source_df.lazy()

        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once_with(
            source="s3://bucket/my/source/",
            selected_columns=job.vUtils.WORKER_DATA_COLUMNS,
        )
        report_json = json.loads(mock_write_reports.call_args[0][0].get_json_report())
        assert not next(
            item
            for item in report_json
            if item["assertion_type"] == "specially" and "no is_leaver" in item["brief"]
        )["all_passed"]
