import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._98_hackathon.fargate.validate_03_worker_turnover_student as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._98_hackathon.tests.fargate.test_validate_01_worker_turnover import (
    build_source_df as build_worker_df,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import (
    HackathonOutputColumns as OutCols,
)
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat

PATCH_PATH = "projects._98_hackathon.fargate.validate_03_worker_turnover_student"


def build_source_df() -> pl.DataFrame:
    """Two rows of the student data, which has no is_leaver or turnover_rate."""
    return build_worker_df(split="test").drop(OutCols.is_leaver, OutCols.turnover_rate)


def build_compare_lf() -> pl.LazyFrame:
    """Cleaned worker data where the third row is dropped by the job's filter
    (agency worker)."""
    return pl.LazyFrame(
        {
            AWKClean.location_id: ["1-001", "1-001", "1-001"],
            AWKClean.ascwds_worker_import_date: [
                date(2025, 11, 1),
                date(2025, 11, 1),
                date(2025, 11, 1),
            ],
            AWKClean.employment_status_clean_labelled: [
                EmpStat.permanent,
                EmpStat.permanent,
                EmpStat.agency,
            ],
        },
        schema={
            AWKClean.location_id: pl.String,
            AWKClean.ascwds_worker_import_date: pl.Date,
            AWKClean.employment_status_clean_labelled: CatColType.EmploymentStatusCatType,
        },
    )


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
        mock_scan_parquet.side_effect = [pl.LazyFrame(), build_compare_lf()]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

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
        mock_scan_parquet.side_effect = [source_df.lazy(), build_compare_lf()]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

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

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_expected_row_count_is_the_test_split_row_count(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = build_source_df()
        mock_scan_parquet.side_effect = [pl.LazyFrame(), build_compare_lf()]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        report_json = json.loads(mock_write_reports.call_args[0][0].get_json_report())
        row_count_step = next(
            item for item in report_json if item["assertion_type"] == "row_count_match"
        )
        assert row_count_step["values"]["count"] == 2
