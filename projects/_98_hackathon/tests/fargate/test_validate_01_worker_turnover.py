import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
import projects._98_hackathon.fargate.validate_01_worker_turnover as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat
from utils.column_values.categorical_column_values import PrimaryServiceType

PATCH_PATH = "projects._98_hackathon.fargate.validate_01_worker_turnover"


def build_source_df() -> pl.DataFrame:
    """A worker who stays between two snapshots 12 months apart. The later
    snapshot has no snapshot 12 months after it, so its flags are null."""
    return pl.DataFrame(
        data={
            AWKClean.establishment_id: ["100", "100"],
            AWKClean.worker_id: ["1", "1"],
            AWKClean.ascwds_worker_import_date: [date(2024, 1, 1), date(2025, 1, 1)],
            AWKClean.employment_status_clean_labelled: [EmpStat.permanent] * 2,
            HackCols.main_service_type: [PrimaryServiceType.non_residential] * 2,
            HackCols.establishment_in_both_snapshots: [True, None],
            HackCols.is_leaver: [False, None],
            HackCols.months_since_save_start: [1, 1],
            HackCols.months_since_save_end: [1, None],
            HackCols.establishment_active: [True, None],
            **{column: [1.0] * 2 for column in wUtils.WORKER_NUMERIC_COLUMNS},
            **{column: ["1"] * 2 for column in wUtils.WORKER_CATEGORICAL_COLUMNS},
        },
        schema={
            AWKClean.establishment_id: CatColType.EstablishmentCatType,
            AWKClean.worker_id: pl.String,
            AWKClean.ascwds_worker_import_date: pl.Date,
            AWKClean.employment_status_clean_labelled: CatColType.EmploymentStatusCatType,
            HackCols.main_service_type: CatColType.PrimaryServiceEnumType,
            HackCols.establishment_in_both_snapshots: pl.Boolean,
            HackCols.is_leaver: pl.Boolean,
            HackCols.months_since_save_start: pl.Int32,
            HackCols.months_since_save_end: pl.Int32,
            HackCols.establishment_active: pl.Boolean,
            **{column: pl.Float32 for column in wUtils.WORKER_NUMERIC_COLUMNS},
            **{column: pl.Categorical for column in wUtils.WORKER_CATEGORICAL_COLUMNS},
        },
    )


def build_compare_lf() -> pl.LazyFrame:
    """Cleaned worker data where the third row is dropped by the job's filter
    (agency worker)."""
    return pl.LazyFrame(
        {
            AWKClean.location_id: ["1-001", "1-001", "1-001"],
            AWKClean.ascwds_worker_import_date: [
                date(2024, 1, 1),
                date(2025, 1, 1),
                date(2024, 1, 1),
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
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = build_source_df()
        mock_scan_parquet.return_value = build_compare_lf()

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        mock_read_parquet.assert_called_once_with(
            source="s3://bucket/my/source/", selected_columns=job.SOURCE_COLUMNS
        )
        mock_scan_parquet.assert_called_once_with(
            "s3://bucket/my/compare/", selected_columns=job.COMPARE_COLUMNS
        )
        mock_write_reports.assert_called_once()

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "row_count_match",
            "col_exists",
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_ge",
            "col_vals_in_set",
            "col_vals_expr",
            "specially",
        }
        assert expected_assertions <= assertion_types_present

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_expected_row_count_comes_from_filtered_cleaned_worker(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = build_source_df()
        mock_scan_parquet.return_value = build_compare_lf()

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_step = next(
            item for item in report_json if item["assertion_type"] == "row_count_match"
        )
        assert row_count_step["values"]["count"] == 2


class TestHasAtMostOneEndMonthPerStartMonth:
    @staticmethod
    def build_df(import_dates: list[date]) -> pl.DataFrame:
        return pl.DataFrame(
            {AWKClean.ascwds_worker_import_date: import_dates},
            schema={AWKClean.ascwds_worker_import_date: pl.Date},
        )

    def test_returns_true_when_one_import_date_per_month(self):
        df = self.build_df(
            [date(2024, 1, 1), date(2024, 1, 1), date(2024, 2, 1), date(2025, 1, 1)]
        )

        assert job.has_at_most_one_end_month_per_start_month(df) is True

    def test_returns_false_when_two_import_dates_in_one_month(self):
        df = self.build_df([date(2024, 1, 1), date(2025, 1, 1), date(2025, 1, 15)])

        assert job.has_at_most_one_end_month_per_start_month(df) is False
