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
from utils.column_values.ascwds_labelled_vocab import (
    CountryOfBirthGroupLabels,
    NationalityGroupLabels,
)
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
            AWKClean.main_job_role_clean_labelled: ["Care worker"] * 2,
            HackCols.main_service_type: [PrimaryServiceType.non_residential] * 2,
            HackCols.establishment_in_both_snapshots: [True, None],
            HackCols.is_leaver: [False, None],
            HackCols.months_since_save_start: [1, 1],
            HackCols.months_since_save_end: [1, None],
            HackCols.establishment_active: [True, None],
            HackCols.turnover_rate: [0.5, None],
            HackCols.headcount_reconciliation_gap: [0, None],
            HackCols.headcount_gap_within_tolerance: [True, None],
            HackCols.plausible_turnover: [True, None],
            HackCols.valid_for_analysis: [True, False],
            HackCols.hours: [30.0, None],
            HackCols.hourly_pay: [12.0, None],
            HackCols.nlw_hourly_rate: [11.0, 11.0],
            HackCols.hourly_pay_minus_nlw: [1.0, None],
            HackCols.nationality_group: [NationalityGroupLabels.non_eea] * 2,
            HackCols.country_of_birth_group: [CountryOfBirthGroupLabels.non_eea] * 2,
            **{column: [1.0] * 2 for column in job.NUMERIC_COLUMNS},
            **{column: ["1"] * 2 for column in wUtils.WORKER_CATEGORICAL_COLUMNS},
            **{column: ["label"] * 2 for column in job.LABEL_COLUMNS},
        },
        schema={
            AWKClean.establishment_id: CatColType.EstablishmentCatType,
            AWKClean.worker_id: pl.String,
            AWKClean.ascwds_worker_import_date: pl.Date,
            AWKClean.employment_status_clean_labelled: CatColType.EmploymentStatusCatType,
            AWKClean.main_job_role_clean_labelled: pl.Categorical,
            HackCols.main_service_type: CatColType.PrimaryServiceEnumType,
            HackCols.establishment_in_both_snapshots: pl.Boolean,
            HackCols.is_leaver: pl.Boolean,
            HackCols.months_since_save_start: pl.Int32,
            HackCols.months_since_save_end: pl.Int32,
            HackCols.establishment_active: pl.Boolean,
            HackCols.turnover_rate: pl.Float32,
            HackCols.headcount_reconciliation_gap: pl.Int32,
            HackCols.headcount_gap_within_tolerance: pl.Boolean,
            HackCols.plausible_turnover: pl.Boolean,
            HackCols.valid_for_analysis: pl.Boolean,
            HackCols.hours: pl.Float32,
            HackCols.hourly_pay: pl.Float32,
            HackCols.nlw_hourly_rate: pl.Float32,
            HackCols.hourly_pay_minus_nlw: pl.Float32,
            HackCols.nationality_group: pl.Categorical,
            HackCols.country_of_birth_group: pl.Categorical,
            **{column: pl.Float32 for column in job.NUMERIC_COLUMNS},
            **{column: pl.Categorical for column in wUtils.WORKER_CATEGORICAL_COLUMNS},
            **{column: pl.Categorical for column in job.LABEL_COLUMNS},
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


class TestMainChecks:
    @staticmethod
    def run_main(source_df: pl.DataFrame) -> list[dict]:
        with (
            patch(f"{PATCH_PATH}.vl.write_reports") as mock_write_reports,
            patch(f"{PATCH_PATH}.utils.scan_parquet") as mock_scan_parquet,
            patch(f"{PATCH_PATH}.utils.read_parquet") as mock_read_parquet,
        ):
            mock_read_parquet.return_value = source_df
            mock_scan_parquet.return_value = build_compare_lf()

            job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        return json.loads(validation_arg.get_json_report())

    def test_all_checks_pass_on_valid_data(self):
        report = self.run_main(build_source_df())

        failed = [item["brief"] for item in report if not item["all_passed"]]
        assert failed == []

    def test_source_columns_exclude_dropped_pay_and_hours_columns(self):
        assert not set(wUtils.PAY_AND_HOURS_COLUMNS) & set(job.SOURCE_COLUMNS)

    def test_fails_when_valid_for_analysis_true_with_a_false_input_flag(self):
        source_df = build_source_df().with_columns(
            pl.lit(False).alias(HackCols.headcount_gap_within_tolerance)
        )

        report = self.run_main(source_df)

        failed = [item["brief"] for item in report if not item["all_passed"]]
        assert any("valid_for_analysis is true exactly" in brief for brief in failed)

    def test_fails_when_main_job_role_label_is_null(self):
        source_df = build_source_df().with_columns(
            pl.lit(None, dtype=pl.Categorical).alias(
                AWKClean.main_job_role_clean_labelled
            )
        )

        report = self.run_main(source_df)

        failed = [item["brief"] for item in report if not item["all_passed"]]
        assert "main job role label has no nulls" in failed

    def test_fails_when_tolerance_flag_true_for_gap_above_max_gap(self):
        source_df = build_source_df().with_columns(
            pl.when(pl.col(HackCols.headcount_reconciliation_gap).is_not_null())
            .then(3)
            .cast(pl.Int32)
            .alias(HackCols.headcount_reconciliation_gap)
        )

        report = self.run_main(source_df)

        failed = [item["brief"] for item in report if not item["all_passed"]]
        assert any("headcount_gap_within_tolerance is true" in b for b in failed)

    def test_passes_tolerance_check_when_flag_false_for_gap_above_max_gap(self):
        source_df = build_source_df().with_columns(
            pl.when(pl.col(HackCols.headcount_reconciliation_gap).is_not_null())
            .then(3)
            .cast(pl.Int32)
            .alias(HackCols.headcount_reconciliation_gap),
            pl.when(pl.col(HackCols.headcount_gap_within_tolerance).is_not_null())
            .then(False)
            .alias(HackCols.headcount_gap_within_tolerance),
            pl.col(HackCols.valid_for_analysis).and_(False),
        )

        report = self.run_main(source_df)

        failed = [item["brief"] for item in report if not item["all_passed"]]
        assert failed == []

    def test_fails_when_turnover_rate_above_one(self):
        source_df = build_source_df().with_columns(
            pl.when(pl.col(HackCols.turnover_rate).is_not_null())
            .then(1.5)
            .cast(pl.Float32)
            .alias(HackCols.turnover_rate)
        )

        report = self.run_main(source_df)

        failed = [item["brief"] for item in report if not item["all_passed"]]
        assert "turnover_rate is between 0 and 1" in failed

    def test_fails_when_label_missing_for_a_code(self):
        source_df = build_source_df().with_columns(
            pl.lit(None, dtype=pl.Categorical).alias(job.LABEL_COLUMNS[0])
        )

        report = self.run_main(source_df)

        failed = [item["brief"] for item in report if not item["all_passed"]]
        assert any("label column is populated exactly" in brief for brief in failed)


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
