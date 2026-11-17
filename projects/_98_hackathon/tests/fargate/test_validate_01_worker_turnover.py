import json
from dataclasses import dataclass
from datetime import date
from unittest.mock import Mock, patch

import polars as pl
import pytest

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


def build_source_df(
    start_dates: list[date | None] | None = None, ages: list[float | None] | None = None
) -> pl.DataFrame:
    """Two snapshots of a worker 12 months apart. The later snapshot has no
    snapshot 12 months after it, so its turnover rate is null. Start dates default
    to null and ages to 1."""
    return pl.DataFrame(
        data={
            AWKClean.establishment_id: ["100", "100"],
            AWKClean.worker_id: ["1", "1"],
            AWKClean.ascwds_worker_import_date: [date(2023, 1, 1), date(2024, 1, 1)],
            AWKClean.employment_status_clean_labelled: [EmpStat.permanent] * 2,
            AWKClean.main_job_role_clean_labelled: ["Care worker"] * 2,
            HackCols.main_service_type: [PrimaryServiceType.non_residential] * 2,
            HackCols.split: ["train"] * 2,
            HackCols.is_leaver: [False, None],
            HackCols.turnover_rate: [0.5, None],
            HackCols.valid_for_analysis: [True, False],
            HackCols.hours: [30.0, None],
            HackCols.hourly_pay: [12.0, None],
            HackCols.nlw_hourly_rate: [11.0, 11.0],
            HackCols.hourly_pay_minus_nlw: [1.0, None],
            HackCols.nationality_group: [NationalityGroupLabels.non_eea] * 2,
            HackCols.country_of_birth_group: [CountryOfBirthGroupLabels.non_eea] * 2,
            **{column: [1.0] * 2 for column in job.NUMERIC_COLUMNS},
            AWKClean.age: ages or [1.0] * 2,
            AWKClean.start_date: start_dates or [None] * 2,
            **{
                label_column: [vocabulary.label_cls(label_column).categorical_values[0]]
                * 2
                for label_column, vocabulary in zip(
                    wUtils.LABEL_COLUMNS, wUtils.LABEL_VOCABULARIES.values()
                )
            },
        },
        schema={
            AWKClean.establishment_id: CatColType.EstablishmentCatType,
            AWKClean.worker_id: pl.String,
            AWKClean.ascwds_worker_import_date: pl.Date,
            AWKClean.employment_status_clean_labelled: CatColType.EmploymentStatusCatType,
            AWKClean.main_job_role_clean_labelled: pl.Categorical,
            HackCols.main_service_type: CatColType.PrimaryServiceEnumType,
            HackCols.split: pl.Categorical,
            HackCols.is_leaver: pl.Boolean,
            HackCols.turnover_rate: pl.Float32,
            HackCols.valid_for_analysis: pl.Boolean,
            HackCols.hours: pl.Float32,
            HackCols.hourly_pay: pl.Float32,
            HackCols.nlw_hourly_rate: pl.Float32,
            HackCols.hourly_pay_minus_nlw: pl.Float32,
            HackCols.nationality_group: pl.Categorical,
            HackCols.country_of_birth_group: pl.Categorical,
            **{column: pl.Float32 for column in job.NUMERIC_COLUMNS},
            AWKClean.start_date: pl.Date,
            **{column: pl.Categorical for column in wUtils.LABEL_COLUMNS},
        },
    )


def build_compare_lf() -> pl.LazyFrame:
    """Cleaned worker data where the third row is dropped by the job's filter
    (agency worker)."""
    return pl.LazyFrame(
        {
            AWKClean.location_id: ["1-001", "1-001", "1-001"],
            AWKClean.ascwds_worker_import_date: [
                date(2023, 1, 1),
                date(2024, 1, 1),
                date(2023, 1, 1),
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


AFTER_IMPORT_BRIEF = "start date is on or before the import date"
START_AGE_BRIEF = "start date implies a start age of 16 or over where age is known"
TENURE_BRIEF = "start date is within 70 years of the import date where age is unknown"
START_DATE_BRIEFS = {AFTER_IMPORT_BRIEF, START_AGE_BRIEF, TENURE_BRIEF}


@dataclass
class StartDateCheckCase:
    id: str
    start_dates: list[date | None]
    ages: list[float | None]
    failed_briefs: set[str]


# The source rows are imported on 2023-01-01 and 2024-01-01.
START_DATE_CHECK_CASES = [
    StartDateCheckCase(
        id="fails_when_start_date_is_after_import_date",
        start_dates=[date(2023, 6, 1), None],
        ages=[30.0, 31.0],
        failed_briefs={AFTER_IMPORT_BRIEF},
    ),
    StartDateCheckCase(
        id="fails_when_start_date_implies_start_age_under_16",
        start_dates=[date(2013, 1, 1), None],
        ages=[20.0, 21.0],
        failed_briefs={START_AGE_BRIEF},
    ),
    StartDateCheckCase(
        id="fails_when_age_unknown_and_start_date_over_70_years_before_import",
        start_dates=[date(1940, 1, 1), None],
        ages=[None, None],
        failed_briefs={TENURE_BRIEF},
    ),
    StartDateCheckCase(
        id="passes_when_start_date_is_null",
        start_dates=[None, None],
        ages=[1.0, 1.0],
        failed_briefs=set(),
    ),
]


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

        job.main("bucket", "my/source/", "my/compare/", "my/reports/", "train")

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
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_ge",
            "col_vals_gt",
            "col_vals_between",
            "col_vals_in_set",
            "col_vals_expr",
            "specially",
        }
        assert assertion_types_present == expected_assertions

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

        job.main("bucket", "my/source/", "my/compare/", "my/reports/", "train")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_step = next(
            item for item in report_json if item["assertion_type"] == "row_count_match"
        )
        assert row_count_step["values"]["count"] == 2

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_expected_row_count_only_counts_the_split(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = build_source_df()
        mock_scan_parquet.return_value = build_compare_lf()

        job.main("bucket", "my/source/", "my/compare/", "my/reports/", "test")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_step = next(
            item for item in report_json if item["assertion_type"] == "row_count_match"
        )
        assert row_count_step["values"]["count"] == 0

    @pytest.mark.parametrize(
        "case", [pytest.param(case, id=case.id) for case in START_DATE_CHECK_CASES]
    )
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_start_date_checks_fail_only_for_implausible_start_dates(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
        case: StartDateCheckCase,
    ):
        mock_read_parquet.return_value = build_source_df(case.start_dates, case.ages)
        mock_scan_parquet.return_value = build_compare_lf()

        job.main("bucket", "my/source/", "my/compare/", "my/reports/", "train")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        briefs_in_report = {item["brief"] for item in report_json}
        failed_briefs = {
            item["brief"]
            for item in report_json
            if item["brief"] in START_DATE_BRIEFS and item["n_failed"] > 0
        }
        assert START_DATE_BRIEFS <= briefs_in_report
        assert failed_briefs == case.failed_briefs

    def test_source_columns_include_start_date(self):
        assert AWKClean.start_date in job.SOURCE_COLUMNS

    def test_source_columns_exclude_columns_dropped_from_job_output(self):
        dropped_columns = {
            *wUtils.PAY_AND_HOURS_COLUMNS,
            *wUtils.WORKER_CATEGORICAL_COLUMNS,
            HackCols.establishment_in_both_snapshots,
            HackCols.months_since_save_start,
            HackCols.months_since_save_end,
            HackCols.establishment_active,
            HackCols.headcount_reconciliation_gap,
            HackCols.headcount_gap_within_tolerance,
            HackCols.plausible_turnover,
            HackCols.plausible_start_date,
        }

        assert not dropped_columns & set(job.SOURCE_COLUMNS)
