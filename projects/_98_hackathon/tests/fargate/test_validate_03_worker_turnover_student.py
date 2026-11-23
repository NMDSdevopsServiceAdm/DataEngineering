import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
import projects._98_hackathon.fargate.validate_03_worker_turnover_student as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import AgeSource
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_values.ascwds_labelled_vocab import (
    CountryOfBirthGroupLabels,
    NationalityGroupLabels,
)
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat
from utils.column_values.categorical_column_values import (
    CurrentCSSR,
    PrimaryServiceType,
    Region,
)

PATCH_PATH = "projects._98_hackathon.fargate.validate_03_worker_turnover_student"


def build_source_df() -> pl.DataFrame:
    """Two rows of the student data, which has no is_leaver or turnover_rate."""
    return pl.DataFrame(
        data={
            AWKClean.establishment_id: ["100", "100"],
            AWKClean.worker_id: ["1", "1"],
            AWKClean.ascwds_worker_import_date: [date(2025, 11, 1)] * 2,
            AWKClean.employment_status_clean_labelled: [EmpStat.permanent] * 2,
            AWKClean.main_job_role_clean_labelled: ["Care worker"] * 2,
            AWPClean.organisation_id: ["1-ORG"] * 2,
            HackCols.main_service_type: [PrimaryServiceType.non_residential] * 2,
            HackCols.split: ["test"] * 2,
            HackCols.valid_for_analysis: [True, False],
            HackCols.hours: [30.0, None],
            HackCols.hourly_pay: [12.0, None],
            HackCols.nlw_hourly_rate: [11.0, 11.0],
            HackCols.hourly_pay_minus_nlw: [1.0, None],
            HackCols.age_imputed: [30.0, 31.0],
            HackCols.age_source: [AgeSource.observed, AgeSource.assumed_birth_month],
            HackCols.nationality_group: [NationalityGroupLabels.non_eea] * 2,
            HackCols.country_of_birth_group: [CountryOfBirthGroupLabels.non_eea] * 2,
            ONSClean.current_cssr: [CurrentCSSR.barnet] * 2,
            ONSClean.current_region: [Region.london] * 2,
            **{column: [1.0] * 2 for column in job.vUtils.NUMERIC_COLUMNS},
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
            AWPClean.organisation_id: pl.Categorical,
            HackCols.main_service_type: CatColType.PrimaryServiceEnumType,
            HackCols.split: pl.Categorical,
            HackCols.valid_for_analysis: pl.Boolean,
            HackCols.hours: pl.Float32,
            HackCols.hourly_pay: pl.Float32,
            HackCols.nlw_hourly_rate: pl.Float32,
            HackCols.hourly_pay_minus_nlw: pl.Float32,
            HackCols.age_imputed: pl.Float32,
            HackCols.age_source: pl.Categorical,
            HackCols.nationality_group: pl.Categorical,
            HackCols.country_of_birth_group: pl.Categorical,
            ONSClean.current_cssr: pl.Categorical,
            ONSClean.current_region: pl.Categorical,
            **{column: pl.Float32 for column in job.vUtils.NUMERIC_COLUMNS},
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
            pl.lit(True).alias(HackCols.is_leaver),
            pl.lit(0.5).alias(HackCols.turnover_rate),
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
