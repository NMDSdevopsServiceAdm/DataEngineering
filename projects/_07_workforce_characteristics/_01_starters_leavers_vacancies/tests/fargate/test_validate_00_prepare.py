import json
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare"


class TestMain:
    @pytest.fixture(autouse=True)
    def setup(self):
        source_schema = {
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
            SLVCols.job_role_label: pl.String,
            SLVCols.employees: pl.Int64,
            SLVCols.starters: pl.Int64,
            SLVCols.leavers: pl.Int64,
            SLVCols.vacancies: pl.Int64,
        }
        source_rows = [
            ("1-001", date(2026, 1, 1), label, 1, 1, 1, 1)
            for label in job.PUBLISHED_JOB_ROLE_LABELS
        ]
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")

        # The compare frame is the unreduced cleaned ASCWDS data, so it carries rows
        # the reduction filters drop. Dates are chosen to stay stable as time passes:
        # January always survives (quarterly sampling) and a pre-window May never does.
        # 1-003 shares January with 1-001 but has a later date, so it's dropped by the
        # earliest-file-per-month filter rather than the retention filter - proving the
        # monthly reduction itself is exercised here, not just retention.
        compare_schema = {
            AWPClean.location_id: pl.String,
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
        }
        compare_rows = [
            ("Loc-001", "1-001", date(2026, 1, 1)),
            ("Loc-003", "1-003", date(2026, 1, 15)),  # same month as 1-001, later date -> dropped by monthly filter
            ("Loc-002", "1-002", date(2020, 5, 1)),
        ]  # fmt: skip
        self.compare_df = pl.DataFrame(compare_rows, compare_schema, orient="row")

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        assert mock_read_parquet.call_count == 2
        mock_read_parquet.assert_has_calls(
            [
                call(source="s3://bucket/my/source/"),
                call(
                    source="s3://bucket/my/compare/",
                    selected_columns=job.COMPARE_COLS_TO_IMPORT,
                ),
            ]
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "row_count_match",
            "specially",
            "rows_distinct",
            "col_vals_in_set",
        }

        for assertion in expected_assertions:
            assert (
                assertion in assertion_types_present
            ), f"{assertion} not found in validation report"

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_expected_row_count_is_multiplied_by_published_label_count(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        # Only 1-001 survives the compare-side reduction filters (see setup's comment),
        # so each published label contributes exactly one exploded row for it.
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_match_item = next(
            item for item in report_json if item["assertion_type"] == "row_count_match"
        )

        assert row_count_match_item["values"]["count"] == len(
            job.PUBLISHED_JOB_ROLE_LABELS
        )


class TestNoLeftoverRawJobRoleCodeColumns:
    def test_true_when_no_raw_job_role_code_columns_remain(self):
        df = pl.DataFrame(schema={AWPClean.establishment_id: pl.String})

        assert job.no_leftover_raw_job_role_code_columns(df)

    def test_false_when_a_raw_jrNN_column_remains(self):
        df = pl.DataFrame(schema={"jr01emp": pl.Int64})

        assert not job.no_leftover_raw_job_role_code_columns(df)
