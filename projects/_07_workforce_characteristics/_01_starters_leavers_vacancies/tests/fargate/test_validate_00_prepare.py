import json
import unittest
from dataclasses import dataclass
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.slv_job_role_columns import SlvJobRoleColumns as SLVJR

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare"


@dataclass
class DiscoverJobRoleCodeCountCase:
    id: str
    schema: dict
    expected_count: int

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


discover_job_role_code_count_cases = [
    DiscoverJobRoleCodeCountCase(
        id="returns_zero_when_no_job_role_columns_present",
        schema={
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
            "region": pl.String,
        },
        expected_count=0,
    ),
    DiscoverJobRoleCodeCountCase(
        id="counts_a_single_code",
        schema={
            AWPClean.establishment_id: pl.String,
            "jr01emp": pl.Int32,
            "jr01strt": pl.Int32,
            "jr01stop": pl.Int32,
            "jr01vacy": pl.Int32,
        },
        expected_count=1,
    ),
    DiscoverJobRoleCodeCountCase(
        id="counts_multiple_codes_of_varying_digit_length",
        schema={
            AWPClean.establishment_id: pl.String,
            "jr01emp": pl.Int32,
            "jr01strt": pl.Int32,
            "jr01stop": pl.Int32,
            "jr01vacy": pl.Int32,
            "jr43emp": pl.Int32,
            "jr43strt": pl.Int32,
            "jr43stop": pl.Int32,
            "jr43vacy": pl.Int32,
            "jr1001emp": pl.Int32,
            "jr1001strt": pl.Int32,
            "jr1001stop": pl.Int32,
            "jr1001vacy": pl.Int32,
        },
        expected_count=3,
    ),
    DiscoverJobRoleCodeCountCase(
        id="ignores_non_metric_suffixes_for_the_same_code",
        schema={
            AWPClean.establishment_id: pl.String,
            "jr01emp": pl.Int32,
            "jr01strt": pl.Int32,
            "jr01stop": pl.Int32,
            "jr01vacy": pl.Int32,
            "jr01temp": pl.Int32,
            "jr01agcy": pl.Int32,
        },
        expected_count=1,
    ),
]


class TestDiscoverJobRoleCodeCount:
    @pytest.mark.parametrize(
        "case",
        [c.as_pytest_param() for c in discover_job_role_code_count_cases],
    )
    def test_discover_job_role_code_count(self, case):
        assert job.discover_job_role_code_count(case.schema) == case.expected_count


class ValidatePreparedSLVDataTests(unittest.TestCase):
    def setUp(self) -> None:
        source_schema = {
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
            SLVJR.job_role_code: pl.String,
        }
        source_rows = [
            ("1-001", date(2026, 1, 1), "1"),
            ("1-001", date(2026, 1, 1), "43"),
        ]  # fmt: skip
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")

        # The compare frame is the unreduced, wide, pre-reshape cleaned ASCWDS data, so
        # it carries rows the reduction filters drop plus the jr* columns the reshape
        # explodes into rows. Dates are chosen to stay stable as time passes: January
        # always survives (quarterly sampling) and a pre-window May never does. 1-003
        # shares January with 1-001 but has a later date, so it's dropped by the
        # earliest-file-per-month filter rather than the retention filter - proving the
        # monthly reduction itself is exercised here, not just retention. Two job-role
        # codes (01, 43) are present, matching source_df's two rows per establishment.
        compare_schema = {
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
            "jr01emp": pl.Int32,
            "jr01strt": pl.Int32,
            "jr01stop": pl.Int32,
            "jr01vacy": pl.Int32,
            "jr43emp": pl.Int32,
            "jr43strt": pl.Int32,
            "jr43stop": pl.Int32,
            "jr43vacy": pl.Int32,
        }
        compare_rows = [
            ("1-001", date(2026, 1, 1), 1, 0, 0, 1, 2, 1, 0, 0),
            ("1-003", date(2026, 1, 15), 1, 0, 0, 1, 2, 1, 0, 0),  # same month as 1-001, later date -> dropped by monthly filter
            ("1-002", date(2020, 5, 1), 1, 0, 0, 1, 2, 1, 0, 0),  # before the retention window -> dropped
        ]  # fmt: skip
        self.compare_df = pl.DataFrame(compare_rows, compare_schema, orient="row")
        self.compare_schema = pl.Schema(compare_schema)

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.utils.discover_combined_schema")
    def test_validation_runs(
        self,
        mock_discover_combined_schema: Mock,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_discover_combined_schema.return_value = self.compare_schema
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        mock_discover_combined_schema.assert_called_once_with("s3://bucket/my/compare/")

        self.assertEqual(mock_read_parquet.call_count, 2)
        mock_read_parquet.assert_has_calls(
            [
                call(source="s3://bucket/my/source/"),
                call(
                    source="s3://bucket/my/compare/",
                    schema=self.compare_schema,
                    selected_columns=job.COMPARE_COLS_TO_IMPORT,
                ),
            ]
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.utils.discover_combined_schema")
    def test_validation_report_includes_expected_validations(
        self,
        mock_discover_combined_schema: Mock,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_discover_combined_schema.return_value = self.compare_schema
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "row_count_match",
            "rows_distinct",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
