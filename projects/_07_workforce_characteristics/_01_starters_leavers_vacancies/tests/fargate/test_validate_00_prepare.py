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


def _wide_job_role_schema(codes: list[str]) -> dict:
    schema = {AWPClean.establishment_id: pl.String}
    for code in codes:
        for suffix in ["emp", "strt", "stop", "vacy"]:
            schema[f"jr{code}{suffix}"] = pl.Int32
    return schema


@dataclass
class DiscoverJobRoleCodeCountAfterTransformsCase:
    id: str
    raw_schema: dict
    expected_count: int

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


discover_job_role_code_count_after_transforms_cases = [
    DiscoverJobRoleCodeCountAfterTransformsCase(
        id="returns_zero_when_no_job_role_columns_present",
        raw_schema={
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
            "region": pl.String,
        },
        expected_count=0,
    ),
    DiscoverJobRoleCodeCountAfterTransformsCase(
        # merge_job_role_columns() requires at least one matching source column
        # per mapping key (01/43 untouched; 02+03->1001, 35->1002, 10->1003,
        # 25->1004 are minimal representatives for each synthetic code; 28/30 are
        # summary columns dropped outright, not part of any mapping).
        id="merges_unpublished_codes_drops_summary_columns_and_adds_synthetic_codes",
        raw_schema=_wide_job_role_schema(
            ["01", "43", "02", "03", "35", "10", "25", "28", "30"]
        ),
        expected_count=6,  # 01, 43, 1001, 1002, 1003, 1004
    ),
]


class TestDiscoverJobRoleCodeCountAfterTransforms:
    @pytest.mark.parametrize(
        "case",
        [
            c.as_pytest_param()
            for c in discover_job_role_code_count_after_transforms_cases
        ],
    )
    def test_discover_job_role_code_count_after_transforms(self, case):
        assert (
            job.discover_job_role_code_count_after_transforms(case.raw_schema)
            == case.expected_count
        )


class ValidatePreparedSLVDataTests(unittest.TestCase):
    def setUp(self) -> None:
        # Grain matches the 6 codes that survive compare_schema's merge+drop below:
        # 01/43 pass through untouched, 1001/1002/1003/1004 are the synthetic codes
        # merge_job_role_columns() creates.
        source_schema = {
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
            SLVJR.job_role_code: pl.String,
        }
        source_rows = [
            ("1-001", date(2026, 1, 1), "1"),
            ("1-001", date(2026, 1, 1), "43"),
            ("1-001", date(2026, 1, 1), "1001"),
            ("1-001", date(2026, 1, 1), "1002"),
            ("1-001", date(2026, 1, 1), "1003"),
            ("1-001", date(2026, 1, 1), "1004"),
        ]  # fmt: skip
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")

        # The compare frame is the unreduced, wide, pre-transform cleaned ASCWDS data -
        # i.e. before _00_prepare.py's own merge_job_role_columns() and jr28/jr30 drop
        # run. It carries: 01/43, which pass through untouched; 02/03/35/10/25, which
        # merge_job_role_columns() merges into synthetic 1001/1002/1003/1004 and then
        # drops (merge_job_role_columns() requires at least one matching source column
        # per mapping key, hence one representative code per synthetic code rather than
        # every historic code); and jr28/jr30 "total" summary columns, dropped outright.
        # Naively counting job-role codes against this raw schema would find 9 (01, 43,
        # 02, 03, 35, 10, 25, 28, 30); the correct post-transform count _00_prepare.py's
        # pivot actually sees is 6 (01, 43, 1001, 1002, 1003, 1004) - this fixture
        # exercises exactly the class of bug found in production (raw schema of 52
        # columns wrongly counted vs. the correct 25). Dates are chosen to stay stable
        # as time passes: January always survives (quarterly sampling) and a pre-window
        # May never does. 1-003 shares January with 1-001 but has a later date, so it's
        # dropped by the earliest-file-per-month filter rather than the retention
        # filter - proving the monthly reduction itself is exercised here, not just
        # retention.
        compare_schema = {
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
            **{
                col: pl.Int32
                for code in ["01", "43", "02", "03", "35", "10", "25", "28", "30"]
                for col in (
                    f"jr{code}emp",
                    f"jr{code}strt",
                    f"jr{code}stop",
                    f"jr{code}vacy",
                )
            },
        }
        metric_values = [1] * (len(compare_schema) - 2)
        compare_rows = [
            ("1-001", date(2026, 1, 1), *metric_values),
            ("1-003", date(2026, 1, 15), *metric_values),  # same month as 1-001, later date -> dropped by monthly filter
            ("1-002", date(2020, 5, 1), *metric_values),  # before the retention window -> dropped
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

    @patch(f"{PATCH_PATH}.discover_job_role_code_count")
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.utils.discover_combined_schema")
    def test_job_role_code_count_uses_post_transform_schema_not_raw_schema(
        self,
        mock_discover_combined_schema: Mock,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
        mock_discover_job_role_code_count: Mock,
    ):
        """Regression guard: discover_job_role_code_count_after_transforms() runs
        for real here (only the leaf discover_job_role_code_count is mocked), so
        this proves main() feeds it the merged+dropped schema, not the raw
        compare_schema - the exact bug found in production."""
        mock_discover_combined_schema.return_value = self.compare_schema
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        mock_discover_job_role_code_count.return_value = 6

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        mock_discover_job_role_code_count.assert_called_once()
        called_schema = mock_discover_job_role_code_count.call_args[0][0]

        self.assertNotEqual(
            set(called_schema.names()), set(self.compare_schema.names())
        )
        self.assertEqual(
            set(called_schema.names()),
            {
                AWPClean.establishment_id,
                AWPClean.ascwds_workplace_import_date,
                "jr01emp", "jr01strt", "jr01stop", "jr01vacy",
                "jr43emp", "jr43strt", "jr43stop", "jr43vacy",
                "jr1001emp", "jr1001strt", "jr1001stop", "jr1001vacy",
                "jr1002emp", "jr1002strt", "jr1002stop", "jr1002vacy",
                "jr1003emp", "jr1003strt", "jr1003stop", "jr1003vacy",
                "jr1004emp", "jr1004strt", "jr1004stop", "jr1004vacy",
            },  # fmt: skip
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
