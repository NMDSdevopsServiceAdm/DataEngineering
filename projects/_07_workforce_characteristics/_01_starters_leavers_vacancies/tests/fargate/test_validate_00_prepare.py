import json
import unittest
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare"


class ValidatePreparedSLVDataTests(unittest.TestCase):
    def setUp(self) -> None:
        source_schema = {
            AWPClean.establishment_id: pl.String,
        }
        source_rows = [
            ("1-001"),
        ]  # fmt: skip
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")

        # The compare frame is the unreduced cleaned ASCWDS data, so it carries rows
        # the reduction filters drop. Dates are chosen to stay stable as time passes:
        # January always survives (quarterly sampling) and a pre-window May never does.
        # 1-003 shares January with 1-001 but has a later date, so it's dropped by the
        # earliest-file-per-month filter rather than the retention filter - proving the
        # monthly reduction itself is exercised here, not just retention.
        compare_schema = {
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
        }
        compare_rows = [
            ("1-001", date(2026, 1, 1)),
            ("1-003", date(2026, 1, 15)),  # same month as 1-001, later date -> dropped by monthly filter
            ("1-002", date(2020, 5, 1)),
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

        self.assertEqual(mock_read_parquet.call_count, 2)
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
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


class TestHasAllPublishedJobRoleLabelColumns:
    def test_true_when_every_published_label_has_a_column(self):
        columns = [f"{label}_emp" for label in job.PUBLISHED_JOB_ROLE_LABELS]
        df = pl.DataFrame(schema={col: pl.Int64 for col in columns})

        assert job.has_all_published_job_role_label_columns(df)

    def test_false_when_missing_label_shares_a_prefix_with_present_siblings(self):
        # "other", "other_managers", "other_regulated_professions", and
        # "other_direct_care" all share the "other_" prefix - pins that a
        # missing "other" column isn't masked by its siblings being present.
        present_labels = [
            label for label in job.PUBLISHED_JOB_ROLE_LABELS if label != "other"
        ]
        df = pl.DataFrame(schema={f"{label}_emp": pl.Int64 for label in present_labels})

        assert not job.has_all_published_job_role_label_columns(df)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
