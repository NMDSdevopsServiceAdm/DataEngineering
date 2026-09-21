import json
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.validate_00_prepare_workplace as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)
from utils.column_values.categorical_columns_by_dataset import (
    SLVPrepareCategoricalValues,
)

PATCH_PATH = "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.validate_00_prepare_workplace"


class TestMain:
    METADATA_SOURCE = "my/metadata/"

    @pytest.fixture(autouse=True)
    def setup(self):
        source_schema = {
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
            IndCQC.published_job_role_label: pl.String,
            EmpStatus.employee_count: pl.Int64,
            SLVCols.starters: pl.Int64,
            SLVCols.leavers: pl.Int64,
            SLVCols.vacancies: pl.Int64,
        }
        source_rows = [
            ("1-001", date(2026, 1, 1), label, 1, 1, 1, 1)
            for label in SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
        ]
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")

        # The compare frame is the unreduced cleaned ASCWDS data, so it carries rows
        # the metadata-matched-dates filter drops. 1-001 sits on the one date metadata
        # is matched to; 1-002 and 1-003 sit on dates metadata never matched, so they
        # must not count towards the expected row total.
        compare_schema = {
            AWPClean.location_id: pl.String,
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
        }
        compare_rows = [
            ("Loc-001", "1-001", date(2026, 1, 1)),
            ("Loc-002", "1-002", date(2026, 1, 15)),
            ("Loc-003", "1-003", date(2020, 5, 1)),
        ]
        self.compare_df = pl.DataFrame(compare_rows, compare_schema, orient="row")

        self.matched_dates = pl.Series(
            IndCQC.ascwds_workplace_import_date, [date(2026, 1, 1)]
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.get_matched_ascwds_dates")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_get_matched_ascwds_dates: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        mock_get_matched_ascwds_dates.return_value = self.matched_dates

        job.main(
            "bucket", "my/source/", "my/compare/", self.METADATA_SOURCE, "my/reports/"
        )

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
        mock_get_matched_ascwds_dates.assert_called_once_with(
            self.METADATA_SOURCE, IndCQC.ascwds_workplace_import_date
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.get_matched_ascwds_dates")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_get_matched_ascwds_dates: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        mock_get_matched_ascwds_dates.return_value = self.matched_dates

        job.main(
            "bucket", "my/source/", "my/compare/", self.METADATA_SOURCE, "my/reports/"
        )

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
    @patch(f"{PATCH_PATH}.get_matched_ascwds_dates")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_expected_row_count_uses_dates_matched_in_metadata(
        self,
        mock_read_parquet: Mock,
        mock_get_matched_ascwds_dates: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        mock_get_matched_ascwds_dates.return_value = self.matched_dates

        job.main(
            "bucket", "my/source/", "my/compare/", self.METADATA_SOURCE, "my/reports/"
        )

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_match_entry = next(
            item for item in report_json if item["assertion_type"] == "row_count_match"
        )

        # Only 1-001 (2026-1-1) sits on a metadata-matched date, so one compare row
        # explodes into one row per published job role label.
        expected_row_count = len(
            SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
        )
        assert row_count_match_entry["values"]["count"] == expected_row_count
        assert row_count_match_entry["all_passed"] is True
