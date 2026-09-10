import json
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare_worker as job
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_values.categorical_column_values import MainJobRoleLabels

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare_worker"


class TestMain:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.source_df = pl.DataFrame({"worker_id": ["1", "2"]})
        # Two raw rows for loc1/1-001 share every RESHAPED_GROUP_COLUMNS value
        # (they'd only have differed by employment status, which is no longer
        # part of the compare columns since it's pivoted to columns, not rows,
        # in the prepared output), so they count as one group, not two.
        self.compare_df = pl.DataFrame(
            {
                AWKClean.location_id: ["loc1", "loc1", "loc2"],
                AWKClean.establishment_id: ["1-001", "1-001", "1-002"],
                AWKClean.ascwds_worker_import_date: ["2026-01-01"] * 3,
                AWKClean.main_job_role_clean_labelled: [MainJobRoleLabels.care_worker]
                * 3,
            }
        )

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
    def test_validation_report_includes_row_count_match(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        assert "row_count_match" in assertion_types_present

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_row_count_match_expects_unique_group_count_not_raw_compare_row_count(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_match_entry = next(
            item for item in report_json if item["assertion_type"] == "row_count_match"
        )

        assert row_count_match_entry["values"]["count"] == 2
        assert row_count_match_entry["all_passed"] is True
