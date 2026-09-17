import json
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.validate_00_prepare_worker as job
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_values.categorical_column_values import MainJobRoleLabels

PATCH_PATH = "projects._03_independent_cqc._02_employment_status.fargate.validate_00_prepare_worker"


class TestMain:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.source_df = pl.DataFrame({"worker_id": ["1", "2"]})
        # Row count is based on unique rows per RESHAPED_GROUP_COLUMNS, so the
        # two loc1/1-001 rows below count as one group.
        self.compare_df = pl.DataFrame(
            {
                AWKClean.location_id: ["loc1", "loc1", "loc2"],
                AWKClean.establishment_id: ["1-001", "1-001", "1-002"],
                AWKClean.ascwds_worker_import_date: [date(2026, 1, 1)] * 3,
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
                    selected_columns=job.RAW_RESHAPED_GROUP_COLUMNS,
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

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_expected_row_count_collapses_raw_roles_sharing_a_published_label(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        # Both roles are unpublished and share a job group, so they collapse to the
        # same label - should count as one group, not two.
        compare_df = pl.DataFrame(
            {
                AWKClean.location_id: ["loc1", "loc1"],
                AWKClean.establishment_id: ["1-001", "1-001"],
                AWKClean.ascwds_worker_import_date: [date(2026, 1, 1)] * 2,
                AWKClean.main_job_role_clean_labelled: [
                    MainJobRoleLabels.middle_management,
                    MainJobRoleLabels.first_line_manager,
                ],
            }
        )
        mock_read_parquet.side_effect = [self.source_df, compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_match_entry = next(
            item for item in report_json if item["assertion_type"] == "row_count_match"
        )

        assert row_count_match_entry["values"]["count"] == 1
