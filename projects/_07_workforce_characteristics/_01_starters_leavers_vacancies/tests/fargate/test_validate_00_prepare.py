import json
from unittest.mock import Mock, call, patch

import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_job_roles import (
    AscwdsWorkplaceJobRolesColumns as AWPJobRoles,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare"


def _source_and_compare_dfs():
    compare_df = pl.DataFrame(
        {AWPClean.establishment_id: ["1-001", "1-002"]},
    )
    source_df = pl.DataFrame(
        {
            AWPClean.establishment_id: ["1-001", "1-001", "1-002", "1-002"],
            AWPJobRoles.job_role_code: ["1", "2", "1", "2"],
        },
    )
    return source_df, compare_df


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_reads_source_and_compare_datasets(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        source_df, compare_df = _source_and_compare_dfs()
        mock_read_parquet.side_effect = [source_df, compare_df]

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
    def test_expects_row_count_multiplied_by_distinct_job_role_code_count(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        source_df, compare_df = _source_and_compare_dfs()
        mock_read_parquet.side_effect = [source_df, compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_match_step = next(
            step for step in report_json if step["assertion_type"] == "row_count_match"
        )

        # 2 input rows (establishments) x 2 distinct job_role_code values = 4
        assert row_count_match_step["values"]["count"] == 4

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        source_df, compare_df = _source_and_compare_dfs()
        mock_read_parquet.side_effect = [source_df, compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        assert "row_count_match" in assertion_types_present
