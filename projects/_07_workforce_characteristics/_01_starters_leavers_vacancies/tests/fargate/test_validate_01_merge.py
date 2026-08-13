import json
from dataclasses import dataclass
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_01_merge as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols
from utils.column_values.categorical_columns_by_dataset import (
    SLVPrepareCategoricalValues,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_01_merge"

PUBLISHED_ROLE_COUNT = len(
    SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
)


@dataclass
class CalculateExpectedRowCount:
    id: str
    input_data: list[int]
    expected_row_count: int


calculate_expected_row_count_test_cases = [
    CalculateExpectedRowCount(
        id="single_group_multiple_granular_roles",
        input_data=[1, 1],  # list of id's
        expected_row_count=PUBLISHED_ROLE_COUNT,
    ),
    CalculateExpectedRowCount(
        id="multiple_groups_counted_once_each",
        input_data=[1, 1, 2],  # list of id's
        expected_row_count=PUBLISHED_ROLE_COUNT * 2,
    ),
]


class TestCalculateExpectedRowCount:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in calculate_expected_row_count_test_cases
        ],
    )
    def test_multiplies_distinct_groups_by_published_role_count(self, case):
        compare_df = pl.DataFrame(
            {IndCqcColumns.id_per_locationid_import_date: case.input_data}
        )

        returned_row_count = job.calculate_expected_row_count(compare_df)

        assert returned_row_count == case.expected_row_count


class TestMain:
    @pytest.fixture(autouse=True)
    def _set_up_dataframes(self):
        self.compare_df = pl.DataFrame(
            {IndCqcColumns.id_per_locationid_import_date: [1, 1]}
        )
        self.source_df = pl.DataFrame(
            {
                IndCqcColumns.location_id: ["1-001"] * PUBLISHED_ROLE_COUNT,
                job.METRIC: [10.0] * PUBLISHED_ROLE_COUNT,
                SLVCols.filled_posts_perm: [5.0] * PUBLISHED_ROLE_COUNT,
                SLVCols.filled_posts_temp: [2.0] * PUBLISHED_ROLE_COUNT,
                SLVCols.filled_posts_bank_or_pool: [1.5] * PUBLISHED_ROLE_COUNT,
                SLVCols.filled_posts_agency: [1.0] * PUBLISHED_ROLE_COUNT,
                SLVCols.filled_posts_other: [0.5] * PUBLISHED_ROLE_COUNT,
            }
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_reads_source_and_compare_with_expected_columns(
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

        assert "row_count_match" in assertion_types_present
        assert "col_vals_ge" in assertion_types_present
        assert "col_vals_expr" in assertion_types_present
