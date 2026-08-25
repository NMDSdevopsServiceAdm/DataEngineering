import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._08_publication._01_job_role_estimates.fargate.validate_02_clean_pub_data as job
from polars_utils.column_types import CategoricalColumnTypes
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
    PrimaryServiceType,
)

PATCH_PATH = (
    "projects._08_publication._01_job_role_estimates.fargate.validate_02_clean_pub_data"
)

TEST_BUCKET = "bucket"
TEST_SOURCE_PATH = "my/source/"
TEST_COMPARE_PATH = "my/compare/"
TEST_REPORTS_PATH = "my/reports/"

source_lf = pl.DataFrame(
    [
        (
            date(2026, 1, 1),
            PrimaryServiceType.non_residential,
            MainJobRoleLabels.care_worker,
            JobGroupLabels.direct_care,
            10.0,
        ),
        (
            date(2026, 1, 1),
            PrimaryServiceType.non_residential,
            MainJobRoleLabels.support_worker,
            JobGroupLabels.direct_care,
            10.0,
        ),
    ],  # fmt: skip
    schema={
        IndCqcColumns.cqc_location_import_date: pl.Date,
        IndCqcColumns.primary_service_type: CategoricalColumnTypes.PrimaryServiceEnumType,
        IndCqcColumns.main_job_role_clean_labelled: CategoricalColumnTypes.JobRoleCatType,
        IndCqcColumns.main_job_group_labelled: CategoricalColumnTypes.JobGroupCatType,
        IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated: pl.Float64,
    },
    orient="row",
)


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_main_runs_expected_checks(
        self,
        read_parquet_mock: Mock,
        write_reports_mock: Mock,
    ):
        read_parquet_mock.return_value = source_lf

        job.main(TEST_BUCKET, TEST_SOURCE_PATH, TEST_COMPARE_PATH, TEST_REPORTS_PATH)

        read_parquet_mock.assert_called_once_with(
            source=f"s3://{TEST_BUCKET}/{TEST_SOURCE_PATH}",
            selected_columns=job.VALIDATION_COLS_TO_IMPORT,
        )
        write_reports_mock.assert_called_once()
        validation_arg, bucket_arg, path_arg = write_reports_mock.call_args[0]
        assert bucket_arg == TEST_BUCKET
        assert path_arg == TEST_REPORTS_PATH

        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "col_schema_match",
            "col_vals_not_null",
            "col_vals_ge",
            "col_vals_in_set",
            "specially",
        }

        assert expected_assertions <= assertion_types_present
