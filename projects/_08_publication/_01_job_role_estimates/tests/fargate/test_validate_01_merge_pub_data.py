import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._08_publication._01_job_role_estimates.fargate.validate_01_merge_pub_data as job
from polars_utils.column_types import CategoricalColumnTypes
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns, PartitionKeys
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
    PrimaryServiceType,
)

PATCH_PATH = (
    "projects._08_publication._01_job_role_estimates.fargate.validate_01_merge_pub_data"
)

TEST_BUCKET = "bucket"
TEST_SOURCE_PATH = "my/source/"
TEST_COMPARE_PATH = "my/compare/"
TEST_REPORTS_PATH = "my/reports/"

index_source_lf = pl.DataFrame(
    [
        (1, "1", "1-001", date(2026, 1, 1), MainJobRoleLabels.care_worker),
        (2, "2", "1-002", date(2026, 1, 1), MainJobRoleLabels.support_worker),
    ],
    schema={
        IndCqcColumns.id_per_locationid_import_date: pl.UInt32,
        IndCqcColumns.id_per_locationid_import_date_job_role: pl.String,
        IndCqcColumns.location_id: CategoricalColumnTypes.LocationCatType,
        IndCqcColumns.cqc_location_import_date: pl.Date,
        IndCqcColumns.main_job_role_clean_labelled: CategoricalColumnTypes.JobRoleCatType,
    },
    orient="row",
)

other_source_lf = pl.DataFrame(
    [
        (
            "1",
            "1-001",
            date(2026, 1, 1),
            PrimaryServiceType.non_residential,
            1,
            MainJobRoleLabels.care_worker,
            10.0,
            JobGroupLabels.direct_care,
            2026,
        ),
        (
            "2",
            "1-002",
            date(2026, 1, 1),
            PrimaryServiceType.non_residential,
            2,
            MainJobRoleLabels.support_worker,
            10.0,
            JobGroupLabels.direct_care,
            2026,
        ),
    ],  # fmt: skip
    schema={
        IndCqcColumns.id_per_locationid_import_date_job_role: pl.String,
        IndCqcColumns.location_id: CategoricalColumnTypes.LocationCatType,
        IndCqcColumns.cqc_location_import_date: pl.Date,
        IndCqcColumns.primary_service_type: CategoricalColumnTypes.PrimaryServiceEnumType,
        IndCqcColumns.id_per_locationid_import_date: pl.UInt32,
        IndCqcColumns.main_job_role_clean_labelled: CategoricalColumnTypes.JobRoleCatType,
        IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated: pl.Float64,
        IndCqcColumns.main_job_group_labelled: CategoricalColumnTypes.JobGroupCatType,
        PartitionKeys.year: pl.Int64,
    },
    orient="row",
)

compare_lf = pl.DataFrame(
    [
        ("1-001", date(2026, 1, 1)),
        ("1-002", date(2026, 1, 1)),
    ],
    schema={
        IndCqcColumns.location_id: pl.String,
        IndCqcColumns.cqc_location_import_date: pl.Date,
    },
    orient="row",
)


class TestMain:
    @patch(f"{PATCH_PATH}.other_validation")
    @patch(f"{PATCH_PATH}.index_validation")
    def test_main_runs_index_and_other_validation(
        self,
        index_validation_mock: Mock,
        other_validation_mock: Mock,
    ):
        job.main(TEST_BUCKET, TEST_SOURCE_PATH, TEST_COMPARE_PATH, TEST_REPORTS_PATH)

        index_validation_mock.assert_called_once_with(
            TEST_BUCKET, TEST_SOURCE_PATH, TEST_REPORTS_PATH
        )
        other_validation_mock.assert_called_once_with(
            TEST_BUCKET, TEST_SOURCE_PATH, TEST_COMPARE_PATH, TEST_REPORTS_PATH
        )


class TestIndexValidation:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_index_validation_runs_expected_checks(
        self,
        read_parquet_mock: Mock,
        write_reports_mock: Mock,
    ):
        read_parquet_mock.return_value = index_source_lf

        job.index_validation(TEST_BUCKET, TEST_SOURCE_PATH, TEST_REPORTS_PATH)

        read_parquet_mock.assert_called_once_with(
            source=f"s3://{TEST_BUCKET}/{TEST_SOURCE_PATH}",
            selected_columns=job.INDEX_VALIDATION_COLS_TO_IMPORT,
        )
        write_reports_mock.assert_called_once()
        validation_arg, bucket_arg, path_arg = write_reports_mock.call_args[0]
        assert bucket_arg == TEST_BUCKET
        assert path_arg == f"{TEST_REPORTS_PATH}index_validation/"

        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        assert {"rows_distinct", "col_vals_expr"} <= assertion_types_present


class TestOtherValidation:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_other_validation_runs_expected_checks(
        self,
        read_parquet_mock: Mock,
        write_reports_mock: Mock,
    ):
        read_parquet_mock.side_effect = [other_source_lf, compare_lf]

        job.other_validation(
            TEST_BUCKET, TEST_SOURCE_PATH, TEST_COMPARE_PATH, TEST_REPORTS_PATH
        )

        assert read_parquet_mock.call_count == 2
        read_parquet_mock.assert_any_call(
            source=f"s3://{TEST_BUCKET}/{TEST_SOURCE_PATH}",
            selected_columns=job.OTHER_VALIDATION_COLS_TO_IMPORT,
        )
        read_parquet_mock.assert_any_call(
            source=f"s3://{TEST_BUCKET}/{TEST_COMPARE_PATH}",
            selected_columns=job.COMPARE_COLS_TO_IMPORT,
        )
        write_reports_mock.assert_called_once()
        validation_arg, bucket_arg, path_arg = write_reports_mock.call_args[0]
        assert bucket_arg == TEST_BUCKET
        assert path_arg == f"{TEST_REPORTS_PATH}other_validation/"

        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "col_schema_match",
            "row_count_match",
            "col_vals_not_null",
            "col_vals_in_set",
            "specially",
        }

        assert expected_assertions <= assertion_types_present
