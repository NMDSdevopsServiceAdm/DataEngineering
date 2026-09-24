import json
from unittest.mock import Mock, patch

import polars as pl

import projects._03_independent_cqc._01_filled_posts._04_model.fargate.validate_model_01_features as job
from projects._03_independent_cqc._01_filled_posts.unittest_data.polars_ind_cqc_test_file_data import (
    ValidateModel01FeaturesData as Data,
)
from projects._03_independent_cqc._01_filled_posts.unittest_data.polars_ind_cqc_test_file_schemas import (
    ValidateModel01FeaturesSchemas as Schemas,
)

PATCH_PATH = "projects._03_independent_cqc._01_filled_posts._04_model.fargate.validate_model_01_features"


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.get_expected_row_count_for_model_features")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        read_parquet_mock: Mock,
        scan_parquet_mock: Mock,
        get_expected_row_count_mock: Mock,
        write_reports_mock: Mock,
    ):
        read_parquet_mock.return_value = pl.DataFrame(
            data=Data.validation_rows,
            schema=Schemas.validation_schema,
            strict=False,
            orient="row",
        )
        get_expected_row_count_mock.return_value = (
            Data.expected_get_expected_row_count_rows
        )

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/", "model")

        read_parquet_mock.assert_called_once_with(
            "s3://bucket/my/dataset/", exclude_complex_types=False
        )
        scan_parquet_mock.assert_called_once_with("s3://bucket/other/dataset/")
        get_expected_row_count_mock.assert_called_once_with(
            scan_parquet_mock.return_value, "model"
        )
        write_reports_mock.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.get_expected_row_count_for_model_features")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        read_parquet_mock: Mock,
        scan_parquet_mock: Mock,
        get_expected_row_count_mock: Mock,
        write_reports_mock: Mock,
    ):
        read_parquet_mock.return_value = pl.DataFrame(
            data=Data.validation_rows,
            schema=Schemas.validation_schema,
            strict=False,
            orient="row",
        )
        get_expected_row_count_mock.return_value = (
            Data.expected_get_expected_row_count_rows
        )

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/", "model")

        validation_arg = write_reports_mock.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}
        expected_assertions = {
            "row_count_match",
            "col_vals_not_null",
            "col_exists",
            "rows_distinct",
            "col_vals_between",
        }
        missing_assertions = expected_assertions - assertion_types_present
        assert not missing_assertions, f"{missing_assertions} not in validation report"
