import json
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._03_independent_cqc._03_impute.fargate.validate_imputed_ind_cqc_ascwds_and_pir_data as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ValidateImputedIndCqcAscwdsAndPir as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ValidateImputedIndCqcAscwdsAndPir as Schemas,
)

PATCH_PATH = "projects._03_independent_cqc._03_impute.fargate.validate_imputed_ind_cqc_ascwds_and_pir_data"


@pytest.fixture
def source_df() -> pl.DataFrame:
    return pl.DataFrame(
        Data.imputed_ind_cqc_ascwds_and_pir_rows,
        Schemas.imputed_ind_cqc_ascwds_and_pir_schema,
        strict=False,
        orient="row",
    )


@pytest.fixture
def compare_df() -> pl.DataFrame:
    return pl.DataFrame(
        Data.cleaned_ind_cqc_rows,
        Schemas.cleaned_ind_cqc_schema,
        strict=False,
        orient="row",
    )


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        read_parquet_mock: Mock,
        write_reports_mock: Mock,
        source_df: pl.DataFrame,
        compare_df: pl.DataFrame,
    ):
        read_parquet_mock.side_effect = [source_df, compare_df]

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        read_parquet_mock.assert_has_calls(
            [
                call("s3://bucket/my/dataset/", exclude_complex_types=True),
                call(
                    "s3://bucket/other/dataset/",
                    selected_columns=job.cleaned_ind_cqc_columns_to_import,
                ),
            ]
        )
        write_reports_mock.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        read_parquet_mock: Mock,
        write_reports_mock: Mock,
        source_df: pl.DataFrame,
        compare_df: pl.DataFrame,
    ):
        read_parquet_mock.side_effect = [source_df, compare_df]

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        validation_arg = write_reports_mock.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "row_count_match",
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_between",
            "col_vals_in_set",
            "col_vals_expr",
            "specially",
        }

        assert expected_assertions <= assertion_types_present
