import json
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._01_ingest.capacity_tracker.fargate.validate_clean_capacity_tracker_non_res_data as job
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResCleanColumns as CTNRClean,
)

PATCH_PATH = "projects._01_ingest.capacity_tracker.fargate.validate_clean_capacity_tracker_non_res_data"

RAW_DF = pl.DataFrame({CTNRClean.cqc_id: ["1-001", "1-002"]})

CLEANED_DF = pl.DataFrame(
    {
        CTNRClean.cqc_id: ["1-001", "1-002"],
        CTNRClean.ct_non_res_import_date: ["20240101", "20240101"],
        CTNRClean.cqc_care_workers_employed: [5, 10],
        CTNRClean.service_user_count: [10, 20],
    }
)


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_reads_source_and_compare_datasets(
        self, read_parquet_mock: Mock, write_reports_mock: Mock
    ):
        read_parquet_mock.side_effect = [CLEANED_DF, RAW_DF]

        job.main("bucket", "my/cleaned/", "my/reports/", "my/raw/")

        read_parquet_mock.assert_has_calls(
            [
                call("s3://bucket/my/cleaned/", exclude_complex_types=True),
                call("s3://bucket/my/raw/", selected_columns=ANY),
            ]
        )
        write_reports_mock.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_expected_row_count_matches_raw_row_count(
        self, read_parquet_mock: Mock, write_reports_mock: Mock
    ):
        read_parquet_mock.side_effect = [CLEANED_DF, RAW_DF]

        job.main("bucket", "my/cleaned/", "my/reports/", "my/raw/")

        validation_arg = write_reports_mock.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_step = next(
            step for step in report_json if step["assertion_type"] == "row_count_match"
        )
        assert row_count_step["values"]["count"] == 2

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self, read_parquet_mock: Mock, write_reports_mock: Mock
    ):
        read_parquet_mock.side_effect = [CLEANED_DF, RAW_DF]

        job.main("bucket", "my/cleaned/", "my/reports/", "my/raw/")

        validation_arg = write_reports_mock.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "row_count_match",
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_between",
        }
        for assertion in expected_assertions:
            assert assertion in assertion_types_present

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_null_values_pass_the_bound_checks(
        self, read_parquet_mock: Mock, write_reports_mock: Mock
    ):
        # The clean job nulls out-of-range values rather than dropping the row,
        # so a null here should not itself be reported as a failing value.
        cleaned_with_nulls_df = CLEANED_DF.with_columns(
            pl.Series(CTNRClean.cqc_care_workers_employed, [None, 10]),
            pl.Series(CTNRClean.service_user_count, [10, None]),
        )
        read_parquet_mock.side_effect = [cleaned_with_nulls_df, RAW_DF]

        job.main("bucket", "my/cleaned/", "my/reports/", "my/raw/")

        validation_arg = write_reports_mock.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        between_steps = [
            step for step in report_json if step["assertion_type"] == "col_vals_between"
        ]
        for step in between_steps:
            assert step["n_failed"] == 0
