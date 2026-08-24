import json
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._01_ingest.capacity_tracker.fargate.validate_clean_capacity_tracker_care_home_data as job
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
)

PATCH_PATH = "projects._01_ingest.capacity_tracker.fargate.validate_clean_capacity_tracker_care_home_data"

RAW_DF = pl.DataFrame(
    {
        CTCH.nurses_employed: [1, 2],
        CTCH.agency_nurses_employed: [1, 9],  # row 0 matches -> excluded from expected
        CTCH.care_workers_employed: [1, 1],
        CTCH.agency_care_workers_employed: [1, 1],
        CTCH.non_care_workers_employed: [1, 1],
        CTCH.agency_non_care_workers_employed: [1, 1],
    }
)

CLEANED_DF = pl.DataFrame(
    {
        CTCHClean.cqc_id: ["1-001"],
        CTCHClean.ct_care_home_import_date: ["20240101"],
        CTCHClean.nurses_employed: [2],
        CTCHClean.care_workers_employed: [1],
        CTCHClean.non_care_workers_employed: [1],
        CTCHClean.agency_nurses_employed: [9],
        CTCHClean.agency_care_workers_employed: [1],
        CTCHClean.agency_non_care_workers_employed: [1],
        CTCHClean.non_agency_total_employed: [4],
        CTCHClean.agency_total_employed: [11],
        CTCHClean.ct_care_home_total_employed: [15],
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
    def test_expected_row_count_excludes_rows_where_values_match(
        self, read_parquet_mock: Mock, write_reports_mock: Mock
    ):
        read_parquet_mock.side_effect = [CLEANED_DF, RAW_DF]

        job.main("bucket", "my/cleaned/", "my/reports/", "my/raw/")

        validation_arg = write_reports_mock.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_step = next(
            step for step in report_json if step["assertion_type"] == "row_count_match"
        )
        assert row_count_step["values"]["count"] == 1

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
