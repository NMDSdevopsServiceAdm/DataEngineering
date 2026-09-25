import json
from unittest.mock import Mock, call, patch

import polars as pl

import projects._02_sfc_internal._01_cqc_ratings.fargate.validate_flatten_cqc_ratings as job
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

PATCH_PATH = (
    "projects._02_sfc_internal._01_cqc_ratings.fargate.validate_flatten_cqc_ratings"
)

valid_rows = [
    {
        CQCL.location_id: "1-001",
        CQCL.registration_status: "Registered",
        CQCRatings.date: "2024-01-01",
        CQCL.assessment_plan_id: None,
        CQCL.name: "Care Homes",
        CQCL.source_path: None,
        CQCL.dataset: "Pre SAF",
        CQCRatings.current_or_historic: "Current",
        CQCRatings.latest_rating_flag: 1,
        CQCRatings.overall_rating: "Good",
        CQCRatings.safe_rating: "Good",
        CQCRatings.well_led_rating: "Good",
        CQCRatings.caring_rating: "Good",
        CQCRatings.responsive_rating: "Good",
        CQCRatings.effective_rating: "Good",
        CQCRatings.overall_rating_value: 3,
        CQCRatings.safe_rating_value: 3,
        CQCRatings.well_led_rating_value: 3,
        CQCRatings.caring_rating_value: 3,
        CQCRatings.responsive_rating_value: 3,
        CQCRatings.effective_rating_value: 3,
        CQCRatings.total_rating_value: 15,
    },
]


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_main_reads_validates_and_writes_report(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = pl.DataFrame(valid_rows)

        job.main("bucket", "my/dataset/", "my/reports/")

        mock_read_parquet.assert_called_once_with("s3://bucket/my/dataset/")
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_checks(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = pl.DataFrame(valid_rows)

        job.main("bucket", "my/dataset/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_in_set",
            "col_vals_between",
        }
        for assertion in expected_assertions:
            assert (
                assertion in assertion_types_present
            ), f"{assertion} not found in validation report"
