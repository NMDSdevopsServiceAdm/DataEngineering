import json
import unittest
from dataclasses import dataclass
from unittest.mock import Mock, call, patch

import polars as pl

import projects._04_direct_payment_recipients.fargate.validate_estimate_direct_payments as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

PATCH_PATH = (
    "projects._04_direct_payment_recipients.fargate.validate_estimate_direct_payments"
)


@dataclass
class Schemas:
    merged_schema = pl.Schema(
        [
            (DP.LA_AREA, pl.String),
            (DP.YEAR, pl.String),
            (DP.YEAR_AS_INTEGER, pl.Int64),
            (DP.SERVICE_USER_DPRS_DURING_YEAR, pl.Float32),
            (DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, pl.Float32),
            (DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE, pl.Float32),
            (DP.TOTAL_DPRS_DURING_YEAR, pl.Float32),
            (DP.FILLED_POSTS_PER_EMPLOYER, pl.Float32),
        ]
    )
    estimates_schema = pl.Schema(
        [
            *merged_schema.items(),
            (DP.ESTIMATE_USING_MEAN, pl.Float32),
            (DP.FIRST_YEAR_WITH_DATA, pl.Int32),
            (DP.LAST_YEAR_WITH_DATA, pl.Int32),
            (DP.ESTIMATE_USING_EXTRAPOLATION_RATIO, pl.Float32),
            (DP.ESTIMATE_USING_INTERPOLATION, pl.Float32),
            (DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, pl.Float32),
            (
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE,
                pl.String,
            ),
            (
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                pl.Float32,
            ),
            (
                DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
                pl.Float32,
            ),
            (DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF, pl.Float32),
            (DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF, pl.Float32),
            (DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS, pl.Float32),
            (DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF, pl.Float32),
        ]
    )


@dataclass
class Data:
    merged_rows = [("area", "2020", 2020, 10.0, 0.5, 0.5, 20.0, 1.9)]
    estimates_rows = [
        (
            "area",
            "2020",
            2020,
            10.0,
            0.5,
            0.5,
            20.0,
            1.9,
            0.5,
            2020,
            2020,
            None,
            0.5,
            0.5,
            DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
            0.5,
            5.0,
            0.2,
            5.2,
            9.88,
            0.26,
        )
    ]


class ValidateEstimateDirectPaymentsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_df = pl.DataFrame(
            data=Data.estimates_rows,
            schema=Schemas.estimates_schema,
            orient="row",
        )
        self.compare_df = pl.DataFrame(
            data=Data.merged_rows,
            schema=Schemas.merged_schema,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        mock_read_parquet.assert_has_calls(
            [
                call("s3://bucket/my/dataset/", exclude_complex_types=True),
                call("s3://bucket/other/dataset/"),
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
        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        # Extract all assertion types present in the report
        assertion_types_present = {item["assertion_type"] for item in report_json}

        # Check that key validations were run
        expected_assertions = {
            "row_count_match",
            "col_vals_not_null",
            "col_vals_in_set",
            "specially",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )
