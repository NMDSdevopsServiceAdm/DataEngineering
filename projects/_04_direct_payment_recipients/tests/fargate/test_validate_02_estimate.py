import json
from dataclasses import dataclass
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._04_direct_payment_recipients.fargate.validate_02_estimate as job
from projects._04_direct_payment_recipients.direct_payments_config import (
    DirectPaymentConfiguration as Config,
)
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

PATCH_PATH = "projects._04_direct_payment_recipients.fargate.validate_02_estimate"


@dataclass
class Schemas:
    merged_schema = pl.Schema(
        [
            (DP.la_area, pl.String),
            (DP.year, pl.String),
            (DP.year_as_integer, pl.Int64),
            (DP.service_user_dprs_during_year, pl.Float32),
            (DP.proportion_employing_staff, pl.Float32),
            (DP.historic_service_users_employing_staff_estimate, pl.Float32),
            (DP.total_dprs_during_year, pl.Float32),
            (DP.filled_posts_per_employer, pl.Float32),
        ]
    )
    estimates_schema = pl.Schema(
        [
            *merged_schema.items(),
            (DP.estimate_using_mean, pl.Float32),
            (DP.first_year_with_data, pl.Int32),
            (DP.last_year_with_data, pl.Int32),
            (DP.estimate_using_extrapolation_ratio, pl.Float32),
            (DP.estimate_using_interpolation, pl.Float32),
            (DP.imputed_proportion_employing_staff, pl.Float32),
            (
                DP.imputed_proportion_employing_staff_source,
                pl.String,
            ),
            (
                DP.rolling_average_proportion_employing_staff,
                pl.Float32,
            ),
            (
                DP.estimated_service_users_employing_staff,
                pl.Float32,
            ),
            (DP.estimated_service_users_employing_self_employed_staff, pl.Float32),
            (DP.estimated_total_dpr_employing_staff, pl.Float32),
            (DP.estimated_pa_filled_posts, pl.Float32),
            (DP.estimated_proportion_of_total_dpr_employing_staff, pl.Float32),
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
            DP.proportion_employing_staff,
            0.5,
            5.0,
            0.2,
            5.2,
            9.88,
            0.26,
        )
    ]


class TestMain:
    source_df = pl.DataFrame(
        data=Data.estimates_rows,
        schema=Schemas.estimates_schema,
        orient="row",
    )
    compare_df = pl.DataFrame(
        data=Data.merged_rows,
        schema=Schemas.merged_schema,
        orient="row",
    )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(self, mock_read_parquet: Mock, mock_write_reports: Mock):
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
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "row_count_match",
            "col_vals_not_null",
            "col_vals_in_set",
            "rows_distinct",
            "specially",
        }
        for assertion in expected_assertions:
            assert (
                assertion in assertion_types_present
            ), f"{assertion} not found in validation report"

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_first_and_last_year_with_data_accepts_the_dataset_s_own_earliest_year(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        # The dataset's own earliest year must be accepted, even though it
        # predates the years with known proportions.
        source_df = self.source_df.with_columns(
            pl.lit(Config.FIRST_YEAR, dtype=pl.Int32).alias(DP.first_year_with_data),
            pl.lit(Config.FIRST_YEAR, dtype=pl.Int32).alias(DP.last_year_with_data),
        )
        mock_read_parquet.side_effect = [source_df, self.compare_df]

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        year_bound_steps = [
            item
            for item in report_json
            if item["assertion_type"] == "col_vals_between"
            and item["column"] in (DP.first_year_with_data, DP.last_year_with_data)
        ]

        assert len(year_bound_steps) == 2
        assert all(step["all_passed"] for step in year_bound_steps)

    @pytest.mark.parametrize("year", [2011, 2014])
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_estimate_checks_pass_for_nulls_and_unbounded_values_before_2015(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
        year: int,
    ):
        early_year_df = self.source_df.with_columns(
            pl.lit(year).alias(DP.year_as_integer),
            pl.lit(None, dtype=pl.Float32).alias(DP.imputed_proportion_employing_staff),
            pl.lit(None, dtype=pl.Float32).alias(
                DP.rolling_average_proportion_employing_staff
            ),
            pl.lit(None, dtype=pl.String).alias(
                DP.imputed_proportion_employing_staff_source
            ),
            pl.lit(1.5, dtype=pl.Float32).alias(DP.estimate_using_interpolation),
        )
        source_df = pl.concat([early_year_df, self.source_df], how="vertical_relaxed")
        mock_read_parquet.side_effect = [source_df, self.compare_df]

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        estimate_columns = (
            DP.imputed_proportion_employing_staff,
            DP.rolling_average_proportion_employing_staff,
            DP.imputed_proportion_employing_staff_source,
            DP.estimate_using_interpolation,
        )
        estimate_steps = [
            item for item in report_json if item["column"] in estimate_columns
        ]

        assert len(estimate_steps) == len(estimate_columns)
        assert all(step["all_passed"] for step in estimate_steps)

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_estimate_checks_fail_for_nulls_and_unbounded_values_from_2015(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        recent_year_df = self.source_df.with_columns(
            pl.lit(2015).alias(DP.year_as_integer),
            pl.lit(None, dtype=pl.Float32).alias(DP.imputed_proportion_employing_staff),
            pl.lit(None, dtype=pl.Float32).alias(
                DP.rolling_average_proportion_employing_staff
            ),
            pl.lit(None, dtype=pl.String).alias(
                DP.imputed_proportion_employing_staff_source
            ),
            pl.lit(1.5, dtype=pl.Float32).alias(DP.estimate_using_interpolation),
        )
        mock_read_parquet.side_effect = [recent_year_df, self.compare_df]

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        estimate_columns = (
            DP.imputed_proportion_employing_staff,
            DP.rolling_average_proportion_employing_staff,
            DP.imputed_proportion_employing_staff_source,
            DP.estimate_using_interpolation,
        )
        estimate_steps = [
            item for item in report_json if item["column"] in estimate_columns
        ]

        assert len(estimate_steps) == len(estimate_columns)
        assert not any(step["all_passed"] for step in estimate_steps)
