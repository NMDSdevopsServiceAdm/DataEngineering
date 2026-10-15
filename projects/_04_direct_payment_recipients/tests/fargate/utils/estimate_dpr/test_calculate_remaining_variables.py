import unittest

import polars as pl
import polars.testing as pl_testing

import projects._04_direct_payment_recipients.fargate.utils.estimate_dpr.calculate_remaining_variables as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestCalculateRemainingVariables(unittest.TestCase):
    def test_calculate_remaining_variables_returns_expected_values(self):
        schema = {
            DP.la_area: pl.String,
            DP.year_as_integer: pl.Int32,
            DP.service_user_dprs_during_year: pl.Float32,
            DP.total_dprs_during_year: pl.Float32,
            DP.estimated_service_users_employing_staff: pl.Float32,
            DP.filled_posts_per_employer: pl.Float32,
            DP.estimated_service_users_employing_self_employed_staff: pl.Float32,
            DP.estimated_total_dpr_employing_staff: pl.Float32,
            DP.estimated_pa_filled_posts: pl.Float32,
            DP.estimated_proportion_of_total_dpr_employing_staff: pl.Float32,
        }

        rows = [
            ("Area A", 2022, 100.0, 200.0, 30.0, 2.0, 1.7948, 31.7949, 63.5898, 0.1590),
            ("Area B", 2022, 50.0, 100.0, 10.0, 1.5, 0.8974, 10.8975, 16.34625, 0.1090),
        ] # fmt: skip

        expected_lf = pl.LazyFrame(rows, schema, orient="row")
        test_lf = expected_lf.drop(
            DP.estimated_service_users_employing_self_employed_staff,
            DP.estimated_total_dpr_employing_staff,
            DP.estimated_pa_filled_posts,
            DP.estimated_proportion_of_total_dpr_employing_staff,
        )
        returned_lf = job.calculate_remaining_variables(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, abs_tol=1e-4)
