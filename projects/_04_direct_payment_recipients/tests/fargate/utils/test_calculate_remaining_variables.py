import unittest
import polars as pl
import polars.testing as pl_testing

import projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.calculate_remaining_variables as job
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestCalculateRemainingVariables(unittest.TestCase):
    def test_function_retuns_expected_values(self):
        schema = {
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int32,
            DP.SERVICE_USER_DPRS_DURING_YEAR: pl.Float32,
            # DP.CARER_DPRS_DURING_YEAR: pl.Float32,
            DP.TOTAL_DPRS_DURING_YEAR: pl.Float32,
            DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF: pl.Float32,
            DP.FILLED_POSTS_PER_EMPLOYER: pl.Float32,
            DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF: pl.Float32,
            # DP.ESTIMATED_CARERS_EMPLOYING_STAFF: pl.Float32,
            DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF: pl.Float32,
            DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS: pl.Float32,
            DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF: pl.Float32,
            DP.ESTIMATED_PROPORTION_OF_DPR_WHO_ARE_SERVICE_USERS: pl.Float32,
        }

        # rows = [
        #     ("Area A", 2022, 100.0, 40.0, 200.0, 30.0, 2.0, 1.7948, 0.2554, 32.0503, 64.1007, 0.1590, 0.5),
        #     ("Area B", 2022, 50.0, 10.0, 100.0, 10.0, 1.5, 0.8974, 0.0638, 10.9613, 16.4419, 0.1096, 0.5),
        # ] # fmt: skip

        rows = [
            ("Area A", 2022, 100.0, 200.0, 30.0, 2.0, 1.7948, 31.7949, 63.5898, 0.1590, 0.5),
            ("Area B", 2022, 50.0, 100.0, 10.0, 1.5, 0.8974, 10.8975, 16.34625, 0.1090, 0.5),
        ] # fmt: skip

        expected_lf = pl.LazyFrame(rows, schema, orient="row")
        test_lf = expected_lf.drop(
            DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF,
            # DP.ESTIMATED_CARERS_EMPLOYING_STAFF,
            DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF,
            DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
            DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF,
            DP.ESTIMATED_PROPORTION_OF_DPR_WHO_ARE_SERVICE_USERS,
        )
        returned_lf = job.calculate_remaining_variables(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, abs_tol=1e-4)
