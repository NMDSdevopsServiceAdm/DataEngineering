import unittest

import polars as pl
import polars.testing as pl_testing

import projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.create_summary_table as job
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestCreateSummaryTable(unittest.TestCase):
    def test_function_returns_expected_values(self):
        input_schema = {
            DP.YEAR_AS_INTEGER: pl.Int32,
            DP.LA_AREA: pl.String,
            DP.TOTAL_DPRS_DURING_YEAR: pl.Float64,
            DP.SERVICE_USER_DPRS_DURING_YEAR: pl.Float64,
            DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF: pl.Float64,
            DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF: pl.Float64,
            # DP.ESTIMATED_CARERS_EMPLOYING_STAFF: pl.Float64,
            DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF: pl.Float64,
            DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS: pl.Float64,
        }
        # input_rows = [
        #     (2020, "area_1", 100.0, 80.0, 40.0, 10.0, 5.0, 45.0, 50.0),
        #     (2020, "area_2", 200.0, 160.0, 80.0, 20.0, 10.0, 90.0, 100.0),
        #     (2021, "area_1", 110.0, 90.0, 45.0, 11.0, 6.0, 51.0, 55.0),
        #     (2021, "area_2", 210.0, 170.0, 85.0, 21.0, 11.0, 96.0, 105.0),
        # ]
        input_rows = [
            (2020, "area_1", 100.0, 80.0, 40.0, 10.0, 45.0, 50.0),
            (2020, "area_2", 200.0, 160.0, 80.0, 20.0, 90.0, 100.0),
            (2021, "area_1", 110.0, 90.0, 45.0, 11.0, 51.0, 55.0),
            (2021, "area_2", 210.0, 170.0, 85.0, 21.0, 96.0, 105.0),
        ]

        expected_schema = {
            DP.YEAR_AS_INTEGER: pl.Int32,
            DP.TOTAL_DPRS: pl.Float32,
            DP.SERVICE_USER_DPRS: pl.Float32,
            DP.SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
            DP.SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF: pl.Float32,
            # DP.CARERS_EMPLOYING_STAFF: pl.Float32,
            DP.TOTAL_DPRS_EMPLOYING_STAFF: pl.Float32,
            DP.TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS: pl.Float32,
        }
        # expected_rows = [
        #     (2020, 300.0, 240.0, 120.0, 30.0, 15.0, 135.0, 150.0),
        #     (2021, 320.0, 260.0, 130.0, 32.0, 17.0, 147.0, 160.0),
        # ]
        expected_rows = [
            (2020, 300.0, 240.0, 120.0, 30.0, 135.0, 150.0),
            (2021, 320.0, 260.0, 130.0, 32.0, 147.0, 160.0),
        ]

        test_lf = pl.LazyFrame(input_rows, schema=input_schema, orient="row")
        expected_lf = pl.LazyFrame(expected_rows, schema=expected_schema, orient="row")

        returned_lf = job.create_summary_table(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
