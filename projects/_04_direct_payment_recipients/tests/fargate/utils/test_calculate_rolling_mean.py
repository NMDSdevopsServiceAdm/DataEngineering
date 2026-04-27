import unittest

import polars as pl
import polars.testing as pl_testing

import projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.calculate_rolling_mean as job
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestCalculateRollingMean(unittest.TestCase):
    def test_function_returns_expected_values(self):
        rows = [
            ("area_1", 2021, 0.6, 0.5),
            ("area_1", 2020, 0.5, 0.4),
            ("area_1", 2019, 0.4, 0.3),
            ("area_1", 2018, 0.3, 0.2),
            ("area_1", 2017, 0.2, 0.15),
            ("area_1", 2016, 0.1, 0.1),
            ("area_2", 2021, None, None),
            ("area_2", 2020, None, 0.3),
            ("area_2", 2019, None, 0.25),
            ("area_2", 2018, 0.3, 0.2),
            ("area_2", 2017, 0.2, 0.15),
            ("area_2", 2016, 0.1, 0.1),
        ]
        test_schema = {
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int64,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float64,
            DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float64,
        }
        expected_lf = pl.LazyFrame(rows, schema=test_schema, orient="row")
        test_lf = expected_lf.drop(
            DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
        )
        returned_lf = job.calculate_rolling_mean(test_lf)
        returned_lf.show(limit=20)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
