import unittest
import polars as pl
import polars.testing as pl_testing

import projects._04_direct_payment_recipients.fargate.utils.models.interpolation as job
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

PATCH_PATH = "projects._04_direct_payment_recipients.fargate.utils.models.interpolation"


class TestDPRModelInterpolation(unittest.TestCase):
    def test_function_retuns_expected_values(self):
        rows = [
            ("area_1", 2019, 0.3, 0.3),
            ("area_1", 2020, None, 0.3375),
            ("area_1", 2021, 0.375, 0.375),
            ("area_2", 2019, 0.4, 0.4),
            ("area_2", 2020, 0.35, 0.35),
            ("area_2", 2021, None, None),
            ("area_3", 2019, None, None),
            ("area_3", 2020, 0.5, 0.5),
            ("area_3", 2021, None, None),
        ]
        test_schema = {
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int64,
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float64,
            DP.ESTIMATE_USING_INTERPOLATION: pl.Float64,
        }
        expected_lf = pl.LazyFrame(rows, schema=test_schema, orient="row")
        test_lf = expected_lf.drop(DP.ESTIMATE_USING_INTERPOLATION)
        returned_lf = job.model_interpolation(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
