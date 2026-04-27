import unittest

import polars as pl
import polars.testing as pl_testing

import projects._04_direct_payment_recipients.fargate.utils.models.mean_imputation as job
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestCalculationConstants(unittest.TestCase):
    def test_calculation_constants(self):
        expected_schema = pl.Schema(
            {
                DP.YEAR_AS_INTEGER: pl.Int32,
                DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
                DP.ESTIMATE_USING_MEAN: pl.Float32,
            }
        )
        expected_lf = pl.LazyFrame(
            data=[
                (2020, 0.2, 0.4),
                (2020, 0.6, 0.4),
                (2021, None, 0.6),
                (2021, 0.6, 0.6),
                (2022, None, None),
                (2022, None, None),
            ],
            schema=expected_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(DP.ESTIMATE_USING_MEAN)
        returned_lf = job.model_using_mean(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
