import unittest
import polars as pl
import polars.testing as pl_testing

import projects._04_direct_payment_recipients.fargate.utils.models.extrapolation_ratio as job
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestExtrapolationRatio(unittest.TestCase):
    def test_function_retuns_expected_values(self):
        schema = {
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int32,
            DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
            DP.ESTIMATE_USING_MEAN: pl.Float32,
            DP.FIRST_YEAR_WITH_DATA: pl.Int32,
            DP.LAST_YEAR_WITH_DATA: pl.Int32,
            DP.ESTIMATE_USING_EXTRAPOLATION_RATIO: pl.Float32,
        }
        rows = [
            ("area_1", 2018, None, 280.0, 2019, 2021, 0.35),
            ("area_1", 2019, 0.375, 300.0, 2019, 2021, None),
            ("area_1", 2020, None, 300.0, 2019, 2021, None),
            ("area_1", 2021, 0.3, 320.0, 2019, 2021, None),
            ("area_1", 2022, None, 340.0, 2019, 2021, 0.31875),
            ("area_2", 2018, None, 280.0, 2019, 2021, 0.186667),
            ("area_2", 2019, 0.2, 300.0, 2019, 2021, None),
            ("area_2", 2020, 0.35, 300.0, 2019, 2021, None),
            ("area_2", 2021, 0.4, 320.0, 2019, 2021, None),
            ("area_2", 2022, None, 340.0, 2019, 2021, 0.425),
        ]
        expected_lf = pl.LazyFrame(rows, schema, orient="row")
        test_lf = expected_lf.drop(
            DP.FIRST_YEAR_WITH_DATA,
            DP.LAST_YEAR_WITH_DATA,
            DP.ESTIMATE_USING_EXTRAPOLATION_RATIO,
        )
        returned_lf = job.model_extrapolation(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestValueAtBoundaryYear(unittest.TestCase):
    def setUp(self):
        schema = {
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int32,
            DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
            DP.FIRST_YEAR_WITH_DATA: pl.Int32,
            DP.LAST_YEAR_WITH_DATA: pl.Int32,
            "expected_first": pl.Float32,
            "expected_last": pl.Float32,
        }
        rows = [
            ("area_1", 2018, None, 2019, 2021, 0.375, 0.3),
            ("area_1", 2019, 0.375, 2019, 2021, 0.375, 0.3),
            ("area_1", 2020, 0.45, 2019, 2021, 0.375, 0.3),
            ("area_1", 2021, 0.3, 2019, 2021, 0.375, 0.3),
            ("area_1", 2022, None, 2019, 2021, 0.375, 0.3),
            ("area_2", 2018, None, 2019, 2021, 0.2, 0.4),
            ("area_2", 2019, 0.2, 2019, 2021, 0.2, 0.4),
            ("area_2", 2020, 0.35, 2019, 2021, 0.2, 0.4),
            ("area_2", 2021, 0.4, 2019, 2021, 0.2, 0.4),
            ("area_2", 2022, None, 2019, 2021, 0.2, 0.4),
        ]
        self.expected_lf = pl.LazyFrame(rows, schema, orient="row")
        self.test_lf = self.expected_lf.drop("expected_first", "expected_last")

    def test_returns_expected_values_at_first_boundary_year(self):
        returned_lf = self.test_lf.with_columns(
            job.value_at_boundary_year(
                DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                DP.FIRST_YEAR_WITH_DATA,
            ).alias("expected_first")
        )
        pl_testing.assert_frame_equal(
            returned_lf, self.expected_lf.drop("expected_last")
        )

    def test_returns_expected_values_at_last_boundary_year(self):
        returned_lf = self.test_lf.with_columns(
            job.value_at_boundary_year(
                DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                DP.LAST_YEAR_WITH_DATA,
            ).alias("expected_last")
        )
        pl_testing.assert_frame_equal(
            returned_lf, self.expected_lf.drop("expected_first")
        )
