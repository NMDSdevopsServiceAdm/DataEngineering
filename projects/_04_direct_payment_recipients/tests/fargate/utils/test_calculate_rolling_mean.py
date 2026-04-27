from dataclasses import dataclass
from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.calculate_rolling_mean as job
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


@dataclass
class CalculateRollingMeanTestCase:
    id: str
    data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.data, id=self.id)


rolling_mean_test_cases = [
    CalculateRollingMeanTestCase(
        id="all_values_are_populated_in_one_area",
        data=[
            ("area_1", 2021, 0.6, 0.5),
            ("area_1", 2020, 0.5, 0.4),
            ("area_1", 2019, 0.4, 0.3),
            ("area_1", 2018, 0.3, 0.2),
            ("area_1", 2017, 0.2, 0.15),
            ("area_1", 2016, 0.1, 0.1),
        ],
    ),
    CalculateRollingMeanTestCase(
        id="all_values_are_populated_in_multiple_areas",
        data=[
            ("area_1", 2021, 0.6, 0.5),
            ("area_1", 2020, 0.5, 0.4),
            ("area_1", 2019, 0.4, 0.3),
            ("area_1", 2018, 0.3, 0.2),
            ("area_1", 2017, 0.2, 0.15),
            ("area_1", 2016, 0.1, 0.1),
            ("area_2", 2021, 0.7, 0.6),
            ("area_2", 2020, 0.6, 0.5),
            ("area_2", 2019, 0.5, 0.4),
            ("area_2", 2018, 0.4, 0.3),
            ("area_2", 2017, 0.3, 0.25),
            ("area_2", 2016, 0.2, 0.2),
        ],
    ),
    CalculateRollingMeanTestCase(
        id="an_area_has_null_values",
        data=[
            ("area_1", 2021, None, None),
            ("area_1", 2020, None, 0.3),
            ("area_1", 2019, None, 0.25),
            ("area_1", 2018, 0.3, 0.2),
            ("area_1", 2017, 0.2, 0.15),
            ("area_1", 2016, 0.1, 0.1),
        ],
    ),
    CalculateRollingMeanTestCase(
        id="years_are_not_consecutive",
        data=[
            ("area_3", 2019, 0.3, 0.25),
            ("area_3", 2018, 0.2, 0.15),
            ("area_3", 2016, 0.1, 0.1),
        ],
    ),
    CalculateRollingMeanTestCase(
        id="fewer_than_rolling_period_years_in_data",
        data=[
            ("area_3", 2017, 0.2, 0.15),
            ("area_3", 2016, 0.1, 0.1),
        ],
    ),
]


class TestCalculateRollingMean:
    @pytest.mark.parametrize(
        "test_data",
        [case.as_pytest_param() for case in rolling_mean_test_cases],
    )
    def test_function_returns_expected_values(self, test_data):
        expected_lf = pl.LazyFrame(
            test_data,
            schema={
                DP.LA_AREA: pl.String,
                DP.YEAR_AS_INTEGER: pl.Int64,
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float64,
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float64,
            },
            orient="row",
        )
        test_lf = expected_lf.drop(
            DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
        )
        returned_lf = job.calculate_rolling_mean(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
