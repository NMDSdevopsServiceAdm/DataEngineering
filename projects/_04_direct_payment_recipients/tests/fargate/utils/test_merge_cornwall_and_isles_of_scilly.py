from dataclasses import dataclass
from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.merge_cornwall_and_isles_of_scilly as job
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


@dataclass
class MergeCornwallAndIslesOfScillyTestCase:
    id: str
    input_data: list[Any]
    expected_data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.input_data, self.expected_data, id=self.id)


merge_cornwall_and_isles_of_scilly_test_cases = [
    MergeCornwallAndIslesOfScillyTestCase(
        id="aggregates_single_year",
        input_data=[
            ("any_other_area",  2020, 1.0, 0.1, 0.4, 6.0, 1.9),
            ("Cornwall",        2020, 2.0, 0.2, 0.5, 7.0, 1.9),
            ("Isles of Scilly", 2020, 3.0, 0.3, 0.6, 8.0, 1.9),
        ],  # fmt: skip
        expected_data=[
            ("any_other_area",               2020, 1.0, 0.1, 0.4, 6.0, 1.9),
            ("Cornwall and Isles of Scilly", 2020, 5.0, 0.2, 0.5, 15.0, 1.9),
        ],  # fmt: skip
    ),
    MergeCornwallAndIslesOfScillyTestCase(
        id="aggregates_multiple_years",
        input_data=[
            ("any_other_area",  2020, 1.0, 0.1, 0.4, 6.0, 1.9),
            ("any_other_area",  2021, 1.0, 0.1, 0.4, 6.0, 1.9),
            ("Cornwall",        2020, 2.0, 0.2, 0.5, 7.0, 1.9),
            ("Isles of Scilly", 2020, 3.0, 0.3, 0.6, 8.0, 1.9),
            ("Cornwall",        2021, 2.5, 0.25, 0.55, 7.5, 1.9),
            ("Isles of Scilly", 2021, 3.5, 0.35, 0.65, 8.5, 1.9),
        ],  # fmt: skip
        expected_data=[
            ("any_other_area",               2020, 1.0, 0.1, 0.4, 6.0, 1.9),
            ("any_other_area",               2021, 1.0, 0.1, 0.4, 6.0, 1.9),
            ("Cornwall and Isles of Scilly", 2020, 5.0, 0.2, 0.5, 15.0, 1.9),
            ("Cornwall and Isles of Scilly", 2021, 6.0, 0.25, 0.55, 16.0, 1.9),
        ],  # fmt: skip
    ),
    MergeCornwallAndIslesOfScillyTestCase(
        id="handles_areas_when_not_in_alphabetical_order",
        input_data=[
            ("Isles of Scilly", 2020, 3.0, 0.3, 0.6, 8.0, 1.9),
            ("Isles of Scilly", 2021, 3.5, 0.35, 0.65, 8.5, 1.9),
            ("Cornwall",        2020, 2.0, 0.2, 0.5, 7.0, 1.9),
            ("Cornwall",        2021, 2.5, 0.25, 0.55, 7.5, 1.9),
        ],  # fmt: skip
        expected_data=[
            ("Cornwall and Isles of Scilly", 2020, 5.0, 0.2, 0.5, 15.0, 1.9),
            ("Cornwall and Isles of Scilly", 2021, 6.0, 0.25, 0.55, 16.0, 1.9),
        ],  # fmt: skip
    ),
    MergeCornwallAndIslesOfScillyTestCase(
        id="handles_null_values",
        input_data=[
            ("Cornwall",        2020, None, None, None, None, None),
            ("Isles of Scilly", 2020, 3.0, 0.3, 0.6, 8.0, 1.9),
            ("Cornwall",        2021, None, None, None, None, None),
            ("Isles of Scilly", 2021, None, None, None, None, None),
        ],  # fmt: skip
        expected_data=[
            ("Cornwall and Isles of Scilly", 2020, 3.0, None, None, 8.0, None),
            ("Cornwall and Isles of Scilly", 2021, None, None, None, None, None),
        ],  # fmt: skip
    ),
]


class TestMergeCornwallAndIslesOfScilly:
    @pytest.mark.parametrize(
        ("input_data", "expected_data"),
        [
            case.as_pytest_param()
            for case in merge_cornwall_and_isles_of_scilly_test_cases
        ],
    )
    def test_function_returns_expected_values(self, input_data, expected_data):
        test_schema = {
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int32,
            DP.SERVICE_USER_DPRS_DURING_YEAR: pl.Float32,
            DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
            DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE: pl.Float32,
            DP.TOTAL_DPRS_DURING_YEAR: pl.Float32,
            DP.FILLED_POSTS_PER_EMPLOYER: pl.Float32,
        }
        input_lf = pl.LazyFrame(
            input_data,
            schema=test_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            expected_data,
            schema=test_schema,
            orient="row",
        )
        returned_lf = job.merge_cornwall_and_isles_of_scilly(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
