import polars as pl
import polars.testing as pl_testing
import pytest

import projects._04_direct_payment_recipients.fargate.utils.prepare_dpr_utils.remove_outliers as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

SCHEMA = {
    DP.LA_AREA: pl.String,
    DP.YEAR_AS_INTEGER: pl.Int32,
    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
    DP.TOTAL_DPRS_DURING_YEAR: pl.Float32,
}


def run(input_data, expected_data):
    input_lf = pl.LazyFrame(input_data, schema=SCHEMA, orient="row")
    expected_lf = pl.LazyFrame(expected_data, schema=SCHEMA, orient="row")
    returned_lf = job.remove_outliers(input_lf)
    pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestRemoveOutliers:
    @pytest.mark.parametrize(
        ("neighbour", "value"),
        [(0.0, -0.1), (1.0, 1.1)],
        ids=["below_zero", "above_one"],
    )
    def test_removes_values_outside_zero_to_one(self, neighbour, value):
        # Neighbours sit next to the value so only the range rule applies.
        run(
            [("a", 2019, neighbour, 1.0), ("a", 2020, neighbour, 1.0), ("a", 2021, value, 1.0)],
            [("a", 2019, neighbour, 1.0), ("a", 2020, neighbour, 1.0), ("a", 2021, None, 1.0)],
        )  # fmt: skip

    @pytest.mark.parametrize(
        ("value", "expected"),
        [(0.9, None), (0.1, None), (0.8, 0.8), (0.2, 0.2)],
        ids=["above_85", "below_15", "inside_high", "inside_low"],
    )
    def test_removes_extreme_value_when_la_has_only_one_value(self, value, expected):
        run(
            [("a", 2020, None, 1.0), ("a", 2021, value, 1.0)],
            [("a", 2020, None, 1.0), ("a", 2021, expected, 1.0)],
        )  # fmt: skip

    def test_keeps_extreme_value_when_la_has_several_values(self):
        run(
            [("a", 2020, 0.9, 1.0), ("a", 2021, 0.9, 1.0)],
            [("a", 2020, 0.9, 1.0), ("a", 2021, 0.9, 1.0)],
        )  # fmt: skip

    def test_removes_values_far_from_la_mean(self):
        # Mean is 0.35: only 0.8 is >= 0.3 away.
        run(
            [("a", 2018, 0.2, 1.0), ("a", 2019, 0.2, 1.0), ("a", 2020, 0.2, 1.0), ("a", 2021, 0.8, 1.0)],
            [("a", 2018, 0.2, 1.0), ("a", 2019, 0.2, 1.0), ("a", 2020, 0.2, 1.0), ("a", 2021, None, 1.0)],
        )  # fmt: skip

    @pytest.mark.parametrize(
        ("value_2019", "value_2021", "value_2022", "expected_2022"),
        [
            (0.6, 0.6, 0.95, None),
            (0.4, 0.4, 0.05, None),
            (0.8, 0.8, 0.95, 0.95),
            (0.6, None, 0.95, 0.95),
            (0.6, 0.6, 0.85, 0.85),
        ],
        ids=[
            "high_and_far_from_2021",
            "low_and_far_from_2021",
            "follows_2021",
            "null_2021_keeps",
            "not_extreme",
        ],
    )
    def test_removes_extreme_2022_value_not_following_2021_trend(
        self, value_2019, value_2021, value_2022, expected_2022
    ):
        # 2019 mirrors 2021 so the LA mean stays within 0.3 of 2022 and only the trend rule applies.
        run(
            [("a", 2019, value_2019, 1.0), ("a", 2021, value_2021, 1.0), ("a", 2022, value_2022, 1.0)],
            [("a", 2019, value_2019, 1.0), ("a", 2021, value_2021, 1.0), ("a", 2022, expected_2022, 1.0)],
        )  # fmt: skip

    def test_retains_latest_value_when_between_25_and_75_percent(self):
        # Mean is 0.15, so 0.6 is far from the mean, but it is the latest value and in range.
        run(
            [("a", 2018, 0.0, 1.0), ("a", 2019, 0.0, 1.0), ("a", 2020, 0.0, 1.0), ("a", 2021, 0.6, 1.0)],
            [("a", 2018, 0.0, 1.0), ("a", 2019, 0.0, 1.0), ("a", 2020, 0.0, 1.0), ("a", 2021, 0.6, 1.0)],
        )  # fmt: skip

    def test_retains_latest_known_value_when_latest_year_is_null(self):
        # The null 2021 doesn't count, so 2020 is the latest known year.
        run(
            [("a", 2018, 0.0, 1.0), ("a", 2019, 0.0, 1.0), ("a", 2020, 0.6, 1.0), ("a", 2021, None, 1.0)],
            [("a", 2018, 0.0, 1.0), ("a", 2019, 0.0, 1.0), ("a", 2020, 0.6, 1.0), ("a", 2021, None, 1.0)],
        )  # fmt: skip

    def test_removes_in_range_value_that_is_not_the_latest_year(self):
        # 0.6 in 2019 is far from the mean and not the latest year (2021 is known), so it is removed.
        run(
            [("a", 2018, 0.0, 1.0), ("a", 2019, 0.6, 1.0), ("a", 2020, 0.0, 1.0), ("a", 2021, 0.0, 1.0)],
            [("a", 2018, 0.0, 1.0), ("a", 2019, None, 1.0), ("a", 2020, 0.0, 1.0), ("a", 2021, 0.0, 1.0)],
        )  # fmt: skip

    def test_other_columns_and_non_outliers_unchanged(self):
        run(
            [("a", 2020, 0.5, 11.0), ("a", 2021, 0.5, 12.0), ("b", 2020, 0.4, 13.0), ("b", 2021, 0.45, 14.0), ("c", 2021, 1.5, 15.0)],
            [("a", 2020, 0.5, 11.0), ("a", 2021, 0.5, 12.0), ("b", 2020, 0.4, 13.0), ("b", 2021, 0.45, 14.0), ("c", 2021, None, 15.0)],
        )  # fmt: skip

    def test_nulls_in_input_stay_null(self):
        run(
            [("a", 2020, None, 1.0), ("a", 2021, None, 2.0), ("b", 2020, 0.5, 3.0), ("b", 2021, None, 4.0)],
            [("a", 2020, None, 1.0), ("a", 2021, None, 2.0), ("b", 2020, 0.5, 3.0), ("b", 2021, None, 4.0)],
        )  # fmt: skip

    def test_returns_only_input_columns(self):
        input_lf = pl.LazyFrame([("a", 2021, 0.5, 1.0)], schema=SCHEMA, orient="row")
        returned_lf = job.remove_outliers(input_lf)
        assert returned_lf.collect_schema() == input_lf.collect_schema()
