import polars as pl
import polars.testing as pl_testing
import pytest

import projects._04_direct_payment_recipients.fargate.utils.prepare_dpr_utils.calculate_pa_ratio as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

# Historic years are always in the output. Tests use years after them and
# compare only from 2020 on, so historic ratios never enter a rolling window.
FIRST_TEST_YEAR = 2020
ALL_YEARS = 0


def make_survey_lf(rows: list[tuple[int, float]]) -> pl.LazyFrame:
    return pl.LazyFrame(
        rows,
        schema={DP.YEAR: pl.Int32, DP.TOTAL_STAFF_RECODED: pl.Float64},
        orient="row",
    )


def make_expected_df(rows: list[tuple[int, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={DP.YEAR_AS_INTEGER: pl.Int32, DP.RATIO_ROLLING_AVERAGE: pl.Float64},
        orient="row",
    )


def run(survey_lf: pl.LazyFrame, from_year: int = FIRST_TEST_YEAR) -> pl.DataFrame:
    return (
        job.calculate_pa_ratio(survey_lf)
        .filter(pl.col(DP.YEAR_AS_INTEGER) >= from_year)
        .sort(DP.YEAR_AS_INTEGER)
        .collect()
    )


def test_excludes_total_staff_outside_1_to_9_inclusive():
    survey_lf = make_survey_lf(
        [(2030, 0.5), (2030, 1.0), (2030, 9.0), (2030, 9.5), (2030, 5.0)]
    )

    returned_df = run(survey_lf)

    pl_testing.assert_frame_equal(returned_df, make_expected_df([(2029, 5.0)]))


def test_average_is_mean_total_staff_per_year():
    survey_lf = make_survey_lf([(2030, 2.0), (2030, 4.0), (2031, 6.0)])

    returned_df = run(survey_lf)

    pl_testing.assert_frame_equal(
        returned_df, make_expected_df([(2029, 3.0), (2030, 4.5)])
    )


def test_survey_overrides_historic_ratio_and_historic_fills_gaps():
    # Historic years are 2011-13, 2015, 2016 and 2018; survey 2013 overrides 1.98.
    survey_lf = make_survey_lf([(2013, 5.0)])

    returned_df = run(survey_lf, from_year=ALL_YEARS)

    expected_df = make_expected_df(
        [
            (2010, 1.98),
            (2011, 1.98),
            (2012, (1.98 + 1.98 + 5.0) / 3),
            (2014, (5.0 + 2.00) / 2),
            (2015, (2.00 + 2.01) / 2),
            (2017, (2.01 + 1.96) / 2),
        ]
    )
    pl_testing.assert_frame_equal(returned_df, expected_df)


def test_empty_survey_returns_historic_years_only():
    survey_lf = make_survey_lf([])

    returned_df = run(survey_lf, from_year=ALL_YEARS)

    expected_df = make_expected_df(
        [
            (2010, 1.98),
            (2011, 1.98),
            (2012, 1.98),
            (2014, (1.98 + 2.00) / 2),
            (2015, (2.00 + 2.01) / 2),
            (2017, (2.01 + 1.96) / 2),
        ]
    )
    pl_testing.assert_frame_equal(returned_df, expected_df)


def test_year_is_reduced_by_one():
    survey_lf = make_survey_lf([(2030, 5.0)])

    returned_df = run(survey_lf)

    assert returned_df[DP.YEAR_AS_INTEGER].to_list() == [2029]


@pytest.mark.parametrize(
    "survey_years, expected_ratios",
    [
        pytest.param(
            [2030, 2031, 2032, 2033],
            [1.0, 1.5, 2.0, 3.0],
            id="consecutive_years",
        ),
        pytest.param(
            [2030, 2031, 2033, 2034],
            [1.0, 1.5, 2.5, 3.5],
            id="gap_year_shrinks_window_and_divides_by_rows_present",
        ),
    ],
)
def test_rolling_average_shrinks_window_over_gap_years(
    survey_years: list[int], expected_ratios: list[float]
):
    survey_lf = make_survey_lf(
        [(year, float(i + 1)) for i, year in enumerate(survey_years)]
    )

    returned_df = run(survey_lf)

    expected_df = make_expected_df(
        [(year - 1, ratio) for year, ratio in zip(survey_years, expected_ratios)]
    )
    pl_testing.assert_frame_equal(returned_df, expected_df)


def test_returns_only_year_and_rolling_average():
    survey_lf = make_survey_lf([(2030, 5.0)])

    returned_df = run(survey_lf)

    assert returned_df.columns == [DP.YEAR_AS_INTEGER, DP.RATIO_ROLLING_AVERAGE]
