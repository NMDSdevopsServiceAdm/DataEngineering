import math

import polars as pl

from polars_utils.cleaning_utils import create_banded_bed_count_column
from projects._03_independent_cqc.utils.imputation.interpolation import (
    model_interpolation,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_names.ind_cqc_pipeline_columns import (
    PrimaryServiceRateOfChangeColumns as TempCol,
)
from utils.column_values.categorical_column_values import CareHome

BANDED_BED_THRESHOLDS: list = [0, 1, 15, 25, math.inf]


def model_primary_service_rate_of_change_trendline(
    lf: pl.LazyFrame,
    value_col: str,
    days: int,
    out_col: str,
    max_days_between_submissions: int | None = None,
) -> pl.LazyFrame:
    """
    Calculates a trendline of the rate of change of a column split by primary
    service.

    The steps in this function are:
    1. Create a banded bed count column for grouping.
    2. Null-mask the value column for ineligible rows (inconsistent care home
        status, or fewer than two submissions) rather than filtering them out,
        so every row of `lf` stays present throughout and the trendline can be
        broadcast back onto it with `.over()` instead of a join.
    3. Apply interpolation to the current period values where needed.
    4. Calculate the previous period values using a lag.
    5. Clean the rate of change values for non-residential locations using
        percentile-based thresholds.
    6. Calculate rolling sums of current and previous values over the specified
        number of days, grouped by service type and bed band.
    7. Compute the single period rate of change as the ratio of rolling current
        sum to rolling previous sum.
    8. Calculate the trendline by taking the cumulative product of the single
        period rate of change values, grouped by service type and bed band,
        broadcasting the result onto every row (eligible or not) that shares a
        service type, bed band, and import date. Rows with no eligible peer at
        all for their group and date fall back to 1.0.

    Example:
        Given a rate of change sequence:
            - Period 2: 1.01 (1.0% increase from period 1 to 2)
            - Period 3: 1.02 (2.0% increase from period 2 to 3)
            - Period 4: 0.99 (1.0% decrease from period 3 to 4)
        The computed trendline:
            - Period 1: 1.0 (no change)
            - Period 2: 1.01 (1.0% increase from period 1 to 2)
            - Period 3: 1.01 * 1.02 = 1.0301 (3.0% increase from period 1 to 3)
            - Period 4: 1.01 * 1.02 * 0.99 = 1.02 (2.0% increase from period 1 to 4)

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        value_col (str): The column containing the values for which to compute the rate of change.
        days (int): The number of days over which to compute the rolling sum.
        out_col (str): The column name for the output trendline values.
        max_days_between_submissions (int | None): The maximum number of days between submissions for interpolation.

    Returns:
        pl.LazyFrame: The LazyFrame with the computed trendline values.
    """
    lf = create_banded_bed_count_column(
        lf, new_col=IndCqc.number_of_beds_banded_roc, splits=BANDED_BED_THRESHOLDS
    )

    aggregation_group_cols = [
        IndCqc.primary_service_type,
        IndCqc.number_of_beds_banded_roc,
    ]

    # The measurements differ for care home vs non-residential so only locations
    # with a single care home status contribute.
    # At least two submissions are required to measure change.
    is_eligible = (pl.col(IndCqc.care_home_status_count) == 1) & (
        pl.len().over([IndCqc.location_id, IndCqc.care_home]) >= 2
    )

    lf = lf.with_columns(
        pl.when(is_eligible)
        .then(pl.col(value_col))
        .otherwise(None)
        .alias(TempCol.current_period)
    )

    lf = model_interpolation(
        lf,
        TempCol.current_period,
        method="straight",
        new_column_name=TempCol.current_period_interpolated,
        max_days_between_submissions=max_days_between_submissions,
    ).with_columns(
        pl.coalesce(
            pl.col(TempCol.current_period),
            pl.col(TempCol.current_period_interpolated),
        )
        .cast(pl.Float64)
        .alias(TempCol.current_period_interpolated)
    )

    lf = lf.with_columns(
        pl.col(TempCol.current_period_interpolated)
        .sort_by(IndCqc.cqc_location_import_date)
        .shift(1)
        .over(IndCqc.location_id)
        .cast(pl.Float64)
        .alias(TempCol.previous_period_interpolated)
    )

    lf = clean_non_residential_rate_of_change(lf)

    lf = calculate_rolling_sums(lf, days, aggregation_group_cols)

    lf = lf.with_columns(
        pl.when(pl.col(TempCol.rolling_previous_sum) != 0)
        .then(
            pl.col(TempCol.rolling_current_sum) / pl.col(TempCol.rolling_previous_sum)
        )
        .otherwise(None)
        .alias(IndCqc.single_period_rate_of_change)
    ).drop(TempCol.rolling_current_sum, TempCol.rolling_previous_sum)

    lf = calculate_trendline(lf, out_col, aggregation_group_cols)

    lf = lf.drop(
        IndCqc.number_of_beds_banded_roc,
        TempCol.current_period,
        TempCol.current_period_interpolated,
        TempCol.previous_period_interpolated,
        TempCol.current_period_cleaned,
        TempCol.previous_period_cleaned,
    )

    return lf.with_columns(pl.col(out_col).fill_null(1.0))


def calculate_rolling_sums(
    lf: pl.LazyFrame,
    days: int,
    group_cols: list[str],
) -> pl.LazyFrame:
    """
    Adds the rolling sum of the current and previous period values, broadcast
    onto every row via `.over()`, partitioned by primary service type.

    The rolling sum is calculated over a specified number of days using
    `rolling_sum_by`, which reasons off the import date column's actual values
    rather than physical row position, so no upfront sort is needed. Only rows
    where both the current and previous period cleaned values are known
    (non-null) contribute their value to the sum — an unpaired first
    submission, or a null-masked ineligible row, contributes nothing, matching
    the previous row-filtering behaviour without removing rows. Every row,
    including rows that don't themselves contribute, still receives the
    correct broadcast sum for its own group and import date.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the current and
            previous period cleaned values.
        days (int): The number of days over which to compute the rolling sum.
        group_cols (list[str]): The columns to group by for the rolling sum
            calculation.

    Returns:
        pl.LazyFrame: The LazyFrame with the rolling sums of current and
            previous period values added.
    """
    paired = (
        pl.col(TempCol.current_period_cleaned).is_not_null()
        & pl.col(TempCol.previous_period_cleaned).is_not_null()
    )
    window = f"{days}d"

    return lf.with_columns(
        pl.when(paired)
        .then(pl.col(TempCol.current_period_cleaned))
        .otherwise(None)
        .rolling_sum_by(by=IndCqc.cqc_location_import_date, window_size=window)
        .over(group_cols)
        .cast(pl.Float64)
        .alias(TempCol.rolling_current_sum),
        pl.when(paired)
        .then(pl.col(TempCol.previous_period_cleaned))
        .otherwise(None)
        .rolling_sum_by(by=IndCqc.cqc_location_import_date, window_size=window)
        .over(group_cols)
        .cast(pl.Float64)
        .alias(TempCol.rolling_previous_sum),
    )


def clean_non_residential_rate_of_change(
    lf: pl.LazyFrame,
    abs_percentile: float = 0.99,
    perc_percentile: float = 0.99,
) -> pl.LazyFrame:
    """
    Cleans the rate of change values for non-residential locations by applying
    percentile-based thresholds.

    For non-residential rows, the function calculates the absolute and
    percentage change between the current and previous period values. It then
    computes upper thresholds for both absolute and percentage change based on
    specified percentiles of the distribution of changes in non-residential
    rows. A lower threshold for percentage change is also calculated as the
    reciprocal of the upper percentage change threshold.

    Rows outside the qualifying population (see below) are masked to null
    rather than filtered out before the percentile thresholds are computed,
    so the thresholds are evaluated as part of the same lazy chain as the
    rest of the frame rather than requiring an early collect. If no rows
    qualify, both thresholds are null and non-residential rows are only kept
    via the small-location passthrough below.

    Small locations are removed from the threshold calculations as minor changes
    in these locations can result in large percentage changes which would widen
    the thresholds and reduce the effectiveness of the cleaning. However, small
    locations are retained in the final output regardless of their rate of
    change values as they will have a minimal impact on the overall trendline.

    Args:
        lf (pl.LazyFrame): The input DataFrame containing the current and
            previous values.
        abs_percentile (float): The percentile to use for the absolute change
            threshold.
        perc_percentile (float): The percentile to use for the percentage change
            threshold.

    Returns:
        pl.LazyFrame: The DataFrame with cleaned current and previous period
            columns.
    """
    # Aliases for readability in calculations
    prev = TempCol.previous_period_interpolated
    curr = TempCol.current_period_interpolated

    is_care_home = pl.col(IndCqc.care_home) == CareHome.care_home
    is_non_res = pl.col(IndCqc.care_home) == CareHome.not_care_home

    SMALL_NON_RES_THRESHOLD = 10

    lf = lf.with_columns(
        [
            (pl.col(curr) - pl.col(prev)).abs().alias(TempCol.abs_change),
            (pl.col(curr) / pl.col(prev)).alias(TempCol.perc_change),
        ]
    )

    qualifying_row_expr = (
        is_non_res
        & pl.col(prev).is_not_null()
        & pl.col(curr).is_not_null()
        & (
            (pl.col(prev) > SMALL_NON_RES_THRESHOLD)
            | (pl.col(curr) > SMALL_NON_RES_THRESHOLD)
        )
        & (pl.col(prev) != pl.col(curr))
    )

    lf = lf.with_columns(
        [
            pl.when(qualifying_row_expr)
            .then(pl.col(TempCol.abs_change))
            .otherwise(None)
            .quantile(abs_percentile)
            .alias(TempCol.abs_pct),
            pl.when(qualifying_row_expr)
            .then(pl.col(TempCol.perc_change))
            .otherwise(None)
            .quantile(perc_percentile)
            .alias(TempCol.perc_pct),
        ]
    )

    abs_change_upper_threshold = pl.col(TempCol.abs_pct)
    perc_change_upper_threshold = pl.col(TempCol.perc_pct)
    perc_change_lower_threshold = 1 / perc_change_upper_threshold

    is_small_non_res = (
        is_non_res
        & (pl.col(prev) <= SMALL_NON_RES_THRESHOLD)
        & (pl.col(curr) <= SMALL_NON_RES_THRESHOLD)
    )

    is_valid_non_res = (
        is_non_res
        & abs_change_upper_threshold.is_not_null()
        & perc_change_upper_threshold.is_not_null()
        & (pl.col(TempCol.abs_change) <= abs_change_upper_threshold)
        & (pl.col(TempCol.perc_change) <= perc_change_upper_threshold)
        & (pl.col(TempCol.perc_change) >= perc_change_lower_threshold)
    )

    keep = is_care_home | is_small_non_res | is_valid_non_res

    return lf.with_columns(
        [
            pl.when(keep)
            .then(pl.col(prev))
            .otherwise(None)
            .cast(pl.Float64)
            .alias(TempCol.previous_period_cleaned),
            pl.when(keep)
            .then(pl.col(curr))
            .otherwise(None)
            .cast(pl.Float64)
            .alias(TempCol.current_period_cleaned),
        ]
    ).drop(TempCol.abs_change, TempCol.perc_change, TempCol.abs_pct, TempCol.perc_pct)


def calculate_trendline(
    lf: pl.LazyFrame, out_col: str, group_cols: list[str]
) -> pl.LazyFrame:
    """
    Computes a trendline from a sequence of single-period rate of change values,
    starting at 1.0 in the first period.

    The trendline is then derived by iteratively multiplying each rate of change
    value, resulting in a cumulative measure of change over time. This is
    calculated by taking the exponential of the sum of the logarithms of the
    values.

    Multiple rows can share the same group_cols and import date (e.g. many
    locations under one primary service/bed band triple), each already carrying
    an identical single_period_rate_of_change broadcast by calculate_rolling_sums.
    Only the first row of each such tie (by row order, since `.over()` uses
    `order_by` rather than a physical sort) is allowed to contribute that date's
    value to the cumulative sum, the rest contribute 0.0 — this avoids
    double-counting a date once per tied row, while every row in the tie still
    ends up with the same cumulative total once the whole date has been counted.
    A null rate (no eligible data at all for that group and date) nulls out the
    contribution for every row sharing the tie, not just the first, so the whole
    group of rows falls through to the caller's `fill_null(1.0)`.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        out_col (str): The name of the output column for the trendline.
        group_cols (list[str]): The columns to group by.

    Returns:
        pl.LazyFrame: The LazyFrame with the trendline column added.
    """
    rate_col = IndCqc.single_period_rate_of_change
    date_col = IndCqc.cqc_location_import_date

    is_first_of_date_group = (
        pl.cum_count(date_col).over(group_cols + [date_col], order_by=date_col) == 1
    )

    contribution = (
        pl.when(pl.col(rate_col).is_null())
        .then(None)
        .when(is_first_of_date_group)
        .then(pl.col(rate_col).log())
        .otherwise(0.0)
    )

    rolling_product = contribution.cum_sum().over(group_cols, order_by=date_col).exp()

    return lf.with_columns(rolling_product.alias(out_col)).drop(rate_col)
