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
    2. Prepare the data by selecting relevant columns and calculating
        submission counts.
    3. Filter to eligible rows (consistent care home status with at least two
        submissions).
    4. Apply interpolation to the current period values where needed.
    5. Calculate the previous period values using a lag.
    6. Clean the rate of change values for non-residential locations using
        percentile-based thresholds.
    7. Calculate rolling sums of current and previous values over the specified
        number of days, grouped by service type and bed band.
    8. Compute the single period rate of change as the ratio of rolling current
        sum to rolling previous sum.
    9. Calculate the trendline by taking the cumulative product of the single
        period rate of change values, grouped by service type and bed band.

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

    roc_lf = (
        lf.select(
            IndCqc.location_id,
            IndCqc.care_home,
            IndCqc.care_home_status_count,
            IndCqc.primary_service_type,
            IndCqc.number_of_beds_banded_roc,
            IndCqc.cqc_location_import_date,
            value_col,
        )
        .rename({value_col: TempCol.current_period})
        .with_columns(
            pl.len()
            .over([IndCqc.location_id, IndCqc.care_home])
            .alias(TempCol.submission_count)
        )
    )

    # The measurements differ for care home vs non-residential so only locations
    # with a single care home status are included.
    # At least two submissions are required to measure change.
    roc_lf = roc_lf.filter(
        (pl.col(IndCqc.care_home_status_count) == 1)
        & (pl.col(TempCol.submission_count) >= 2)
    )

    roc_lf = model_interpolation(
        roc_lf,
        TempCol.current_period,
        method="straight",
        new_column_name=TempCol.current_period_interpolated,
        max_days_between_submissions=max_days_between_submissions,
    ).with_columns(
        pl.coalesce(
            pl.col(TempCol.current_period),
            pl.col(TempCol.current_period_interpolated),
        )
        .cast(pl.Float32)
        .alias(TempCol.current_period_interpolated)
    )

    roc_lf = roc_lf.with_columns(
        pl.col(TempCol.current_period_interpolated)
        .sort_by(IndCqc.cqc_location_import_date)
        .shift(1)
        .over(IndCqc.location_id)
        .cast(pl.Float32)
        .alias(TempCol.previous_period_interpolated)
    )

    roc_lf = clean_non_residential_rate_of_change(roc_lf)

    roc_lf = calculate_rolling_sums(roc_lf, days, aggregation_group_cols)

    roc_lf = roc_lf.with_columns(
        pl.when(pl.col(TempCol.rolling_previous_sum) != 0)
        .then(
            pl.col(TempCol.rolling_current_sum) / pl.col(TempCol.rolling_previous_sum)
        )
        .otherwise(None)
        .alias(IndCqc.single_period_rate_of_change)
    ).drop(TempCol.rolling_current_sum, TempCol.rolling_previous_sum)

    roc_lf = calculate_trendline(roc_lf, out_col, aggregation_group_cols)

    lf = lf.join(
        roc_lf,
        [
            IndCqc.primary_service_type,
            IndCqc.number_of_beds_banded_roc,
            IndCqc.cqc_location_import_date,
        ],
        "left",
    ).drop(IndCqc.number_of_beds_banded_roc)

    return lf.with_columns(pl.col(out_col).fill_null(1.0))


def calculate_rolling_sums(
    lf: pl.LazyFrame,
    days: int,
    group_cols: list[str],
) -> pl.LazyFrame:
    """
    Calculates the rolling sum of the current and previous period values
    partitioned by primary service type.

    The rolling sum is calculated over a specified number of days, and the input
    rows are sorted by the grouping columns and import date to ensure stable and
    deterministic results even when input rows arrive out of order.

    The function first filters the input LazyFrame to include only rows where
    both the current and previous period interpolated values are known
    (non-null). It then computes the rolling sum of the current and previous
    period values over the defined period. Finally, it drops duplicate rows
    based on the partitioning columns and import date to ensure one row per
    primary service type, number of beds band, and time period, and drops
    non-aggregated columns that are no longer needed for subsequent
    calculations.

    Rolling returns one row per contributing source row so `.unique()` is used
    to deduplicate to one row per group and import date.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the current and
            previous period values.
        days (int): The number of days over which to compute the rolling sum.
        group_cols (list[str]): The columns to group by for the rolling sum
            calculation.

    Returns:
        pl.LazyFrame: The LazyFrame with the rolling sums of current and
            previous period values added.
    """
    return (
        lf.filter(
            pl.col(TempCol.current_period_cleaned).is_not_null()
            & pl.col(TempCol.previous_period_cleaned).is_not_null()
        )
        .sort(group_cols + [IndCqc.cqc_location_import_date])
        .rolling(
            index_column=IndCqc.cqc_location_import_date,
            period=f"{days}d",
            group_by=group_cols,
        )
        .agg(
            [
                pl.sum(TempCol.current_period_cleaned)
                .cast(pl.Float32)
                .alias(TempCol.rolling_current_sum),
                pl.sum(TempCol.previous_period_cleaned)
                .cast(pl.Float32)
                .alias(TempCol.rolling_previous_sum),
            ]
        )
    ).unique(group_cols + [IndCqc.cqc_location_import_date])


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

    abs_change_upper_threshold, perc_change_upper_threshold = (
        lf.filter(
            is_non_res
            & pl.col(prev).is_not_null()
            & pl.col(curr).is_not_null()
            & (
                (pl.col(prev) > SMALL_NON_RES_THRESHOLD)
                | (pl.col(curr) > SMALL_NON_RES_THRESHOLD)
            )
            & (pl.col(prev) != pl.col(curr))
        )
        .select(
            [
                pl.col(TempCol.abs_change).quantile(abs_percentile),
                pl.col(TempCol.perc_change).quantile(perc_percentile),
            ]
        )
        .collect()
        .row(0)
    )

    is_small_non_res = (
        is_non_res
        & (pl.col(prev) <= SMALL_NON_RES_THRESHOLD)
        & (pl.col(curr) <= SMALL_NON_RES_THRESHOLD)
    )

    if abs_change_upper_threshold is None or perc_change_upper_threshold is None:
        is_valid_non_res = pl.lit(False)
    else:
        perc_change_lower_threshold = 1 / perc_change_upper_threshold
        is_valid_non_res = (
            is_non_res
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
            .cast(pl.Float32)
            .alias(TempCol.previous_period_cleaned),
            pl.when(keep)
            .then(pl.col(curr))
            .otherwise(None)
            .cast(pl.Float32)
            .alias(TempCol.current_period_cleaned),
        ]
    ).drop(TempCol.abs_change, TempCol.perc_change)


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

    The input rows are sorted by the grouping columns and import date before the
    cumulative sum is computed. This ensures stable and deterministic trendline
    values even when input rows arrive out of order.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        out_col (str): The name of the output column for the trendline.
        group_cols (list[str]): The columns to group by.

    Returns:
        pl.LazyFrame: The LazyFrame with the trendline column added.
    """
    rate_col = IndCqc.single_period_rate_of_change

    rolling_product = pl.col(rate_col).log().cum_sum().over(group_cols).exp()

    return (
        lf.sort(group_cols + [IndCqc.cqc_location_import_date])
        .with_columns(rolling_product.alias(out_col))
        .drop(rate_col)
    )
