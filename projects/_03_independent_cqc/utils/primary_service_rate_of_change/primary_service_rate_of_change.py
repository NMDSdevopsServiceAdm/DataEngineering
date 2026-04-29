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
    Compute rate-of-change trendline for primary service types.

    Pipeline:
        prepare → filter → interpolate → lag → clean → aggregate → calculate → trend
    """
    lf = create_banded_bed_count_column(
        lf, new_col=IndCqc.number_of_beds_banded_roc, splits=BANDED_BED_THRESHOLDS
    )

    group_cols = [
        IndCqc.primary_service_type,
        IndCqc.number_of_beds_banded_roc,
    ]

    # --------------------------------------------------------
    # Prepare
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # Eligibility filter (inline — simple + obvious)
    # --------------------------------------------------------
    roc_lf = roc_lf.filter(
        (pl.col(IndCqc.care_home_status_count) == 1)
        & (pl.col(TempCol.submission_count) >= 2)
    )

    # --------------------------------------------------------
    # Interpolation
    # --------------------------------------------------------
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
        ).alias(TempCol.current_period_interpolated)
    )

    # --------------------------------------------------------
    # Previous value (lag)
    # --------------------------------------------------------
    roc_lf = roc_lf.with_columns(
        pl.col(TempCol.current_period_interpolated)
        .sort_by(IndCqc.cqc_location_import_date)
        .shift(1)
        .over(IndCqc.location_id)
        .alias(TempCol.previous_period_interpolated)
    )

    # --------------------------------------------------------
    # Cleaning (kept as function — real logic)
    # --------------------------------------------------------
    roc_lf = clean_non_residential_rate_of_change(roc_lf)

    # --------------------------------------------------------
    # Rolling aggregation (core logic)
    # --------------------------------------------------------
    roc_lf = calculate_rolling_sums(roc_lf, days, group_cols)

    # --------------------------------------------------------
    # Rate of change
    # --------------------------------------------------------
    roc_lf = roc_lf.with_columns(
        pl.when(pl.col(TempCol.rolling_previous_sum) != 0)
        .then(
            pl.col(TempCol.rolling_current_sum) / pl.col(TempCol.rolling_previous_sum)
        )
        .otherwise(None)
        .alias(IndCqc.single_period_rate_of_change)
    ).drop(TempCol.rolling_current_sum, TempCol.rolling_previous_sum)

    # --------------------------------------------------------
    # Trendline (log-sum-exp)
    # --------------------------------------------------------
    roc_lf = calculate_trendline(roc_lf, out_col, group_cols)

    # --------------------------------------------------------
    # Final shape
    # --------------------------------------------------------
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


# ============================================================
# Complex business logic — keep separate
# ============================================================


def calculate_rolling_sums(
    lf: pl.LazyFrame,
    days: int,
    group_cols: list[str],
) -> pl.LazyFrame:
    """
    Compute rolling sums of current and previous values
    grouped by service type and bed band.
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
                pl.sum(TempCol.current_period_cleaned).alias(
                    TempCol.rolling_current_sum
                ),
                pl.sum(TempCol.previous_period_cleaned).alias(
                    TempCol.rolling_previous_sum
                ),
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

    prev = TempCol.previous_period_interpolated
    curr = TempCol.current_period_interpolated

    # --------------------------------------------------------
    # Compute changes
    # --------------------------------------------------------
    lf = lf.with_columns(
        [
            (pl.col(curr) - pl.col(prev)).abs().alias(TempCol.abs_change),
            (pl.col(curr) / pl.col(prev)).alias(TempCol.perc_change),
        ]
    )

    # --------------------------------------------------------
    # Thresholds (eager — intentional)
    # --------------------------------------------------------
    abs_upper, perc_upper = (
        lf.filter(
            (pl.col(IndCqc.care_home) == CareHome.not_care_home)
            & pl.col(prev).is_not_null()
            & pl.col(curr).is_not_null()
            & ((pl.col(prev) > 10) | (pl.col(curr) > 10))
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

    perc_lower = 1 / perc_upper if perc_upper else None

    # --------------------------------------------------------
    # Conditions
    # --------------------------------------------------------
    is_care_home = pl.col(IndCqc.care_home) == CareHome.care_home

    is_small_non_res = (
        (pl.col(IndCqc.care_home) == CareHome.not_care_home)
        & (pl.col(prev) <= 10)
        & (pl.col(curr) <= 10)
    )

    is_valid_non_res = (
        (pl.col(IndCqc.care_home) == CareHome.not_care_home)
        & (pl.col(TempCol.abs_change) <= abs_upper)
        & (pl.col(TempCol.perc_change) <= perc_upper)
        & (pl.col(TempCol.perc_change) >= perc_lower)
    )

    keep = is_care_home | is_small_non_res | is_valid_non_res

    # --------------------------------------------------------
    # Apply cleaning
    # --------------------------------------------------------
    return lf.with_columns(
        [
            pl.when(keep)
            .then(pl.col(prev))
            .otherwise(None)
            .alias(TempCol.previous_period_cleaned),
            pl.when(keep)
            .then(pl.col(curr))
            .otherwise(None)
            .alias(TempCol.current_period_cleaned),
        ]
    ).drop(TempCol.abs_change, TempCol.perc_change)


def calculate_trendline(
    lf: pl.LazyFrame, out_col: str, group_cols: list[str]
) -> pl.LazyFrame:
    """
    Computes a trendline from a sequence of single-period rate of change values, starting at 1.0 in the first period.

    The trendline is then derived by iteratively multiplying each rate of change value, resulting in a cumulative
    measure of change over time.
    This is calculated by taking the exponential of the sum of the logarithms of the values.
    This approach avoids issues in pyspark with direct multiplication of many numbers.

    The input rows are sorted by the grouping columns and import date before the cumulative sum is computed.
    This ensures stable and deterministic trendline values even when input rows arrive out of order.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        out_col (str): The name of the output column for the trendline.
        group_cols (list[str]): The columns to group by.

    Returns:
        pl.LazyFrame: The LazyFrame with the trendline column added.
    """
    return (
        lf.sort(group_cols + [IndCqc.cqc_location_import_date])
        .with_columns(
            pl.col(IndCqc.single_period_rate_of_change)
            .log()
            .cum_sum()
            .over(group_cols)
            .exp()
            .alias(out_col)
        )
        .drop(IndCqc.single_period_rate_of_change)
    )
