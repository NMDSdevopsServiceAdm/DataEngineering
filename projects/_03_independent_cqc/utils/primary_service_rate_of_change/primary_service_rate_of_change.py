import polars as pl

from projects._03_independent_cqc.utils.imputation.interpolation import (
    model_interpolation,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_names.ind_cqc_pipeline_columns import (
    PrimaryServiceRateOfChangeColumns as TempCol,
)
from utils.column_values.categorical_column_values import CareHome


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

    group_cols = [
        IndCqc.primary_service_type,
        IndCqc.number_of_beds_banded_roc,
    ]

    # --------------------------------------------------------
    # Prepare
    # --------------------------------------------------------
    lf = (
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
        .sort([IndCqc.location_id, IndCqc.cqc_location_import_date])
        .with_columns(
            pl.count()
            .over([IndCqc.location_id, IndCqc.care_home])
            .alias(TempCol.submission_count)
        )
    )

    # --------------------------------------------------------
    # Eligibility filter (inline — simple + obvious)
    # --------------------------------------------------------
    lf = lf.filter(
        (pl.col(IndCqc.care_home_status_count) == 1)
        & (pl.col(TempCol.submission_count) >= 2)
    )

    # --------------------------------------------------------
    # Interpolation
    # --------------------------------------------------------
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
        ).alias(TempCol.current_period_interpolated)
    )

    # --------------------------------------------------------
    # Previous value (lag)
    # --------------------------------------------------------
    lf = lf.with_columns(
        pl.col(TempCol.current_period_interpolated)
        .shift(1)
        .over(IndCqc.location_id)
        .alias(TempCol.previous_period_interpolated)
    )

    # --------------------------------------------------------
    # Cleaning (kept as function — real logic)
    # --------------------------------------------------------
    lf = clean_non_residential_rate_of_change(lf)

    # --------------------------------------------------------
    # Rolling aggregation (core logic)
    # --------------------------------------------------------
    lf = (
        lf.filter(
            pl.col(TempCol.current_period_cleaned).is_not_null()
            & pl.col(TempCol.previous_period_cleaned).is_not_null()
        )
        .groupby_rolling(
            index_column=IndCqc.cqc_location_import_date,
            period=f"{days}d",
            by=group_cols,
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
    )

    # --------------------------------------------------------
    # Rate of change
    # --------------------------------------------------------
    lf = lf.with_columns(
        pl.when(pl.col(TempCol.rolling_previous_sum) != 0)
        .then(
            pl.col(TempCol.rolling_current_sum) / pl.col(TempCol.rolling_previous_sum)
        )
        .otherwise(None)
        .alias(TempCol.single_period_rate_of_change)
    )

    # --------------------------------------------------------
    # Trendline (log-sum-exp)
    # --------------------------------------------------------
    lf = lf.with_columns(
        pl.exp(
            pl.col(TempCol.single_period_rate_of_change).log().cumsum().over(group_cols)
        ).alias(out_col)
    ).drop(TempCol.single_period_rate_of_change)

    # --------------------------------------------------------
    # Final shape
    # --------------------------------------------------------
    return lf.drop(IndCqc.number_of_beds_banded_roc).with_columns(
        pl.col(out_col).fill_null(1.0)
    )


# ============================================================
# Complex business logic — keep separate
# ============================================================


def clean_non_residential_rate_of_change(
    lf: pl.LazyFrame,
    abs_percentile: float = 0.99,
    perc_percentile: float = 0.99,
) -> pl.LazyFrame:
    """
    Apply percentile-based cleaning for non-residential rows.
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
