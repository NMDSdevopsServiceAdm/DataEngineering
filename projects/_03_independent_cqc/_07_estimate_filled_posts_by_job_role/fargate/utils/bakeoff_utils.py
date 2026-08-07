"""
Bake-off comparing upfront fill regimes for the ASC-WDS job role rolling ratio.

Investigation code for ticket 1859. Production `_03_impute` is untouched; this builds a small
side dataset so the rolling ratio produced by each regime can be charted in Tableau before we
commit to one as the trendline for nominal extrapolation.

Two deliberate deviations from production worth knowing about:

- Ratios use `percentage_share_handling_zero_sum` rather than the unguarded `percentage_share`
  production uses. An all-zero submission should already be nulled by `filter_job_role_group_equal_zero`,
  so this ought to be a no-op — but the unguarded version yields NaN, and one NaN would poison every
  rolling sum in its stratum and silently destroy the comparison.
- All fill and cap limits are day-based, never `.forward_fill(limit=n)`. The date axis is monthly for
  roughly the last three financial years and quarterly before that, so a row-based limit would mean
  a different calendar span depending on the era.
"""

from dataclasses import dataclass
from typing import Optional

import polars as pl

from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.impute_utils import (
    estimate_filled_posts_size_group_expression,
)
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    percentage_share_handling_zero_sum,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

IMPORT_DATE: str = IndCQC.cqc_location_import_date
JOB_ROLE_GROUPS: list[str] = [IndCQC.location_id, IndCQC.main_job_role_clean_labelled]

# Gap between two known values beyond which we stop believing a straight line between them.
# Deliberately not a bake-off axis: the null_interior_rows diagnostic measures what raising it
# would buy, and we decide from the charts.
INTERPOLATION_CAP_DAYS: int = 730

ROLLING_WINDOWS: tuple[str, ...] = ("6mo", "12mo")


@dataclass(frozen=True)
class BakeoffCols:
    """The names of the columns produced by the bake-off."""

    variant: str = "variant"
    window: str = "window"
    ratio: str = "job_role_ratio"
    rolling_ratio: str = "rolling_ratio"
    rolling_ratio_change_pp: str = "rolling_ratio_change_pp"
    sum_of_ratios: str = "sum_of_ratios"
    weighted_sum: str = "weighted_sum"
    # Location-months, not distinct workplaces: the source is unique on location, date and
    # role, so a workplace submitting every month counts once per month in the window.
    contributing_rows: str = "contributing_rows"
    known_rows: str = "known_rows"
    never_submitted_rows: str = "never_submitted_rows"
    interpolated_rows: str = "interpolated_rows"
    filled_rows: str = "filled_rows"
    null_interior_rows: str = "null_interior_rows"
    null_edge_rows: str = "null_edge_rows"


@dataclass(frozen=True)
class TempCols:
    """The names of the temporary columns used while building the variants."""

    first_known_date: str = "_first_known_date"
    last_known_date: str = "_last_known_date"
    first_known_value: str = "_first_known_value"
    last_known_value: str = "_last_known_value"
    prev_known_date: str = "_prev_known_date"
    next_known_date: str = "_next_known_date"
    interpolated: str = "_interpolated"
    is_interior: str = "_is_interior"


@dataclass(frozen=True)
class Variant:
    """
    One upfront fill regime to compare.

    Attributes:
        name (str): Label emitted in the `variant` column.
        fill_days (Optional[int]): Symmetric edge fill limit in days. 0 disables the fill
            entirely; None fills indefinitely.
        weighted (bool): True to weight each workplace by its estimated filled posts, False
            to treat every submission equally.
        legacy (bool): True to reproduce production exactly — positional `.interpolate()`
            with no cap, then unlimited forward and backward fill.
    """

    name: str
    fill_days: Optional[int]
    weighted: bool
    legacy: bool


# `base` is today's production behaviour. The five that follow are unweighted, date-aware and
# capped, so `base -> indefinite` isolates the weighting and cap change while
# `indefinite -> the rest` isolates the edge fill, which is the axis under investigation.
VARIANTS: tuple[Variant, ...] = (
    Variant("base", fill_days=None, weighted=True, legacy=True),
    Variant("indefinite", fill_days=None, weighted=False, legacy=False),
    # Variant("none", fill_days=0, weighted=False, legacy=False),
    # Variant("fill_6m", fill_days=183, weighted=False, legacy=False),
    # Variant("fill_12m", fill_days=365, weighted=False, legacy=False),
    # Variant("fill_24m", fill_days=730, weighted=False, legacy=False),
)


def measure_col(variant: Variant, measure: str) -> str:
    """
    Build the wide-format column name holding one measure for one variant.

    Args:
        variant (Variant): The variant the measure belongs to.
        measure (str): The measure name, from BakeoffCols.

    Returns:
        str: The combined column name.
    """
    return f"{variant.name}__{measure}"


def add_job_role_ratios(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add each job role's share of its workplace's total ASC-WDS count for that import date.

    Args:
        lf (pl.LazyFrame): Cleaned job role dataset, one row per location, import date and role.

    Returns:
        pl.LazyFrame: The dataset with the job role ratio column added.
    """
    return lf.with_columns(
        percentage_share_handling_zero_sum(IndCQC.ascwds_job_role_counts)
        .over([IndCQC.location_id, IMPORT_DATE])
        .cast(pl.Float32)
        .alias(BakeoffCols.ratio)
    )


def add_fill_boundaries(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add the per location and job role boundaries every variant is built from.

    Adds the first and last dates a ratio was known and the values observed on them, the
    nearest known dates either side of each row, and whether the row sits inside the known
    range (so interior gaps can be told apart from the leading and trailing edges).

    Args:
        lf (pl.LazyFrame): Dataset with the job role ratio column present.

    Returns:
        pl.LazyFrame: The dataset with the temporary boundary columns added.
    """
    is_known = pl.col(BakeoffCols.ratio).is_not_null()
    known_date = pl.when(is_known).then(pl.col(IMPORT_DATE))

    first_known_date = known_date.min().over(JOB_ROLE_GROUPS)
    last_known_date = known_date.max().over(JOB_ROLE_GROUPS)

    return lf.with_columns(
        first_known_date.alias(TempCols.first_known_date),
        last_known_date.alias(TempCols.last_known_date),
        pl.when(is_known & (pl.col(IMPORT_DATE) == first_known_date))
        .then(pl.col(BakeoffCols.ratio))
        .max()
        .over(JOB_ROLE_GROUPS)
        .alias(TempCols.first_known_value),
        pl.when(is_known & (pl.col(IMPORT_DATE) == last_known_date))
        .then(pl.col(BakeoffCols.ratio))
        .max()
        .over(JOB_ROLE_GROUPS)
        .alias(TempCols.last_known_value),
        known_date.forward_fill()
        .over(JOB_ROLE_GROUPS, order_by=IMPORT_DATE)
        .alias(TempCols.prev_known_date),
        known_date.backward_fill()
        .over(JOB_ROLE_GROUPS, order_by=IMPORT_DATE)
        .alias(TempCols.next_known_date),
    ).with_columns(
        (
            (pl.col(IMPORT_DATE) > pl.col(TempCols.first_known_date))
            & (pl.col(IMPORT_DATE) < pl.col(TempCols.last_known_date))
        ).alias(TempCols.is_interior)
    )


def add_capped_interpolation(lf: pl.LazyFrame, cap_days: int) -> pl.LazyFrame:
    """
    Add date-aware interpolated values, only where the gap being spanned is short enough.

    Uses `interpolate_by` rather than `interpolate` so values are placed by date rather than by
    row position — which matters because the import date axis switches from quarterly to monthly
    partway through the series.

    Args:
        lf (pl.LazyFrame): Dataset with the boundary columns present.
        cap_days (int): Longest gap between two known values that will still be interpolated.

    Returns:
        pl.LazyFrame: The dataset with the interpolated column added.
    """
    gap_days = (
        pl.col(TempCols.next_known_date) - pl.col(TempCols.prev_known_date)
    ).dt.total_days()

    interpolated = (
        pl.col(BakeoffCols.ratio)
        .interpolate_by(pl.col(IMPORT_DATE))
        .over(JOB_ROLE_GROUPS, order_by=IMPORT_DATE)
    )

    return lf.with_columns(
        pl.when(gap_days <= cap_days)
        .then(interpolated)
        .cast(pl.Float32)
        .alias(TempCols.interpolated)
    )


def variant_expression(variant: Variant) -> pl.Expr:
    """
    Build the filled ratio expression for one variant.

    Non-legacy variants coalesce the known value, then the capped interpolation, then an
    edge fill applied only outside the known range and only within the variant's day limit.

    Args:
        variant (Variant): The variant to build an expression for.

    Returns:
        pl.Expr: The filled ratio, aliased to the variant's ratio column.
    """
    ratio_col = measure_col(variant, BakeoffCols.ratio)

    if variant.legacy:
        return (
            pl.col(BakeoffCols.ratio)
            .interpolate()
            .forward_fill()
            .backward_fill()
            .over(JOB_ROLE_GROUPS, order_by=IMPORT_DATE)
            .cast(pl.Float32)
            .alias(ratio_col)
        )

    candidates = [pl.col(BakeoffCols.ratio), pl.col(TempCols.interpolated)]

    if variant.fill_days != 0:
        after_last = pl.col(IMPORT_DATE) > pl.col(TempCols.last_known_date)
        before_first = pl.col(IMPORT_DATE) < pl.col(TempCols.first_known_date)

        if variant.fill_days is not None:
            after_last = after_last & (
                (pl.col(IMPORT_DATE) - pl.col(TempCols.last_known_date)).dt.total_days()
                <= variant.fill_days
            )
            before_first = before_first & (
                (
                    pl.col(TempCols.first_known_date) - pl.col(IMPORT_DATE)
                ).dt.total_days()
                <= variant.fill_days
            )

        candidates.append(
            pl.when(after_last)
            .then(pl.col(TempCols.last_known_value))
            .when(before_first)
            .then(pl.col(TempCols.first_known_value))
        )

    return pl.coalesce(candidates).cast(pl.Float32).alias(ratio_col)


def add_variant_ratios(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add one filled ratio column per variant.

    Args:
        lf (pl.LazyFrame): Dataset with the boundary and interpolated columns present.

    Returns:
        pl.LazyFrame: The dataset with a filled ratio column for every variant.
    """
    return lf.with_columns([variant_expression(variant) for variant in VARIANTS])


def variant_aggregations(variant: Variant) -> list[pl.Expr]:
    """
    Build the aggregation expressions describing one variant within a stratum and month.

    Alongside the value totals these count how each row came to hold (or not hold) a value,
    so the charts can distinguish a trendline built on real submissions from one built on
    synthetic ones.

    The counts are mutually exclusive and, together with `never_submitted_rows`, add up to
    the row total — locations that never submitted have no known range at all, so they fall
    outside the interior and edge counts rather than landing in one of them.

    Args:
        variant (Variant): The variant to aggregate.

    Returns:
        list[pl.Expr]: Aggregation expressions for use inside a group_by.
    """
    filled = pl.col(measure_col(variant, BakeoffCols.ratio))
    was_known = pl.col(BakeoffCols.ratio).is_not_null()
    interior = pl.col(TempCols.is_interior)

    aggregations = [
        filled.sum()
        .cast(pl.Float64)
        .alias(measure_col(variant, BakeoffCols.sum_of_ratios)),
        filled.is_not_null()
        .sum()
        .cast(pl.UInt32)
        .alias(measure_col(variant, BakeoffCols.contributing_rows)),
        (~was_known & filled.is_not_null() & interior)
        .sum()
        .cast(pl.UInt32)
        .alias(measure_col(variant, BakeoffCols.interpolated_rows)),
        (~was_known & filled.is_not_null() & ~interior)
        .sum()
        .cast(pl.UInt32)
        .alias(measure_col(variant, BakeoffCols.filled_rows)),
        (filled.is_null() & interior)
        .sum()
        .cast(pl.UInt32)
        .alias(measure_col(variant, BakeoffCols.null_interior_rows)),
        (filled.is_null() & ~interior)
        .sum()
        .cast(pl.UInt32)
        .alias(measure_col(variant, BakeoffCols.null_edge_rows)),
    ]

    if variant.weighted:
        aggregations.append(
            (pl.col(IndCQC.estimate_filled_posts) * filled)
            .sum()
            .cast(pl.Float64)
            .alias(measure_col(variant, BakeoffCols.weighted_sum))
        )

    return aggregations


def build_pre_aggregate(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Collapse the location level dataset to one row per stratum, job role and import date.

    This is the only pass over the full dataset — every subsequent step works on the result,
    which is a few tens of thousands of rows rather than tens of millions.

    Args:
        lf (pl.LazyFrame): Dataset with a filled ratio column for every variant.

    Returns:
        pl.LazyFrame: Wide pre-aggregate carrying every variant's measures side by side.
    """
    group_cols = [
        IndCQC.primary_service_type,
        IndCQC.estimate_filled_posts_size_group,
        IndCQC.main_job_role_clean_labelled,
        IMPORT_DATE,
    ]

    # Both are the same for every variant, so they are counted once rather than per variant.
    aggregations: list[pl.Expr] = [
        pl.col(BakeoffCols.ratio)
        .is_not_null()
        .sum()
        .cast(pl.UInt32)
        .alias(BakeoffCols.known_rows),
        pl.col(TempCols.first_known_date)
        .is_null()
        .sum()
        .cast(pl.UInt32)
        .alias(BakeoffCols.never_submitted_rows),
    ]
    for variant in VARIANTS:
        aggregations.extend(variant_aggregations(variant))

    return lf.group_by(group_cols).agg(aggregations)


def collapse_size_groups(pre_aggregate_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Re-aggregate the pre-aggregate up to primary service type, dropping the size group.

    Every measure is a sum or a distinct count over disjoint sets — a location sits in exactly
    one size group on any given date — so this is exact and avoids a second pass over the full
    dataset.

    Args:
        pre_aggregate_lf (pl.LazyFrame): The wide pre-aggregate including size group.

    Returns:
        pl.LazyFrame: The same measures aggregated across size groups.
    """
    group_cols = [
        IndCQC.primary_service_type,
        IndCQC.main_job_role_clean_labelled,
        IMPORT_DATE,
    ]
    measure_cols = [
        col
        for col in pre_aggregate_lf.collect_schema().names()
        if col not in group_cols and col != IndCQC.estimate_filled_posts_size_group
    ]

    return pre_aggregate_lf.group_by(group_cols).agg(
        [pl.col(col).sum() for col in measure_cols]
    )


def to_long_format(
    pre_aggregate_lf: pl.LazyFrame, group_cols: list[str]
) -> pl.LazyFrame:
    """
    Reshape the wide pre-aggregate into one row per variant.

    Long format keeps the Tableau side simple — variant becomes a dimension to colour by rather
    than a set of measures to reconcile.

    Args:
        pre_aggregate_lf (pl.LazyFrame): The wide pre-aggregate.
        group_cols (list[str]): The stratification columns, excluding job role and import date.

    Returns:
        pl.LazyFrame: One row per stratum, job role, import date and variant.
    """
    keys = group_cols + [IndCQC.main_job_role_clean_labelled, IMPORT_DATE]

    per_variant = []
    for variant in VARIANTS:
        weighted_sum = (
            pl.col(measure_col(variant, BakeoffCols.weighted_sum))
            if variant.weighted
            else pl.lit(None, dtype=pl.Float64)
        )
        per_variant.append(
            pre_aggregate_lf.select(
                *keys,
                pl.lit(variant.name).alias(BakeoffCols.variant),
                pl.col(BakeoffCols.known_rows),
                pl.col(BakeoffCols.never_submitted_rows),
                weighted_sum.alias(BakeoffCols.weighted_sum),
                *[
                    pl.col(measure_col(variant, measure)).alias(measure)
                    for measure in (
                        BakeoffCols.sum_of_ratios,
                        BakeoffCols.contributing_rows,
                        BakeoffCols.interpolated_rows,
                        BakeoffCols.filled_rows,
                        BakeoffCols.null_interior_rows,
                        BakeoffCols.null_edge_rows,
                    )
                ],
            )
        )

    return pl.concat(per_variant, how="vertical")


def add_rolling_ratio(
    long_lf: pl.LazyFrame, group_cols: list[str], window: str
) -> pl.LazyFrame:
    """
    Add the rolling job role ratio and its month on month change for one window length.

    Unweighted variants take the mean of workplace shares across the window — summing the
    ratios and dividing by the contributing row count, which cannot be done by averaging the
    monthly averages because months carry different numbers of rows. The weighted variant
    reproduces production, dividing a stratum's rolling total by the same total summed across
    job roles.

    Both self-normalise to sum to 1 across job roles, because every populated workplace and
    date contributes to all 37 roles or to none of them. A stratum with no contributing rows
    at all gets a null rather than a NaN, so it charts as the gap it is.

    Args:
        long_lf (pl.LazyFrame): Long format pre-aggregate.
        group_cols (list[str]): The stratification columns, excluding job role and import date.
        window (str): Rolling window length as a Polars duration, e.g. "6mo".

    Returns:
        pl.LazyFrame: The input with the window, rolling ratio and change columns added.
    """
    series_groups = group_cols + [
        IndCQC.main_job_role_clean_labelled,
        BakeoffCols.variant,
    ]
    stratum_groups = group_cols + [IMPORT_DATE, BakeoffCols.variant]

    def rolling(column: str) -> pl.Expr:
        return (
            pl.col(column)
            .rolling_sum_by(by=IMPORT_DATE, window_size=window)
            .over(series_groups, order_by=IMPORT_DATE)
        )

    long_lf = long_lf.with_columns(
        rolling(BakeoffCols.sum_of_ratios).alias(BakeoffCols.sum_of_ratios),
        rolling(BakeoffCols.contributing_rows).alias(BakeoffCols.contributing_rows),
        rolling(BakeoffCols.weighted_sum).alias(BakeoffCols.weighted_sum),
        *[
            rolling(column).alias(column)
            for column in (
                BakeoffCols.known_rows,
                BakeoffCols.never_submitted_rows,
                BakeoffCols.interpolated_rows,
                BakeoffCols.filled_rows,
                BakeoffCols.null_interior_rows,
                BakeoffCols.null_edge_rows,
            )
        ],
    )

    # A stratum where nobody ever submitted has a genuine zero divisor — Polars sums all-null
    # to 0 rather than null — and an unguarded divide yields NaN, which charts as a value
    # rather than as the gap it actually is.
    weighted_variants = [variant.name for variant in VARIANTS if variant.weighted]
    weighted_total = pl.col(BakeoffCols.weighted_sum).sum().over(stratum_groups)
    weighted_ratio = pl.when(weighted_total != 0).then(
        pl.col(BakeoffCols.weighted_sum) / weighted_total
    )
    unweighted_ratio = pl.when(pl.col(BakeoffCols.contributing_rows) > 0).then(
        pl.col(BakeoffCols.sum_of_ratios) / pl.col(BakeoffCols.contributing_rows)
    )

    long_lf = long_lf.with_columns(
        pl.lit(window).alias(BakeoffCols.window),
        pl.when(pl.col(BakeoffCols.variant).is_in(weighted_variants))
        .then(weighted_ratio)
        .otherwise(unweighted_ratio)
        .cast(pl.Float64)
        .alias(BakeoffCols.rolling_ratio),
    )

    return long_lf.with_columns(
        (
            (
                pl.col(BakeoffCols.rolling_ratio)
                - pl.col(BakeoffCols.rolling_ratio).shift(1)
            )
            * 100
        )
        .over(series_groups, order_by=IMPORT_DATE)
        .alias(BakeoffCols.rolling_ratio_change_pp)
    )


def build_bakeoff(
    pre_aggregate_lf: pl.LazyFrame, group_cols: list[str]
) -> pl.LazyFrame:
    """
    Turn a pre-aggregate into the final long output, one block per rolling window.

    Args:
        pre_aggregate_lf (pl.LazyFrame): Wide pre-aggregate at the required stratification.
        group_cols (list[str]): The stratification columns, excluding job role and import date.

    Returns:
        pl.LazyFrame: One row per stratum, job role, import date, variant and window.
    """
    long_lf = to_long_format(pre_aggregate_lf, group_cols)

    return pl.concat(
        [add_rolling_ratio(long_lf, group_cols, window) for window in ROLLING_WINDOWS],
        how="vertical",
    )


def prepare_variants(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Run every location level step, from raw counts through to a filled ratio per variant.

    Args:
        lf (pl.LazyFrame): Cleaned job role dataset, one row per location, import date and role.

    Returns:
        pl.LazyFrame: The dataset with the size group, boundary and per variant ratio columns.
    """
    lf = lf.with_columns(estimate_filled_posts_size_group_expression())
    lf = add_job_role_ratios(lf)
    lf = add_fill_boundaries(lf)
    lf = add_capped_interpolation(lf, INTERPOLATION_CAP_DAYS)
    return add_variant_ratios(lf)
