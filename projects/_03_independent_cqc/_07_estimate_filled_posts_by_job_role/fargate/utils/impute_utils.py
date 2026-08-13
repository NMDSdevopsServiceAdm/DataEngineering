from dataclasses import dataclass, fields
from typing import Optional

import polars as pl

from polars_utils.expressions import percentage_share
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import PrimaryServiceType

# How far a known job role ratio is carried, and the widest gap that is interpolated.
EDGE_FILL_PERIOD: str = "2y"
INTERPOLATION_CAP_PERIOD: str = "5y"

# The grain every fill works within: one workplace's series for one job role.
JOB_ROLE_GROUPS: list[str] = [
    IndCQC.location_id,
    IndCQC.main_job_role_clean_labelled,
]


@dataclass
class TempCols:
    """The names of the temporary columns used while building the trendline."""

    first_known_date: str = "first_known_date"
    last_known_date: str = "last_known_date"
    first_known_value: str = "first_known_value"
    last_known_value: str = "last_known_value"
    previous_known_date: str = "previous_known_date"
    next_known_date: str = "next_known_date"
    ratio_total: str = "ratio_total"
    contributing_rows: str = "contributing_rows"


def create_imputed_ascwds_job_role_counts(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Impute job role ratios per location and job role by interpolation, forward fill,
    and backward fill, then broadcast the result back onto every row with `.over()`.

    The frame is sorted by (location_id, job_role, date) before the `.over()` call,
    since its default mapping strategy writes results back in original row order and
    would otherwise misassign values on unsorted input.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset to impute

    Returns:
        pl.LazyFrame: dataset with additional columns with imputed data
    """
    impute_groups = [IndCQC.location_id, IndCQC.main_job_role_clean_labelled]
    order_key = IndCQC.cqc_location_import_date

    estimated_job_role_posts_lf = get_percent_share_ratios(
        estimated_job_role_posts_lf,
        input_col=IndCQC.ascwds_job_role_counts,
        output_col=IndCQC.ascwds_job_role_ratios,
    )

    imputed_ratios = (
        pl.col(IndCQC.ascwds_job_role_ratios)
        .interpolate()
        .forward_fill()
        .backward_fill()
        .over(impute_groups)
        .alias(IndCQC.imputed_ascwds_job_role_ratios)
    )

    # Must be sorted before the .over() call above — see docstring.
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.sort(
        *impute_groups, order_key
    ).with_columns(imputed_ratios)

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        pl.col(IndCQC.estimate_filled_posts)
        .mul(pl.col(IndCQC.imputed_ascwds_job_role_ratios))
        .alias(IndCQC.imputed_ascwds_job_role_counts)
    )
    return estimated_job_role_posts_lf


def get_percent_share_ratios(
    estimated_job_role_posts_lf: pl.LazyFrame,
    input_col: str,
    output_col: str,
    groups: Optional[list[str]] = None,
) -> pl.LazyFrame:
    """
    Calculate ratios over location and date, broadcasting via `.over()`.

    `.over()` computes and writes its result in place; the equivalent groupby-agg-
    explode-join would cost more peak memory for the same reason a join-based
    `.over()` replacement does (see the `over-vs-join` skill).

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset to calculate ratios over. Must contain location_id and cqc_location_import_date_columns for grouping
        input_col(str): column on which to calculate percentage share
        output_col(str): name of new column containing percentage share
        groups(Optional[list[str]]): list of columns to group by

    Returns:
        pl.LazyFrame: dataset with new column containing percentage share
    """
    if groups is None:
        groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]

    return estimated_job_role_posts_lf.with_columns(
        percentage_share(input_col).cast(pl.Float32).over(groups).alias(output_col)
    )


ESTIMATE_FILLED_POSTS_SIZE_GROUPS = {
    PrimaryServiceType.care_home_only: [
        (1, "COH 1 to 9"),
        (10, "COH 10 to 19"),
        (20, "COH 20 to 29"),
        (30, "COH 30 plus"),
    ],
    PrimaryServiceType.care_home_with_nursing: [
        (1, "CHWN 1 to 19"),
        (20, "CHWN 20 to 29"),
        (30, "CHWN 30 plus"),
    ],
    PrimaryServiceType.non_residential: [
        (1, "NR 1 to 24"),
        (25, "NR 25 to 49"),
        (50, "NR 50 to 74"),
        (75, "NR 75 to 99"),
        (100, "NR 100 plus"),
    ],
}


def estimate_filled_posts_size_group_expression() -> pl.Expr:
    """Create the expression to calculate the size group for estimated filled posts.
    This function recursively builds a polars expression, using the
    ESTIMATE_FILLED_POSTS_SIZE_GROUPS data structure adding a check
    for each combination of:
       - service type,
       - upper size bound, and
       - lower size bound.

    Returns:
        pl.Expr: A polars expression to calculate the size group for
            estimated filled posts.
    """
    estimate_col = pl.col(IndCQC.estimate_filled_posts)
    primary_col = pl.col(IndCQC.primary_service_type)

    expr = pl.lit(None)

    for service_type, buckets in ESTIMATE_FILLED_POSTS_SIZE_GROUPS.items():
        for i, (lower, label) in enumerate(buckets):
            upper = buckets[i + 1][0] if i + 1 < len(buckets) else None

            condition = (primary_col == service_type) & (estimate_col >= lower)

            if upper is not None:
                condition = condition & (estimate_col < upper)

            expr = pl.when(condition).then(pl.lit(label)).otherwise(expr)

    return expr.alias(IndCQC.estimate_filled_posts_size_group)


def add_fill_boundaries(estimated_job_role_posts_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add the first and last known ratio and date, and the nearest known date either side of
    every row, for each workplace and job role.

    The masked `max` broadcasts a single non-null value onto every row of the group.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset containing job role ratios

    Returns:
        pl.LazyFrame: dataset with the boundary columns added
    """
    order_key = IndCQC.cqc_location_import_date

    is_known = pl.col(IndCQC.ascwds_job_role_ratios).is_not_null()
    known_date = pl.when(is_known).then(pl.col(order_key))

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        known_date.min().over(JOB_ROLE_GROUPS).alias(TempCols.first_known_date),
        known_date.max().over(JOB_ROLE_GROUPS).alias(TempCols.last_known_date),
        known_date.forward_fill()
        .over(JOB_ROLE_GROUPS, order_by=order_key)
        .alias(TempCols.previous_known_date),
        known_date.backward_fill()
        .over(JOB_ROLE_GROUPS, order_by=order_key)
        .alias(TempCols.next_known_date),
    )

    # Reads the boundary dates as columns rather than repeating their expressions, which would
    # make Polars compute those two windows a second time.
    return estimated_job_role_posts_lf.with_columns(
        pl.when(is_known & (pl.col(order_key) == pl.col(TempCols.first_known_date)))
        .then(pl.col(IndCQC.ascwds_job_role_ratios))
        .max()
        .over(JOB_ROLE_GROUPS)
        .alias(TempCols.first_known_value),
        pl.when(is_known & (pl.col(order_key) == pl.col(TempCols.last_known_date)))
        .then(pl.col(IndCQC.ascwds_job_role_ratios))
        .max()
        .over(JOB_ROLE_GROUPS)
        .alias(TempCols.last_known_value),
    )


def add_imputed_job_role_ratios_for_trendline(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Impute job role ratios within time limits, for use by the trendline only.

    Gaps are interpolated by date if they span no more than `INTERPOLATION_CAP_PERIOD`, and the
    first and last known values are carried outside the known range for no more than
    `EDGE_FILL_PERIOD`.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset containing job role ratios

    Returns:
        pl.LazyFrame: dataset with an additional column of ratios for the trendline
    """
    order_key = IndCQC.cqc_location_import_date

    estimated_job_role_posts_lf = add_fill_boundaries(estimated_job_role_posts_lf)

    within_interpolation_cap = pl.col(TempCols.next_known_date) <= pl.col(
        TempCols.previous_known_date
    ).dt.offset_by(INTERPOLATION_CAP_PERIOD)

    interpolated = (
        pl.col(IndCQC.ascwds_job_role_ratios)
        .interpolate_by(pl.col(order_key))
        .over(JOB_ROLE_GROUPS, order_by=order_key)
    )

    within_forward_fill = (pl.col(order_key) > pl.col(TempCols.last_known_date)) & (
        pl.col(order_key)
        <= pl.col(TempCols.last_known_date).dt.offset_by(EDGE_FILL_PERIOD)
    )
    within_backward_fill = (pl.col(order_key) < pl.col(TempCols.first_known_date)) & (
        pl.col(order_key)
        >= pl.col(TempCols.first_known_date).dt.offset_by(f"-{EDGE_FILL_PERIOD}")
    )

    return estimated_job_role_posts_lf.with_columns(
        pl.coalesce(
            pl.col(IndCQC.ascwds_job_role_ratios),
            pl.when(within_interpolation_cap).then(interpolated),
            pl.when(within_forward_fill)
            .then(pl.col(TempCols.last_known_value))
            .when(within_backward_fill)
            .then(pl.col(TempCols.first_known_value)),
        )
        .cast(pl.Float32)
        .alias(IndCQC.imputed_job_role_ratios_for_trendline)
    )


def create_ascwds_job_role_rolling_ratio(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Create rolling ASC-WDS job role ratios over a 6-month period.

    The ratio is the mean trendline job role share across the workplaces contributing to a primary
    service type, estimated filled posts size group and cleaned main job role label. Each
    workplace counts once per month it contributes, regardless of size.

    Ratios are summed and counted separately so the rolling window divides one total by the
    other. The result sums to 1 across job roles without explicit normalisation, because a
    workplace has all of its job role ratios populated or none of them.

    Pre-aggregating before the rolling calculation keeps processing within the Polars streaming
    engine, and is valid because every location sharing a service type, size group and import
    date receives the same ratio.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset to calculate ratio on

    Returns:
        pl.LazyFrame: dataset with an additional column containing the rolling
            ASC-WDS job role ratio
    """

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        estimate_filled_posts_size_group_expression()
    )

    estimated_job_role_posts_lf = add_imputed_job_role_ratios_for_trendline(
        estimated_job_role_posts_lf
    )

    rolling_groups = [
        IndCQC.primary_service_type,
        IndCQC.estimate_filled_posts_size_group,
        IndCQC.main_job_role_clean_labelled,
    ]
    order_key = IndCQC.cqc_location_import_date
    monthly_groups = rolling_groups + [order_key]

    # STEP A: Total the ratios and count the contributing workplaces per month.
    # polars_streaming: groupby-agg pre-aggregation workaround; data reduction allows streaming but limits flexibility
    monthly_totals_lf = estimated_job_role_posts_lf.group_by(monthly_groups).agg(
        pl.col(IndCQC.imputed_job_role_ratios_for_trendline)
        .sum()
        .alias(TempCols.ratio_total),
        pl.col(IndCQC.imputed_job_role_ratios_for_trendline)
        .is_not_null()
        .sum()
        .alias(TempCols.contributing_rows),
    )

    # STEP B: Roll both totals over 6 months on the small dataset.
    # polars_streaming: .rolling() with groupby requires pre-aggregation workaround; could use .over() for grouped rolling windows when streaming is supported
    rolling_agg_lf = (
        monthly_totals_lf.sort(*rolling_groups, order_key)
        .rolling(index_column=order_key, group_by=rolling_groups, period="6mo")
        .agg(
            pl.col(TempCols.ratio_total).sum(),
            pl.col(TempCols.contributing_rows).sum(),
        )
    )

    # STEP C: Divide one total by the other, carrying the nearest known ratio into any group
    # with no contributing workplaces.
    rolling_agg_lf = rolling_agg_lf.with_columns(
        pl.when(pl.col(TempCols.contributing_rows) > 0)
        .then(pl.col(TempCols.ratio_total) / pl.col(TempCols.contributing_rows))
        .cast(pl.Float32)
        .alias(IndCQC.ascwds_job_role_rolling_ratio)
    ).with_columns(
        pl.col(IndCQC.ascwds_job_role_rolling_ratio)
        .forward_fill()
        .backward_fill()
        .over(rolling_groups, order_by=order_key)
    )

    rolling_agg_lf = rolling_agg_lf.drop(
        TempCols.ratio_total, TempCols.contributing_rows
    )

    # STEP D: Join back to the location level dataset and drop the temporary columns. Field
    # defaults hold the column names, which differ from the field names.
    columns_to_drop = [field.default for field in fields(TempCols)]

    return estimated_job_role_posts_lf.join(
        rolling_agg_lf,
        on=monthly_groups,
        how="left",
    ).drop(*columns_to_drop, strict=False)
