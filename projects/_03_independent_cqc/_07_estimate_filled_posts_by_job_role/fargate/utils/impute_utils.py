from dataclasses import fields
from typing import Optional

import polars as pl

from polars_utils.expressions import percentage_share
from projects._03_independent_cqc.utils.imputation.extrapolation import (
    model_extrapolation,
)
from projects._03_independent_cqc.utils.imputation.interpolation import (
    model_interpolation,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    JobRoleImputeTempColumns as TempCols,
)
from utils.column_values.categorical_column_values import PrimaryServiceType

JOB_ROLE_GROUPS: list[str] = [
    IndCQC.location_id,
    IndCQC.main_job_role_clean_labelled,
]


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

    Each known value is copied onto every row of its group by masking out the other rows, then
    taking `.max()` over the group. Split into two `.with_columns()` calls so the second can read
    the boundary dates back as columns, instead of repeating the expressions that built them and
    making Polars compute those windows twice.

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
    extrapolation_period: str,
    interpolation_cap_period: str,
) -> pl.LazyFrame:
    """
    Impute job role ratios within time limits, for use by the trendline only.

    Gaps are interpolated by date if they span no more than `interpolation_cap_period`, and the
    first and last known values are carried outside the known range for no more than
    `extrapolation_period`.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset containing job role ratios
        extrapolation_period(str): how far to carry the first/last known value outside the known
            range, as a Polars offset string (e.g. "2y")
        interpolation_cap_period(str): the widest gap to interpolate across, as a Polars offset
            string (e.g. "5y")

    Returns:
        pl.LazyFrame: dataset with an additional column of ratios for the trendline
    """
    order_key = IndCQC.cqc_location_import_date

    estimated_job_role_posts_lf = add_fill_boundaries(estimated_job_role_posts_lf)

    within_interpolation_cap = pl.col(TempCols.next_known_date) <= pl.col(
        TempCols.previous_known_date
    ).dt.offset_by(interpolation_cap_period)

    interpolated = (
        pl.col(IndCQC.ascwds_job_role_ratios)
        .interpolate_by(pl.col(order_key))
        .over(JOB_ROLE_GROUPS, order_by=order_key)
    )

    within_forward_fill = (pl.col(order_key) > pl.col(TempCols.last_known_date)) & (
        pl.col(order_key)
        <= pl.col(TempCols.last_known_date).dt.offset_by(extrapolation_period)
    )
    within_backward_fill = (pl.col(order_key) < pl.col(TempCols.first_known_date)) & (
        pl.col(order_key)
        >= pl.col(TempCols.first_known_date).dt.offset_by(f"-{extrapolation_period}")
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
    extrapolation_period: str,
    interpolation_cap_period: str,
) -> pl.LazyFrame:
    """
    Create rolling ASC-WDS job role ratios over a 6-month period.

    The ratio is the mean trendline job role share across the workplaces contributing to a
    primary service type, size group and job role, counting each workplace once regardless of
    size. Ratios sum to 1 across job roles without extra normalisation, because a workplace has
    every job role ratio populated or none of them.

    Steps:
        1. Total the ratios and count the contributing workplaces per month, pre-aggregated so
           the calculation stays within the Polars streaming engine.
        2. Roll both totals over a 6-month window on this small aggregated dataset.
        3. Divide one total by the other, carrying the nearest known ratio into any group with
           no contributing workplaces.
        4. Join the ratio back onto the location-level dataset and drop the temporary columns
           (matched by their string values, since those now differ from the attribute names).

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset to calculate ratio on
        extrapolation_period(str): passed through to `add_imputed_job_role_ratios_for_trendline`
        interpolation_cap_period(str): passed through to
            `add_imputed_job_role_ratios_for_trendline`

    Returns:
        pl.LazyFrame: dataset with an additional column containing the rolling
            ASC-WDS job role ratio
    """

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        estimate_filled_posts_size_group_expression()
    )

    estimated_job_role_posts_lf = add_imputed_job_role_ratios_for_trendline(
        estimated_job_role_posts_lf,
        extrapolation_period=extrapolation_period,
        interpolation_cap_period=interpolation_cap_period,
    )

    rolling_groups = [
        IndCQC.primary_service_type,
        IndCQC.estimate_filled_posts_size_group,
        IndCQC.main_job_role_clean_labelled,
    ]
    order_key = IndCQC.cqc_location_import_date
    monthly_groups = rolling_groups + [order_key]

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

    # polars_streaming: .rolling() with groupby requires pre-aggregation workaround; could use .over() for grouped rolling windows when streaming is supported
    rolling_agg_lf = (
        monthly_totals_lf.sort(*rolling_groups, order_key)
        .rolling(index_column=order_key, group_by=rolling_groups, period="6mo")
        .agg(
            pl.col(TempCols.ratio_total).sum(),
            pl.col(TempCols.contributing_rows).sum(),
        )
    )

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

    columns_to_drop = [field.default for field in fields(TempCols)]

    return estimated_job_role_posts_lf.join(
        rolling_agg_lf,
        on=monthly_groups,
        how="left",
    ).drop(*columns_to_drop, strict=False)


def add_imputed_ascwds_job_role_ratios(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Impute job role ratios by carrying each workplace's own known split along the rolling
    ratio trendline.

    The trendline is `ascwds_job_role_rolling_ratio`: the mean ratio across workplaces
    sharing a primary service type, size band and job role, rolled over a 6-month window. A
    workplace's own known ratio is carried along the trendline's nominal *change*, not its
    absolute value — e.g. if the rolling average for "non-res, 1 to 24 employees, care
    worker" rises by 0.1 percentage points from month 1 to month 2, a workplace with a known
    ratio of 0.3 in month 1 is imputed at 0.4 in month 2.

    Ratios follow the nominal change in the trendline, uncapped in both directions, with
    interior gaps apportioned by days rather than by rows. Imputed values are floored at zero
    and re-shared across job roles, since flooring is what breaks their total of 1. A
    workplace's own submitted ratios are left alone: they already total 1, so re-sharing them
    would only move them by a float rounding step.

    Requires `ascwds_job_role_rolling_ratio` to be non-null wherever `ascwds_job_role_ratios`
    is null: a null there would leave a workplace needing imputation with no split at all.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset containing `ascwds_job_role_ratios`
            and the `ascwds_job_role_rolling_ratio` trendline

    Returns:
        pl.LazyFrame: dataset with an additional column of imputed job role ratios
    """
    estimated_job_role_posts_lf = model_extrapolation(
        estimated_job_role_posts_lf,
        column_with_null_values=IndCQC.ascwds_job_role_ratios,
        model_to_extrapolate_from=IndCQC.ascwds_job_role_rolling_ratio,
        extrapolation_method="nominal",
        group_columns=JOB_ROLE_GROUPS,
    )

    estimated_job_role_posts_lf = model_interpolation(
        estimated_job_role_posts_lf,
        column_with_null_values=IndCQC.ascwds_job_role_ratios,
        method="trend",
        group_columns=JOB_ROLE_GROUPS,
    )

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        pl.when(pl.col(IndCQC.ascwds_job_role_ratios).is_null())
        .then(
            pl.coalesce(IndCQC.extrapolation_model, IndCQC.interpolation_model).clip(
                lower_bound=0
            )
        )
        # Trend interpolation returns Float64, so cast before this lands on the full frame.
        .cast(pl.Float32)
        .alias(TempCols.unnormalised_ratios)
    ).drop(
        IndCQC.extrapolation_forwards,
        IndCQC.extrapolation_model,
        IndCQC.interpolation_model,
    )

    estimated_job_role_posts_lf = get_percent_share_ratios(
        estimated_job_role_posts_lf,
        input_col=TempCols.unnormalised_ratios,
        output_col=IndCQC.imputed_ascwds_job_role_ratios,
    )

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        pl.coalesce(
            IndCQC.ascwds_job_role_ratios, IndCQC.imputed_ascwds_job_role_ratios
        ).alias(IndCQC.imputed_ascwds_job_role_ratios)
    )

    return estimated_job_role_posts_lf.drop(TempCols.unnormalised_ratios)


def add_imputed_ascwds_job_role_counts(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Split a workplace's estimated filled posts across job roles at the imputed ratios.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset containing imputed job role ratios

    Returns:
        pl.LazyFrame: dataset with an additional column of imputed job role counts
    """
    return estimated_job_role_posts_lf.with_columns(
        pl.col(IndCQC.estimate_filled_posts)
        .mul(pl.col(IndCQC.imputed_ascwds_job_role_ratios))
        .alias(IndCQC.imputed_ascwds_job_role_counts)
    )
