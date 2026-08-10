from dataclasses import dataclass, fields
from typing import Optional

import polars as pl

from polars_utils.expressions import percentage_share
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import PrimaryServiceType

# Limits on how far a known job role ratio is trusted when building the trendline. Chosen by the
# ticket 1859 bake-off, which compared these regimes against full production data: 24 months of
# carry-forward plugs most gaps without freezing a workplace's mix indefinitely, and a 60 month
# interpolation cap recovers the bulk of the base that a shorter cap withholds.
#
# Both are calendar durations rather than day counts, and deliberately so. The import date is the
# earliest file in each calendar month, so its day of month drifts; the axis is quarterly before
# roughly the last three financial years; and 365 days falls short of a year across a leap year.
# Day counts and row counts both mean the wrong thing here.
EDGE_FILL_PERIOD: str = "24mo"
INTERPOLATION_CAP_PERIOD: str = "60mo"

# A workplace's series of ratios for one job role, which is the grain every fill works within.
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
    Impute job role ratios by interpolation forward fill and backward fill.

    Uses groupby-agg-explode pattern to keep processing within polars streaming
    engine.

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
        .sort_by(order_key)
        .interpolate()
        .forward_fill()
        .backward_fill()
        .alias(IndCQC.imputed_ascwds_job_role_ratios)
    )

    impute_agg_lf = (
        # polars_streaming: groupby-agg-explode workaround; should be .over() when window functions support streaming
        estimated_job_role_posts_lf.group_by(impute_groups)
        .agg(
            # Sort the join key in the same manner as the imputed values.
            pl.col(IndCQC.id_per_locationid_import_date_job_role).sort_by(order_key),
            imputed_ratios,
        )
        .explode(
            IndCQC.id_per_locationid_import_date_job_role,
            IndCQC.imputed_ascwds_job_role_ratios,
        )
        .drop(impute_groups)
    )

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
        impute_agg_lf, on=IndCQC.id_per_locationid_import_date_job_role, how="left"
    )

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
    Calculate ratios over location and date using groupby-agg-explode pattern.

    Using groupby-agg-explode ensures it can be processed with the streaming engine.

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

    # Groupby-agg-explode on only necessary subset, before joining back on id_per_locationid_import_date_job_role.
    # polars_streaming: groupby-agg-explode workaround; could be replaced with .over() and simpler join when window functions support streaming
    ratios_agg_lf = (
        estimated_job_role_posts_lf.group_by(groups)
        .agg(
            pl.col(
                IndCQC.id_per_locationid_import_date_job_role
            ),  # Keep to align during explode
            percentage_share(input_col).cast(pl.Float32).alias(output_col),
        )
        .explode(IndCQC.id_per_locationid_import_date_job_role, output_col)
        # Drop groups to prevent duplicate columns after join.
        .drop(groups)
    )

    return estimated_job_role_posts_lf.join(
        ratios_agg_lf, on=IndCQC.id_per_locationid_import_date_job_role, how="left"
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
    Add the edges of each workplace's known job role ratios, as columns on every row.

    For each workplace and job role this finds the first and last dates a ratio was known, the
    values observed on those two dates, and the nearest known date either side of every row.
    Together these say where a series starts and ends and how wide the gap containing any given
    row is, which is what both of the time limits are then applied to.

    The first and last known values are picked out with a masked `max` rather than a sort: the
    mask leaves a single non-null value per group, so the `max` is only a way of broadcasting
    that one value onto every row of the group.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset containing job role ratios

    Returns:
        pl.LazyFrame: dataset with the boundary columns added
    """
    order_key = IndCQC.cqc_location_import_date

    is_known = pl.col(IndCQC.ascwds_job_role_ratios).is_not_null()
    known_date = pl.when(is_known).then(pl.col(order_key))
    first_known_date = known_date.min().over(JOB_ROLE_GROUPS)
    last_known_date = known_date.max().over(JOB_ROLE_GROUPS)

    return estimated_job_role_posts_lf.with_columns(
        first_known_date.alias(TempCols.first_known_date),
        last_known_date.alias(TempCols.last_known_date),
        pl.when(is_known & (pl.col(order_key) == first_known_date))
        .then(pl.col(IndCQC.ascwds_job_role_ratios))
        .max()
        .over(JOB_ROLE_GROUPS)
        .alias(TempCols.first_known_value),
        pl.when(is_known & (pl.col(order_key) == last_known_date))
        .then(pl.col(IndCQC.ascwds_job_role_ratios))
        .max()
        .over(JOB_ROLE_GROUPS)
        .alias(TempCols.last_known_value),
        known_date.forward_fill()
        .over(JOB_ROLE_GROUPS, order_by=order_key)
        .alias(TempCols.previous_known_date),
        known_date.backward_fill()
        .over(JOB_ROLE_GROUPS, order_by=order_key)
        .alias(TempCols.next_known_date),
    )


def add_capped_ascwds_job_role_ratios(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Fill job role ratios within time limits, as the input to the trendline.

    This is a deliberately partial fill, separate from `imputed_ascwds_job_role_ratios`. Its
    only purpose is to give the rolling ratio enough base to be stable while still letting it
    move over time, so both limits are calendar bounded rather than open ended:

    - a gap between two known values is interpolated only if it spans no more than
      `INTERPOLATION_CAP_PERIOD`
    - the first and last known values are carried outside the known range for no more than
      `EDGE_FILL_PERIOD`

    Interpolation uses `interpolate_by` rather than `interpolate` so values are positioned by
    date rather than by row, which matters because the import date axis switches from quarterly
    to monthly partway through the series.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset containing job role ratios

    Returns:
        pl.LazyFrame: dataset with an additional column of capped ratios
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
        .alias(IndCQC.ascwds_job_role_ratios_capped)
    )


def create_ascwds_job_role_rolling_ratio(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Create rolling ASC-WDS job role ratios over a 6-month period.

    The ratio is the mean job role share of the workplaces contributing to the combination of:
    - primary service type
    - estimated filled posts size group
    - cleaned main job role label

    Every workplace counts once per month it contributes, regardless of size. This is the right
    weighting because the ratio is later multiplied by a single location's estimated filled
    posts, so what is wanted is the share for a workplace of that type rather than the share of
    all posts in the group, which would bias small workplaces towards the mix of large ones.

    Ratios are summed and counted separately before the rolling window is applied, because
    averaging monthly averages would be wrong when months carry different numbers of workplaces.
    The result sums to 1 across job roles without any explicit normalisation, because a
    workplace either has all of its job role ratios populated or none of them, so the
    contributing row count is identical for every role.

    Monthly totals are pre-aggregated to keep processing within the Polars streaming engine and
    reduce the volume of data used in the rolling calculation. The rolling ratio is then
    calculated on this small aggregated dataset before joining back onto the original
    location-level dataset. This is valid because all locations sharing the same primary service
    type, size group, and import date will receive an identical ratio regardless.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset to calculate ratio on

    Returns:
        pl.LazyFrame: dataset with an additional column containing the rolling
            ASC-WDS job role ratio
    """

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        estimate_filled_posts_size_group_expression()
    )

    estimated_job_role_posts_lf = add_capped_ascwds_job_role_ratios(
        estimated_job_role_posts_lf
    )

    rolling_groups = [
        IndCQC.primary_service_type,
        IndCQC.estimate_filled_posts_size_group,
        IndCQC.main_job_role_clean_labelled,
    ]
    order_key = IndCQC.cqc_location_import_date
    monthly_groups = rolling_groups + [order_key]

    # STEP A: Pre-aggregate to monthly totals and contributing workplace counts.
    # polars_streaming: groupby-agg pre-aggregation workaround; data reduction allows streaming but limits flexibility
    monthly_totals_lf = estimated_job_role_posts_lf.group_by(monthly_groups).agg(
        pl.col(IndCQC.ascwds_job_role_ratios_capped).sum().alias(TempCols.ratio_total),
        pl.col(IndCQC.ascwds_job_role_ratios_capped)
        .is_not_null()
        .sum()
        .alias(TempCols.contributing_rows),
    )

    # STEP B: Sort and compute rolling 6-month sums on small dataset
    # polars_streaming: .rolling() with groupby requires pre-aggregation workaround; could use .over() for grouped rolling windows when streaming is supported
    rolling_agg_lf = (
        monthly_totals_lf.sort(*rolling_groups, order_key)
        .rolling(index_column=order_key, group_by=rolling_groups, period="6mo")
        .agg(
            pl.col(TempCols.ratio_total).sum(),
            pl.col(TempCols.contributing_rows).sum(),
        )
    )

    # STEP C: Calculate the mean share on the significantly smaller aggregated dataset before
    # the join. A group with no contributing workplaces would divide zero by zero, so it is left
    # null and then filled with the group's own nearest known ratio. Job roles all fall null
    # together, so every role fills from the same date and the ratios still sum to 1.
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

    # STEP D: Join the ratio back to the full location level dataset, dropping the temporary
    # columns which exist only to build the trendline. Collect the field defaults rather than
    # the field names, since the two differ here.
    columns_to_drop = [field.default for field in fields(TempCols)]

    return estimated_job_role_posts_lf.join(
        rolling_agg_lf,
        on=monthly_groups,
        how="left",
    ).drop(*columns_to_drop, strict=False)
