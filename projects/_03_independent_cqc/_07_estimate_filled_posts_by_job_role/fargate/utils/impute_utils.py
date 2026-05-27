from typing import Optional

import polars as pl

from polars_utils.expressions import percentage_share
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import PrimaryServiceType


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


def create_ascwds_job_role_rolling_ratio(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Create rolling ASC-WDS job role ratios over a 6-month period.

    The rolling sums are calculated at an aggregated level using the combination of:
    - primary service type
    - estimated filled posts size group
    - cleaned main job role label

    Monthly ASC-WDS job role counts are first pre-aggregated at this level to keep
    processing within the Polars streaming engine and reduce the volume of data used
    in the rolling calculation. The rolling ratio is then calculated on this small
    aggregated dataset before joining back onto the original location-level dataset.
    This is valid because all locations sharing the same primary service type, size
    group, and import date will receive an identical ratio regardless.

    Uses a groupby-aggregate-rolling-ratio-join pattern to improve performance on
    large datasets.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset to calculate ratio on

    Returns:
        pl.LazyFrame: dataset with an additional column containing the rolling
            ASC-WDS job role ratio
    """

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        estimate_filled_posts_size_group_expression()
    )

    rolling_groups = [
        IndCQC.primary_service_type,
        IndCQC.estimate_filled_posts_size_group,
        IndCQC.main_job_role_clean_labelled,
    ]
    order_key = IndCQC.cqc_location_import_date
    monthly_groups = rolling_groups + [order_key]

    # STEP A: Pre-aggregate to monthly totals (~50k rows)
    monthly_totals_lf = estimated_job_role_posts_lf.group_by(monthly_groups).agg(
        pl.col(IndCQC.imputed_ascwds_job_role_counts).sum()
    )

    # STEP B: Sort and compute rolling 6-month sums on small dataset
    rolling_agg_lf = (
        monthly_totals_lf.sort(*rolling_groups, order_key)
        .rolling(index_column=order_key, group_by=rolling_groups, period="6mo")
        .agg(
            pl.col(IndCQC.imputed_ascwds_job_role_counts)
            .sum()
            .alias(IndCQC.ascwds_job_role_rolling_sum)
        )
    )

    # STEP D: Calculate ratio on the small ~50k-row dataset before the join.
    # All locations sharing (primary_service_type, size_group, date) get the
    # same ratio, so this is equivalent to computing it post-join but far cheaper.
    ratio_partition = [
        IndCQC.primary_service_type,
        IndCQC.estimate_filled_posts_size_group,
        order_key,
    ]
    rolling_agg_lf = rolling_agg_lf.with_columns(
        (
            pl.col(IndCQC.ascwds_job_role_rolling_sum)
            / pl.col(IndCQC.ascwds_job_role_rolling_sum).sum().over(ratio_partition)
        )
        .cast(pl.Float32)
        .alias(IndCQC.ascwds_job_role_rolling_ratio)
    ).drop(
        IndCQC.ascwds_job_role_rolling_sum
    )  # drop sum before the join

    # STEP C: Join only the ratio (no sum) back to the 152M-row table
    return estimated_job_role_posts_lf.join(
        rolling_agg_lf,
        on=monthly_groups,
        how="left",
    )
