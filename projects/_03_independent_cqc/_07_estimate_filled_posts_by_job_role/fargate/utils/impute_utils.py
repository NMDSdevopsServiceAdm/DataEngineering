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

    Rolling sums and ratios are calculated at a cohort level defined by:
        - primary service type
        - estimated filled posts size group
        - cleaned main job role label
        - import date

    This approach computes ratios on the aggregated rolling dataset first,
    then joins the final ratios back to the full location-level dataset.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset to calutate ratio on

    Returns:
        pl.LazyFrame: dataset with additional columns with ratio and sum of
            ascwds job roles
    """
    # STEP 0: feature engineering
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        estimate_filled_posts_size_group_expression()
    )

    # Cohort definition (shared across all steps now)
    rolling_groups = [
        IndCQC.primary_service_type,
        IndCQC.estimate_filled_posts_size_group,
        IndCQC.main_job_role_clean_labelled,
    ]

    order_key = IndCQC.cqc_location_import_date
    monthly_groups = rolling_groups + [order_key]

    # STEP A: Pre-aggregate down to monthly totals
    # (Shrinks 152M rows -> ~50k rows instantly via Hash Aggregation)
    monthly_totals_lf = estimated_job_role_posts_lf.group_by(monthly_groups).agg(
        pl.col(IndCQC.imputed_ascwds_job_role_counts).sum()
    )

    # STEP B: Sort and roll on the small dataset.
    # This .sort() is completely safe because it's only operating on ~50k rows.
    rolling_agg_lf = (
        monthly_totals_lf.sort(*rolling_groups, order_key)
        .rolling(
            index_column=order_key,
            group_by=rolling_groups,
            period="6mo",
        )
        .agg(
            pl.col(IndCQC.imputed_ascwds_job_role_counts)
            .sum()
            .alias(IndCQC.ascwds_job_role_rolling_sum)
        )
    )

    # STEP C: compute ratios ON aggregated rolling dataset
    # denominator is total rolling sum per cohort + time + size/service
    # not using get_percent_share_ratios here because we need to sum over the rolling sum column,
    # not the original counts column.
    ratio_groups = [
        IndCQC.primary_service_type,
        IndCQC.estimate_filled_posts_size_group,
        IndCQC.cqc_location_import_date,
    ]

    denominator_lf = rolling_agg_lf.group_by(ratio_groups).agg(
        pl.col(IndCQC.ascwds_job_role_rolling_sum).sum().alias("rolling_total")
    )

    rolling_with_ratios_lf = rolling_agg_lf.join(
        denominator_lf, on=ratio_groups, how="left"
    ).with_columns(
        (pl.col(IndCQC.ascwds_job_role_rolling_sum) / pl.col("rolling_total")).alias(
            IndCQC.ascwds_job_role_rolling_ratio
        )
    )

    # STEP D: join ratios back to original dataset
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
        rolling_with_ratios_lf.select(
            monthly_groups + [IndCQC.ascwds_job_role_rolling_ratio]
        ),
        on=monthly_groups,
        how="left",
    )

    return estimated_job_role_posts_lf
