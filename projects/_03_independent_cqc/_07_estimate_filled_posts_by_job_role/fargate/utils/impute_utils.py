import polars as pl

from polars_utils.expressions import percentage_share
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


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
) -> pl.LazyFrame:
    """
    Calculate ratios over location and date using groupby-agg-explode pattern.

    Using groupby-agg-explode ensures it can be processed with the streaming engine.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset to calculate ratios over. Must contain location_id and cqc_location_import_date_columns for grouping
        input_col(str): column on which to calculate percentage share
        output_col(str): name of new column containing percentage share

    Returns:
        pl.LazyFrame: dataset with new column containing percentage share
    """
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


def create_ascwds_job_role_rolling_ratio(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Create the rolling ratio for ascwds job role values

    Uses groupby-agg-explode pattern to keep processing within polars streaming
    engine.

    Args:
        estimated_job_role_posts_lf(pl.LazyFrame): dataset to calutate ratio on

    Returns:
        pl.LazyFrame: dataset with additional columns with ratio and sum of ascwds job roles
    """
    rolling_groups = [IndCQC.primary_service_type, IndCQC.main_job_role_clean_labelled]
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
        .rolling(index_column=order_key, group_by=rolling_groups, period="6mo")
        .agg(
            pl.col(IndCQC.imputed_ascwds_job_role_counts)
            .sum()
            .alias(IndCQC.ascwds_job_role_rolling_sum)
        )
    )

    # STEP C: Join the rolling sum back to the main 152M row table
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
        rolling_agg_lf,
        on=monthly_groups,
        how="left",
    )
    # STEP D: Calculate ascwds_job_role_rolling_ratio
    estimated_job_role_posts_lf = get_percent_share_ratios(
        estimated_job_role_posts_lf,
        input_col=IndCQC.ascwds_job_role_rolling_sum,
        output_col=IndCQC.ascwds_job_role_rolling_ratio,
    )
    return estimated_job_role_posts_lf
