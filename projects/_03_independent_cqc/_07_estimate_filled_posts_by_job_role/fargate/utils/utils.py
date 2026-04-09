from typing import Final

import polars as pl

from polars_utils.expressions import percentage_share
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    MainJobRoleLabels,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

# Define constants for IDs for original length data and expanded data.
ROW_ID: Final[str] = "id"
EXPANDED_ID: Final[str] = "expanded_id"


def nullify_job_role_count_when_source_not_ascwds(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Set job role counts to NULL when source is not ASCDWS.

    This is to ensure that we're only using ASCDWS job role data when ASCDWS data has
    been used for estimated filled posts.

    Nullify when the following conditions are NOT met:
    1. Source must be "ascwds_pir_merged"
    2. Estimates must equal the value after ASCWDS dedup_clean step.

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.

    Returns:
        pl.LazyFrame: Transformed LazyFrame with ASCDWS job role counts nullified.
    """
    source_is_ascwds = pl.col(IndCQC.estimate_filled_posts_source) == pl.lit(
        EstimateFilledPostsSource.ascwds_pir_merged
    )
    estimate_matches_ascwds = pl.col(IndCQC.estimate_filled_posts) == pl.col(
        IndCQC.ascwds_filled_posts_dedup_clean
    )

    return lf.with_columns(
        pl.when(source_is_ascwds & estimate_matches_ascwds)
        .then(IndCQC.ascwds_job_role_counts)
        .otherwise(None)
    )


def create_imputed_ascwds_job_role_counts(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Impute job role ratios by interpolation forward fill and backward fill.

    Uses groupby-agg-explode pattern to keep processing within polars streaming
    engine.
    """
    impute_groups = [IndCQC.location_id, IndCQC.main_job_role_clean_labelled]
    order_key = IndCQC.cqc_location_import_date

    estimated_job_role_posts_lf = get_percent_share_ratios(
        estimated_job_role_posts_lf,
        input_col=IndCQC.ascwds_job_role_counts,
        output_col=IndCQC.ascwds_job_role_ratios,
    )
    estimated_job_role_posts_lf.sort(EXPANDED_ID).show(9)

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
            pl.col(EXPANDED_ID).sort_by(order_key),
            imputed_ratios,
        )
        .explode(EXPANDED_ID, IndCQC.imputed_ascwds_job_role_ratios)
        .drop(impute_groups)
    )
    impute_agg_lf.sort(EXPANDED_ID).show(10)

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
        impute_agg_lf, on=EXPANDED_ID, how="left"
    )  # this join is not adding the extra column - not clear why
    estimated_job_role_posts_lf.sort(EXPANDED_ID).show(9)

    # Multiply imputed ratios by estimate filled posts
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        pl.col(IndCQC.estimate_filled_posts)
        .mul(pl.col(IndCQC.imputed_ascwds_job_role_ratios))
        .alias(IndCQC.imputed_ascwds_job_role_counts)
    )
    estimated_job_role_posts_lf.sort(EXPANDED_ID).show(9)
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

    # Groupby-agg-explode on only necessary subset, before joining back on EXPANDED_ID.
    ratios_agg_lf = (
        estimated_job_role_posts_lf.group_by(groups)
        .agg(
            pl.col(EXPANDED_ID),  # Keep to align during explode
            percentage_share(input_col).cast(pl.Float32).alias(output_col),
        )
        .explode(EXPANDED_ID, output_col)
        # Drop groups to prevent duplicate columns after join.
        .drop(groups)
    )

    return estimated_job_role_posts_lf.join(ratios_agg_lf, on=EXPANDED_ID, how="left")


def create_ascwds_job_role_rolling_ratio(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
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
        .agg(pl.col(IndCQC.imputed_ascwds_job_role_counts).sum().alias("rolling_sum"))
    )

    # STEP C: Join the rolling sum back to the main 152M row table
    estimated_job_role_posts_lf.join(
        rolling_agg_lf,
        on=monthly_groups,
        how="left",
    )
    estimated_job_role_posts_lf = get_percent_share_ratios(
        estimated_job_role_posts_lf,
        input_col="rolling_sum",
        output_col=IndCQC.ascwds_job_role_rolling_ratio,
    )
    return estimated_job_role_posts_lf


def percentage_share_handling_zero_sum(column: str | pl.Expr) -> pl.Expr:
    """Calculate the percentage share of a column handling zero sum case.

    If the sum of all non-null values is zero, dividing by zero leads to a NaN.
    In this case we want to assume an even distribution across all non-null
    rows.

    Can be used in conjunction with `.group_by` and `.over` methods to get
    proportions within groups.
    """
    col = pl.col(column) if isinstance(column, str) else column
    total = col.sum()
    return (
        pl.when((total == 0) & (col == 0))
        .then(1 / col.is_not_null().sum())
        .otherwise(col / total)
    )


# # Unused - remove?
# def impute_full_time_series(column: str) -> pl.Expr:
#     """Impute nulls using linear interpolation, followed by back and forward fill."""
#     return pl.col(column).interpolate().forward_fill().backward_fill()


# # unused - remove?
# def rolling_sum_of_job_role_counts(
#     period: str = "6mo",
# ) -> pl.Expr:
#     """Compute rolling sum of job role counts within each primary service.

#     Args:
#         period (str): String language timedelta. Default "6mo". See:
#           https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.rolling.html

#     Returns:
#         pl.Expr: Expression for rolling sum of job role counts.
#     """
#     return (
#         pl.sum(IndCQC.imputed_ascwds_job_role_counts)
#         .rolling(index_column=IndCQC.cqc_location_import_date, period=period)
#         .over(
#             [IndCQC.primary_service_type, IndCQC.main_job_role_clean_labelled],
#             order_by=IndCQC.cqc_location_import_date,
#         )
#     )


class ManagerialFilledPostAdjustmentExpr:
    """Polars expression factory for redistributing managerial filled posts.

    This class provides a method to adjust "Registered Manager" (RM) estimates based
    on the counts of names given in the CQC data, and proportionally redistribute the
    difference across other managerial roles.

    The expression is returned via the class method `.build()`.

    Expects DataFrame to contain the following columns:
        - IndCQC.main_job_role_clean_labelled
        - IndCQC.estimate_filled_posts_by_job_role
        - IndCQC.registered_manager_names

    It's expected that we'd execute this expression over each location and import date,
    as shown in the example usage.

    Example Usage:
        >>> adjustment_expr = ManagerialFilledPostAdjustmentExpression.build()
        >>> groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
        >>> lf.with_columns(adjustment_expr.over(groups).alias("new_col"))
    """

    job_roles: pl.Expr = pl.col(IndCQC.main_job_role_clean_labelled)
    filled_post_estimates: pl.Expr = pl.col(IndCQC.estimate_filled_posts_by_job_role)
    _is_registered_manager: pl.Expr = job_roles == MainJobRoleLabels.registered_manager

    @classmethod
    def build(cls) -> pl.Expr:
        """Build adjust managerial filled post estimates expression."""
        return (
            pl.when(cls._is_non_rm_manager())
            .then(cls._adjusted_non_rm_manager_estimates())
            .when(cls._is_registered_manager)
            .then(cls._clip_rm_count())
            .otherwise(cls.filled_post_estimates)
        )

    @classmethod
    def _is_non_rm_manager(cls) -> pl.Expr:
        """Return DataFrame mask for non-RM manager roles."""
        manager_roles = AscwdsWorkerValueLabelsJobGroup.manager_roles()
        non_rm_manager_roles = [
            r for r in manager_roles if r != MainJobRoleLabels.registered_manager
        ]
        return cls.job_roles.is_in(non_rm_manager_roles)

    @classmethod
    def _clip_rm_count(cls) -> pl.Expr:
        """Return 1 if there is one or more registered manager names listed, 0 if not."""
        return (
            pl.col(IndCQC.registered_manager_names)
            .list.len()
            .clip(upper_bound=1)
            .fill_null(0)
        )

    @classmethod
    def _rm_manager_diff(cls) -> pl.Expr:
        """Subtract capped estimate of registered managers from CQC count to get diff.

        The "_is_registered_manager" mask should only equate to a single row
        (for each location and import date), and so summing with all other
        values as 0 results in the value at that "registered_manager" row
        broadcast to all other rows within the group.
        """
        diff = cls.filled_post_estimates.sub(cls._clip_rm_count())
        return pl.when(cls._is_registered_manager).then(diff).otherwise(0).sum()

    @classmethod
    def _non_rm_manager_proportions(cls) -> pl.Expr:
        """Get proportion of non-RM managerial role estimates as a percentage of total.

        The total in this case is the sum of all non-RM managerial roles. If this
        totals to 0, then an even distribution is assumed.
        """
        return percentage_share_handling_zero_sum(
            pl.when(cls._is_non_rm_manager()).then(cls.filled_post_estimates)
        )

    @classmethod
    def _adjusted_non_rm_manager_estimates(cls) -> pl.Expr:
        """Proportionally redistribute difference across remaining managerial roles.

        Ensure non-negative values following redistribution of RMs.
        """
        return cls.filled_post_estimates.add(
            cls._rm_manager_diff().mul(cls._non_rm_manager_proportions())
        ).clip(lower_bound=0)
