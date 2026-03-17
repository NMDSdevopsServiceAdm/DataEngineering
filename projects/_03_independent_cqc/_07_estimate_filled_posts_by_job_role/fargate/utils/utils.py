import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    MainJobRoleLabels,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)


def join_worker_to_estimates_dataframe(
    estimated_filled_posts_lf: pl.LazyFrame,
    aggregated_job_roles_per_establishment_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Join the mainjrid_clean_labels and ascwds_job_role_counts columns from the aggregated worker LazyFrame into the estimated filled post LazyFrame.

    Join as left join where:
      left = estimated filled post LazyFrame
      right = aggregated worker LazyFrame
      where establishment_id matches and ascwds_workplace_import_date == ascwds_worker_import_date.

    Args:
        estimated_filled_posts_lf (pl.LazyFrame): A LazyFrame containing estimated filled posts per workplace.
        aggregated_job_roles_per_establishment_lf (pl.LazyFrame): A LazyFrame with job role counts per workplace.

    Returns:
        pl.LazyFrame: The estimated filled post LazyFrame with columns main_job_role_clean_labelled and ascwds_job_role_counts added.
    """

    merged_lf = estimated_filled_posts_lf.join(
        other=aggregated_job_roles_per_establishment_lf,
        left_on=[IndCQC.establishment_id, IndCQC.ascwds_workplace_import_date],
        right_on=[IndCQC.establishment_id, IndCQC.ascwds_worker_import_date],
        how="left",
    )

    return merged_lf


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


# TODO: Move this into a more centralised module of generic polars expression functions.
def percentage_share(column: str | pl.Expr) -> pl.Expr:
    """Calculate the percentage share of a column across all values.

    Can be used in conjunction with `.group_by` and `.over` methods to get
    proportions within groups.
    """
    # If it's a string, turn it into a column expression; otherwise, use as-is.
    col = pl.col(column) if isinstance(column, str) else column
    return col / col.sum()


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


def impute_full_time_series(column: str) -> pl.Expr:
    """Impute nulls using linear interpolation, followed by back and forward fill."""
    return pl.col(column).interpolate().forward_fill().backward_fill()


def rolling_sum_of_job_role_counts(
    period: str = "6mo",
) -> pl.Expr:
    """Compute rolling sum of job role counts within each primary service.

    Args:
        period (str): String language timedelta. Default "6mo". See:
          https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.rolling.html

    Returns:
        pl.Expr: Expression for rolling sum of job role counts.
    """
    return (
        pl.sum(IndCQC.imputed_ascwds_job_role_counts)
        .rolling(index_column=IndCQC.cqc_location_import_date, period=period)
        .over(
            [IndCQC.primary_service_type, IndCQC.main_job_role_clean_labelled],
            order_by=IndCQC.cqc_location_import_date,
        )
    )


class ManagerialFilledPostAdjustmentExpression:
    """Polars expression factory for redistributing managerial filled posts.

    This class provides a method to adjust "Registered Manager" (RM) estimates based
    on the counts of names given in the CQC data, and proportionally redistributes the
    difference across other managerial roles within each location.

    The expression is returned via the class method `.build()`.

    Expects DataFrame to contain the following columns:
        - IndCQC.main_job_role_clean_labelled
        - IndCQC.estimate_filled_posts_by_job_role
        - IndCQC.location_id
        - IndCQC.registered_manager_names

    Example Usage:
        >>> lf.with_columns(ManagerialFilledPostAdjustmentExpression.build().alias("new_col"))
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
        values as 0 results in the value at that "rsegistered_manager" row
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

        Ensure non-negative values to ensure that the total number of estimated
        managerial filled posts remains consistent after correcting for RM
        discrepancies.
        """
        return cls.filled_post_estimates.add(
            cls._rm_manager_diff().mul(cls._non_rm_manager_proportions())
        ).clip(lower_bound=0)
