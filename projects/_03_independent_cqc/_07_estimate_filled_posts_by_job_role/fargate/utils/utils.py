import polars as pl
from polars import selectors as cs

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    JobGroupLabels,
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


def percentage_share_horizontal(*columns: str) -> pl.Expr:
    """Calculate the percentage share horizontally across given columns."""
    return cs.by_name(*columns) / pl.sum_horizontal(*columns)


def percentage_share_handling_zero_sum(column: str | pl.Expr) -> pl.Expr:
    """Calculate the percentage share of a column handling zero sum case.

    If all values are zero, dividing by zero leads to a NaN. In this case we
    want to assume an even distribution across all rows.

    Can be used in conjunction with `.group_by` and `.over` methods to get
    proportions within groups.
    """
    col = pl.col(column) if isinstance(column, str) else column
    return pl.when(col.sum() == 0).then(1 / pl.len()).otherwise(percentage_share(col))


def percentage_share_horizontal_handling_zero_sum(*columns: str) -> list[pl.Expr]:
    """Calculate the percentage share horizontally handling zero sum case.

    If all values are zero, dividing by zero leads to a NaN. In this case we
    want to assume an even distribution.
    """
    n_cols = len(columns)
    total = pl.sum_horizontal(*columns)
    return [
        pl.when(total == 0)
        .then(pl.lit(1 / n_cols))
        .otherwise(pl.col(col) / total)
        .alias(col)
        for col in columns
    ]


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


def has_elements(column: str) -> pl.Expr:
    """Return True if List column has 1 or more elements.

    Args:
        column (str): Must be a column of type pl.List.

    Returns:
        pl.Expr: A boolean column, True if list has elements, False if not.

    Note:
        Calling .list on a non-list column will raise a `PolarsComputeError` at runtime.
    """
    return pl.col(column).list.len().ge(1)


def cap_registered_managers_to_1() -> pl.Expr:
    """Return 1 if there is one or more registered managers, 0 if not.

    This approach aligns with historical Excel structures where each location
    was effectively recorded with at most one registered manager.

    Fills Nulls to 0 also.
    """
    # Cast bool to 1/0.
    return has_elements(IndCQC.registered_manager_names).cast(pl.Int8).fill_null(0)


def get_estimated_managers_diff_from_cqc_registered_managers() -> pl.Expr:
    """Subtract capped estimate of registered managers from CQC count to get diff.

    A positive value is when CQC have recorded more registered managers than we
    have estimated. A negative value is when CQC have recorded fewer.

    CQC have the official count of registered managers. Our estimate is based on
    records in ASC-WDS.
    """
    is_registered_manager = (
        pl.col(IndCQC.main_job_role_clean_labelled)
        == MainJobRoleLabels.registered_manager
    )
    diff = pl.col(IndCQC.estimate_filled_posts_by_job_role).sub(
        cap_registered_managers_to_1()
    )
    return pl.when(is_registered_manager).then(diff).otherwise(0).sum()


def get_non_rm_manager_proportions() -> pl.Expr:
    filled_posts = IndCQC.estimate_filled_posts_by_job_role
    non_rm_manager_roles = get_non_registered_manager_roles()
    non_rm_manager_mask = pl.col(IndCQC.main_job_role_clean_labelled).is_in(
        non_rm_manager_roles
    )
    masked_estimates = pl.when(non_rm_manager_mask).then(filled_posts)
    return percentage_share_handling_zero_sum(masked_estimates)


def filter_job_roles(group_label: str) -> list[str]:
    """Filter for job roles that match the group label."""
    job_role_dict = AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict
    return [role for role, group in job_role_dict.items() if group == group_label]


def get_non_registered_manager_roles() -> list[str]:
    """Get list of manager roles except registered manager."""
    manager_roles = filter_job_roles(JobGroupLabels.managers)
    return [
        role for role in manager_roles if role != MainJobRoleLabels.registered_manager
    ]


def adjusted_non_rm_managerial_filled_posts_expr() -> pl.Expr:
    """
    Return an expression to calculate adjusted managerial filled posts estimates.

    Proportionally redistributes the difference between estimated and actual "registered
    managers" (RM) across all non-RM managerial roles, ensuring that the total number of
    estimated managerial filled posts remains consistent after correcting for RM
    discrepancies.
    """
    filled_posts = IndCQC.estimate_filled_posts_by_job_role
    proportions_expr = get_non_rm_manager_proportions()
    manager_diff_expr = get_estimated_managers_diff_from_cqc_registered_managers()
    return (
        pl.col(filled_posts)
        .add(manager_diff_expr.mul(proportions_expr).over(IndCQC.location_id))
        .clip(lower_bound=0)
    )
