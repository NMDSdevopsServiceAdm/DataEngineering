import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    MainJobRoleLabels,
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
    return pl.col(column).list.len().ge(1).fill_null(False)


def cap_registered_managers_to_1(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Return 1 if there is one or more registered managers, 0 if not.

    This approach aligns with historical Excel structures where each location
    was effectively recorded with at most one registered manager.

    Fills Nulls to 0 also.
    """
    return lf.with_columns(
        has_elements(IndCQC.registered_manager_names)
        .cast(pl.Int8)  # Cast bool to 1/0.
        .fill_null(0)
        .alias(IndCQC.registered_manager_count)
    )


def get_estimated_managers_diff_from_cqc_registered_managers(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Subtract capped estimate of registered managers from CQC count to get diff.

    A positive value is when CQC have recorded more registered managers than we
    have estimated. A negative value is when CQC have recorded fewer.

    CQC have the official count of registered managers. Our estimate is based on
    records in ASC-WDS.
    """
    return lf.with_columns(
        pl.col(MainJobRoleLabels.registered_manager)
        .sub(IndCQC.registered_manager_count)
        .alias(IndCQC.difference_between_estimate_and_cqc_registered_managers),
    )
