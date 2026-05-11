import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EstimateFilledPostsSource
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

job_group_dict: dict[str, str] = (
    AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict
)


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


def filter_job_role_group_outliers(
    lf: pl.LazyFrame,
    upper_percentile_bound: float = 0.999,
    lower_percentile_bound: float = 0.001,
) -> pl.LazyFrame:
    """
    Placeholder function for filtering ASC-WDS worker data.

    Steps to be implemented:
     1. Assign job role group per row
     2. Aggregate ascwds count by job role, location and date
     3. Calculate job group ratio on aggregate data
     4. Calculate percentile bounds per job group and primary service type on the aggregate data
     5. Filter out rows outside of bounds in _cleaned column

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.
        upper_percentile_bound (float): Upper bound for percentile filtering. Defaults to 0.999.
        lower_percentile_bound (float): Lower bound for percentile filtering. Defaults to 0.001.

    Returns:
        pl.LazyFrame: Transformed LazyFrame.
    """
    lf = lf.with_columns(
        pl.col(IndCQC.main_job_role_clean_labelled)
        .replace_strict(job_group_dict)
        .alias(IndCQC.main_job_group_labelled)
    )

    location_sum_expr = (
        pl.col(IndCQC.ascwds_job_role_counts)
        .sum()
        .over(
            [
                IndCQC.location_id,
                IndCQC.cqc_location_import_date,
                IndCQC.primary_service_type,
            ]
        )
    )

    job_role_percentage_expr = pl.col(IndCQC.ascwds_job_role_counts) / location_sum_expr

    lf = lf.with_columns(job_role_percentage_expr.alias("job_role_percentage"))
    splits_for_bounds = [
        IndCQC.main_job_group_labelled,
        IndCQC.cqc_location_import_date,
        IndCQC.primary_service_type,
    ]

    job_role_percentage_for_upper_bound_expr = (
        pl.col("job_role_percentage")
        .quantile(upper_percentile_bound, interpolation="linear")
        .over(splits_for_bounds)
    )
    job_role_percentage_for_lower_bound_expr = (
        pl.col("job_role_percentage")
        .quantile(lower_percentile_bound, interpolation="linear")
        .over(splits_for_bounds)
    )

    lf = lf.with_columns(
        pl.when(
            (job_role_percentage_expr > job_role_percentage_for_upper_bound_expr)
            | (job_role_percentage_expr < job_role_percentage_for_lower_bound_expr)
        )
        .then(pl.lit(None))
        .otherwise(pl.col(IndCQC.ascwds_job_role_counts_cleaned))
        .alias(IndCQC.ascwds_job_role_counts_cleaned),
    )

    lf = lf.drop(
        IndCQC.main_job_group_labelled,
        # "job_role_percentage_for_upper_bound",
        # "job_role_percentage_for_lower_bound",
        "job_role_percentage",
    )  # Drop job group column as it's no longer needed after filtering
    return lf
