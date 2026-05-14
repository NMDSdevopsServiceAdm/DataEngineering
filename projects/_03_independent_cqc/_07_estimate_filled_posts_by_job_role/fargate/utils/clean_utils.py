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
    Filter out top and bottom percentiles of job role counts per job role group.

    This is to remove outliers from the distribution of filled posts within each job group, which
    may be caused by data quality issues in ASCWDS. If a job group's percentage of total ASCWDS counts
    for a particular location, service type and date is above the upper percentile bound or below the
    lower percentile bound (as passed to the function), then we set the ASCWDS job role count cleaned
    to NULL.

    The steps are as follows:
    1. Map job roles to job groups using the provided dictionary.
    2. Calculate job group ASCWDS count for location, service type and date.
    3. Calculate job group percentage of total ASCWDS count for location, service type and date.
    4. Calculate upper and lower percentile bounds of job group percentages for each job group, date and primary service type.
    5. Nullify ASCWDS job role counts where job role percentage is above upper bound or below lower bound.

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.
        upper_percentile_bound (float): Upper bound for percentile filtering. Defaults to 0.999.
        lower_percentile_bound (float): Lower bound for percentile filtering. Defaults to 0.001.

    Returns:
        pl.LazyFrame: LazyFrame with outliers in job role groups filtered.
    """
    # Define temporary column names
    temp_job_group_column = "job_group"
    temp_ascwds_job_group_count_column = "ascwds_job_group_count"
    temp_job_group_percentage_column = "job_group_percentage"
    temp_upper_bound_column = "upper_bound"
    temp_lower_bound_column = "lower_bound"
    temp_cols_to_drop = [
        temp_job_group_column,
        temp_ascwds_job_group_count_column,
        temp_job_group_percentage_column,
        temp_upper_bound_column,
        temp_lower_bound_column,
    ]
    # Define splits for groupby operations
    splits_for_location_sum = [
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        IndCQC.primary_service_type,
        temp_job_group_column,
    ]
    splits_for_job_group_percentage = [
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        IndCQC.primary_service_type,
    ]
    splits_for_bounds = [
        IndCQC.cqc_location_import_date,
        IndCQC.primary_service_type,
        temp_job_group_column,
    ]

    # Reduce dataset for processing
    # filter_lf = lf.select(
    #     IndCQC.id_per_locationid_import_date_job_role,
    #     IndCQC.location_id,
    #     IndCQC.cqc_location_import_date,
    #     IndCQC.primary_service_type,
    #     IndCQC.main_job_role_clean_labelled,
    #     IndCQC.ascwds_job_role_counts,
    # )

    # 1. Map job roles to job groups
    job_role_group_data = {
        IndCQC.main_job_role_clean_labelled: list(job_group_dict.keys()),
        temp_job_group_column: list(job_group_dict.values()),
    }
    job_role_group_schema = {
        IndCQC.main_job_role_clean_labelled: pl.Enum(
            AscwdsWorkerValueLabelsJobGroup.all_roles()
        ),
        temp_job_group_column: pl.Enum(list(set(job_group_dict.values()))),
    }
    job_role_group_lf = pl.LazyFrame(job_role_group_data, schema=job_role_group_schema)

    lf = lf.join(job_role_group_lf, on=IndCQC.main_job_role_clean_labelled, how="left")

    # 2. Calculate job group ASCWDS count for location, service type and date.
    agg_lf = (
        lf.group_by(splits_for_location_sum)
        .agg(
            pl.col(IndCQC.id_per_locationid_import_date_job_role),
            pl.col(IndCQC.ascwds_job_role_counts)
            .sum()
            .alias(temp_ascwds_job_group_count_column),
        )
        .explode(
            IndCQC.id_per_locationid_import_date_job_role,
        )
        .drop(splits_for_location_sum)
    )  # Drop groups to prevent duplicate columns after join.
    lf = lf.join(agg_lf, on=IndCQC.id_per_locationid_import_date_job_role, how="left")

    # 3. Calculate job group percentage of total ASCWDS count for location, service type and date.
    job_group_percentage_expr = (pl.col(temp_ascwds_job_group_count_column)) / (
        pl.col(IndCQC.ascwds_job_role_counts).sum()
    )

    agg_lf = (
        lf.group_by(splits_for_job_group_percentage)
        .agg(
            pl.col(IndCQC.id_per_locationid_import_date_job_role),
            job_group_percentage_expr.alias(temp_job_group_percentage_column),
        )
        .explode(
            IndCQC.id_per_locationid_import_date_job_role,
            temp_job_group_percentage_column,
        )
        .drop(splits_for_job_group_percentage)
    )  # Drop groups to prevent duplicate columns after join.

    lf = lf.join(agg_lf, on=IndCQC.id_per_locationid_import_date_job_role, how="left")

    # 4. Calculate upper and lower percentile bounds of job group percentages for each job group, date and primary service type.

    job_group_percentage_for_upper_bound_expr = pl.col(
        temp_job_group_percentage_column
    ).quantile(
        upper_percentile_bound, interpolation="linear"
    )  # Not in streaming engine
    job_group_percentage_for_lower_bound_expr = pl.col(
        temp_job_group_percentage_column
    ).quantile(lower_percentile_bound, interpolation="linear")
    agg_lf = (
        lf.group_by(splits_for_bounds)
        .agg(
            pl.col(IndCQC.id_per_locationid_import_date_job_role),
            job_group_percentage_for_upper_bound_expr.alias(temp_upper_bound_column),
            job_group_percentage_for_lower_bound_expr.alias(temp_lower_bound_column),
        )
        .explode(
            IndCQC.id_per_locationid_import_date_job_role,
        )
        .drop(splits_for_bounds)
    )  # Drop groups to prevent duplicate columns after join.

    lf = lf.join(agg_lf, on=IndCQC.id_per_locationid_import_date_job_role, how="left")

    # 5. Nullify ASCWDS job role counts where job role percentage is above upper bound or below lower bound.
    lf = lf.with_columns(
        pl.when(
            (pl.col(temp_job_group_percentage_column) > pl.col(temp_upper_bound_column))
            | (
                pl.col(temp_job_group_percentage_column)
                < pl.col(temp_lower_bound_column)
            )
        )
        .then(pl.lit(None))
        .otherwise(pl.col(IndCQC.ascwds_job_role_counts))
        .alias(IndCQC.ascwds_job_role_counts),
    )  # .drop(temp_cols_to_drop)

    return lf
