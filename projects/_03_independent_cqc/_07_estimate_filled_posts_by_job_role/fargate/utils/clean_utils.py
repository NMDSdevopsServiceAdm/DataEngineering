import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    JobGroupLabels,
)
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
    upper_percentile_bound: float = 0.99,
    lower_percentile_bound: float = 0.01,
) -> pl.LazyFrame:
    """
    Filter out top and bottom percentiles of job role counts per job role group.

    This is to remove outliers from the distribution of filled posts within each job group, which
    may be caused by data quality issues in ASCWDS. If a job group's percentage of total ASCWDS counts
    for a particular location, service type and date is above the upper percentile bound or below the
    lower percentile bound (as passed to the function), then we set the ASCWDS job role count cleaned
    to NULL.

    The steps are as follows:
    1. Map job roles to job groups
    2. Pivot table and aggregate to job group
    3. Calculate total ASCWDS count for location, service type and date
    4. Calculate job group percentage of total ASCWDS count for location, service type and date
    5. Calculate upper and lower percentile bounds of job group percentages for each job group, date and primary service type
    6. Flag where job role percentage is outside bounds
    7. Null ascwds_job_role_counts_column

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.
        upper_percentile_bound (float): Upper bound for percentile filtering. Defaults to 0.995.
        lower_percentile_bound (float): Lower bound for percentile filtering. Defaults to 0.005.

    Returns:
        pl.LazyFrame: LazyFrame with outliers in job role groups filtered.
    """
    # Define temporary column names
    temp_job_group_column = "job_group"
    temp_location_out_of_bounds = "location_out_of_bounds"
    temp_cols_to_drop = [temp_job_group_column, temp_location_out_of_bounds]

    Exprs = FilterJobRoleGroupExpressions(
        upper_percentile_bound, lower_percentile_bound
    )

    # Define splits for groupby operations
    splits_for_pivot = [
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        IndCQC.primary_service_type,
        IndCQC.id_per_locationid_import_date,
    ]

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

    # 2. Pivot table and aggregate to job group
    piv_lf = lf.pivot(
        on=temp_job_group_column,
        on_columns=list(set(job_group_dict.values())),
        index=splits_for_pivot,
        values=IndCQC.ascwds_job_role_counts,
        aggregate_function="sum",
    )

    # 3. Calculate total ASCWDS count for location, service type and date.
    piv_lf = piv_lf.with_columns(Exprs.location_sum_expr)

    # 4. Calculate job group percentage of total ASCWDS count for location, service type and date.
    piv_lf = piv_lf.with_columns(Exprs.job_group_percentage_expr)

    # 5. Calculate upper and lower percentile bounds of job group percentages for each job group, date and primary service type.
    piv_lf = piv_lf.with_columns(
        Exprs.bounds_expressions(Exprs.bounds, Exprs.job_group_cols, Exprs.suffixes),
    )

    # 6. Flag where job role percentage is outside bounds.
    piv_lf = piv_lf.with_columns(
        pl.when(Exprs.evaluation_expr)
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .cast(pl.Boolean)
        .alias(temp_location_out_of_bounds)
    )

    piv_lf = piv_lf.select(
        IndCQC.id_per_locationid_import_date, temp_location_out_of_bounds
    )

    lf = lf.join(piv_lf, on=IndCQC.id_per_locationid_import_date, how="left")

    # 7. Null ascwds_job_role_counts_column
    lf = lf.with_columns(
        pl.when(pl.col(temp_location_out_of_bounds) == True)
        .then(None)
        .otherwise(pl.col(IndCQC.ascwds_job_role_counts))
        .alias(IndCQC.ascwds_job_role_counts)
    ).drop(temp_cols_to_drop)
    return lf


class FilterJobRoleGroupExpressions:

    def __init__(self, upper, lower):
        self.temp_location_sum = "location_sum"
        self.job_group_cols = [
            JobGroupLabels.direct_care,
            JobGroupLabels.managers,
            JobGroupLabels.regulated_professions,
            JobGroupLabels.other,
        ]
        self.upper_bound_suffix = "_upper_bound"
        self.lower_bound_suffix = "_lower_bound"
        self.bounds = [upper, lower]
        self.suffixes = [self.upper_bound_suffix, self.lower_bound_suffix]

        self.location_sum_expr = pl.sum_horizontal(self.job_group_cols).alias(
            self.temp_location_sum
        )
        self.job_group_percentage_expr = pl.col(self.job_group_cols) / pl.col(
            self.temp_location_sum
        )
        self.evaluation_expr = (
            (
                pl.col(JobGroupLabels.direct_care)
                > pl.col(JobGroupLabels.direct_care + self.upper_bound_suffix)
            )
            | (
                pl.col(JobGroupLabels.direct_care)
                < pl.col(JobGroupLabels.direct_care + self.lower_bound_suffix)
            )
            | (
                pl.col(JobGroupLabels.managers)
                > pl.col(JobGroupLabels.managers + self.upper_bound_suffix)
            )
            | (
                pl.col(JobGroupLabels.regulated_professions)
                > pl.col(JobGroupLabels.regulated_professions + self.upper_bound_suffix)
            )
            | (
                pl.col(JobGroupLabels.other)
                > pl.col(JobGroupLabels.other + self.upper_bound_suffix)
            )
        )

    def bounds_expressions(self, bounds, job_group_cols, suffixes):
        for b, s in bounds, suffixes:
            yield (
                pl.col(job_group_cols)
                .quantile(b, interpolation="linear")  # Not in streaming engine
                .cast(pl.Float32)
                .name.suffix(s)
            )
