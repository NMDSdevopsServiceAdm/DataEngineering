import polars as pl

from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    add_job_role_groups_column,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    JobGroupLabels,
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
    1. Map job roles to job groups
    2. Pivot table and aggregate to job group
    3. Calculate total ASCWDS count for location, service type and date
    4. Calculate job group percentage of total ASCWDS count for location, service type and date
    5. Calculate upper and lower percentile bounds of job group percentages for each job group, date and primary service type
    6. Flag where job role percentage is outside bounds
    7. Null ascwds_job_role_counts_column

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.
        upper_percentile_bound (float): Upper bound for percentile filtering. Defaults to 0.999.
        lower_percentile_bound (float): Lower bound for percentile filtering. Defaults to 0.001.

    Returns:
        pl.LazyFrame: LazyFrame with outliers in job role groups filtered.
    """
    # Define temporary column names
    temp_job_group_column = "job_group"
    temp_location_out_of_bounds = "location_out_of_bounds"
    temp_cols_to_drop = [temp_job_group_column, temp_location_out_of_bounds]
    Exprs = FilterJobRoleGroupExpressions()

    # Define splits for groupby operations
    splits_for_pivot = [
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        IndCQC.primary_service_type,
        IndCQC.id_per_locationid_import_date,
    ]

    # 1. Map job roles to job groups
    lf = add_job_role_groups_column(lf, temp_job_group_column)

    # 2. Pivot table and aggregate to job group
    piv_lf = (
        lf.pivot(
            on=temp_job_group_column,
            on_columns=list(set(Exprs.job_group_cols)),
            index=splits_for_pivot,
            values=IndCQC.ascwds_job_role_counts,
            aggregate_function="sum",
        )
        .with_columns(  # 3. Calculate total ASCWDS count for location, service type and date.
            Exprs.location_sum_expr
        )
        .with_columns(  # 4. Calculate job group percentage of total ASCWDS count for location, service type and date.
            Exprs.job_group_percentage_expr
        )
    )
    piv_lf = (
        piv_lf.join(  # 5. Calculate upper and lower percentile bounds of job group percentages for each job group and primary service type.
            Exprs.create_bounds(
                piv_lf, upper_percentile_bound, Exprs.upper_bound_suffix
            ),
            on=IndCQC.primary_service_type,
            how="left",
        )
        .join(
            Exprs.create_bounds(
                piv_lf, lower_percentile_bound, Exprs.lower_bound_suffix
            ),
            on=IndCQC.primary_service_type,
            how="left",
        )
        .with_columns(  # 6. Flag where job role percentage is outside bounds.
            pl.when(Exprs.evaluation_expr)
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .cast(pl.Boolean)
            .alias(temp_location_out_of_bounds)
        )
        .select(IndCQC.id_per_locationid_import_date, temp_location_out_of_bounds)
    )

    lf = (
        lf.join(piv_lf, on=IndCQC.id_per_locationid_import_date, how="left")
        .with_columns(  # 7. Null ascwds_job_role_counts_column
            pl.when(pl.col(temp_location_out_of_bounds) == True)
            .then(None)
            .otherwise(pl.col(IndCQC.ascwds_job_role_counts))
            .alias(IndCQC.ascwds_job_role_counts)
        )
        .drop(temp_cols_to_drop)
    )
    return lf


class FilterJobRoleGroupExpressions:
    """
    Collection of polars expressions for filtering job group outliers.

    This class defines reusable expressions for filtering locations
    with outlying job role group distributions. It also defines column names
    used by these expressions.

    Attributes:
        temp_location_sum (str): Temporary column name for the total number of job roles at a location.
        job_group_cols (list[str]): List of job group column names.
        upper_bound_suffix (str): A column suffix for denoting upper bounds.
        lower_bound_suffix (str): A column suffix for denoting lower bounds.
        location_sum_expr (pl.Expr): Expression to calculate the total job roles at a location.
        job_group_percentage_expr (pl.Expr): Expression to calculate the percentage of job roles.
        evaluation_expr (pl.Expr): Expression to evaluate whether a value is out of bounds.
    """

    temp_location_sum: str
    job_group_cols: list[str]
    upper_bound_suffix: str
    lower_bound_suffix: str
    location_sum_expr: pl.Expr
    job_group_percentage_expr: pl.Expr
    evaluation_expr: pl.Expr

    def __init__(self):
        self.temp_location_sum = "location_sum"
        self.job_group_cols = [
            JobGroupLabels.direct_care,
            JobGroupLabels.managers,
            JobGroupLabels.regulated_professions,
            JobGroupLabels.other,
        ]
        self.upper_bound_suffix = "_upper"
        self.lower_bound_suffix = "_lower"
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

    def create_bounds(
        self, lf: pl.LazyFrame, bound: float, suffix: str
    ) -> pl.LazyFrame:
        """
        Creates table of upper or lower percentages that match the percentile bounds given.

        Yeilds:
            pl.LazyFrame: A lazyframe with the requested percentages.
        """
        return (
            lf.group_by(IndCQC.primary_service_type)
            .quantile(bound, interpolation="linear")
            .select(
                pl.col(IndCQC.primary_service_type),
                pl.col(JobGroupLabels.direct_care).alias(
                    JobGroupLabels.direct_care + suffix
                ),
                pl.col(JobGroupLabels.managers).alias(JobGroupLabels.managers + suffix),
                pl.col(JobGroupLabels.regulated_professions).alias(
                    JobGroupLabels.regulated_professions + suffix
                ),
                pl.col(JobGroupLabels.other).alias(JobGroupLabels.other + suffix),
            )
        )
