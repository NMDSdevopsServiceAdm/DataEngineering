import polars as pl

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
    Null job role counts at locations where job group distribution is out of bounds.

    This function nulls outliers based on a locations job group distribution. Their
    distribution is calculated per location and import date, then upper/lower percentile
    bounds are calculated per service type for each job group across all periods. Values
    are nulled where:

    - direct care outside upper or lower percentile
    - managers/regulated professions/other outside upper percentile

    The steps are as follows:
    1. Pivot table and aggregate to job group
    2. Calculate total ASCWDS count for location, service type and date
    3. Calculate job group percentage of total ASCWDS count for location, service type and date
    4. Calculate upper and lower percentile bounds of job group percentages for each job group and primary service type
    5. Flag where job role percentage is outside bounds
    6. Null ascwds_job_role_counts_column

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.
        upper_percentile_bound (float): Upper bound for percentile filtering. Defaults to 0.999.
        lower_percentile_bound (float): Lower bound for percentile filtering. Defaults to 0.001.

    Returns:
        pl.LazyFrame: LazyFrame with outliers in job role groups filtered.
    """

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

    # 1. Pivot table and aggregate to job group
    piv_lf = (
        lf.pivot(
            on=IndCQC.main_job_group_labelled,
            on_columns=Exprs.job_group_cols,
            index=splits_for_pivot,
            values=IndCQC.ascwds_job_role_counts,
            aggregate_function="sum",
        )
        .with_columns(  # 2. Calculate total ASCWDS count for location, service type and date.
            Exprs.location_sum_expr
        )
        .with_columns(  # 3. Calculate job group percentage of total ASCWDS count for location, service type and date.
            Exprs.job_group_percentage_expr
        )
    )

    # 4. Calculate upper and lower percentile bounds of job group percentages for each job group and primary service type.
    bounds_lf = piv_lf.group_by(IndCQC.primary_service_type).agg(
        Exprs.upper_bounds_expr,
        Exprs.lower_bounds_expr,
    )

    piv_lf = (
        piv_lf.join(
            bounds_lf,
            on=IndCQC.primary_service_type,
            how="left",
        )
        .with_columns(  # 5. Flag where job role percentage is outside bounds.
            pl.when(Exprs.evaluation_expr)
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .cast(pl.Boolean)
            .alias(IndCQC.job_role_filtering_rule)
        )
        .select(IndCQC.id_per_locationid_import_date, IndCQC.job_role_filtering_rule)
    )

    lf = lf.join(
        piv_lf, on=IndCQC.id_per_locationid_import_date, how="left"
    ).with_columns(  # 6. Null ascwds_job_role_counts_column
        pl.when(pl.col(IndCQC.job_role_filtering_rule))
        .then(None)
        .otherwise(pl.col(IndCQC.ascwds_job_role_counts))
        .alias(IndCQC.ascwds_job_role_counts)
    )
    return lf


class FilterJobRoleGroupExpressions:
    """
    Collection of polars expressions for filtering job group outliers.

    This class defines reusable expressions for filtering locations
    with outlying job role group distributions. It also defines column names
    used by these expressions.

    Attributes:
        temp_location_sum (str): Temporary column name for the total number of workers at a location.
        job_group_cols (list[str]): List of job group column names.
        upper_bound_suffix (str): A column suffix for denoting upper bounds.
        lower_bound_suffix (str): A column suffix for denoting lower bounds.
        location_sum_expr (pl.Expr): Expression to calculate the total workers at a location.
        job_group_percentage_expr (pl.Expr): Expression to calculate the percentage of job roles.
        evaluation_expr (pl.Expr): Expression to evaluate whether a value is out of bounds.
        upper_bounds_expr (pl.Expr): Expression to calulate upper percentage bounds.
        lower_bounds_expr (pl.Expr): Expression to calulate lower percentage bounds.

    Args:
        upper_percentile_bound (float): The upper percentile bound for job group ratios.
        lower_percentile_bound (float): The lower percentile bound for job group ratios.

    """

    temp_location_sum: str
    job_group_cols: list[str]
    upper_bound_suffix: str
    lower_bound_suffix: str
    location_sum_expr: pl.Expr
    job_group_percentage_expr: pl.Expr
    evaluation_expr: pl.Expr
    upper_bounds_expr: pl.Expr
    lower_bounds_expr: pl.Expr

    def __init__(self, upper_percentile_bound: float, lower_percentile_bound: float):
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
        ).cast(pl.Float32)
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
        self.upper_bounds_expr = (
            pl.col(self.job_group_cols)
            .quantile(upper_percentile_bound, interpolation="linear")
            .name.suffix(self.upper_bound_suffix)
        )
        self.lower_bounds_expr = (
            pl.col(self.job_group_cols)
            .quantile(lower_percentile_bound, interpolation="linear")
            .name.suffix(self.lower_bound_suffix)
        )


def filter_job_role_group_equal_zero(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Null job role counts at locations with zero direct care or managers +
    regulated professions is zero.

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.

    Returns:
        pl.LazyFrame: LazyFrame with 'ascwds_job_role_counts' conditionally nulled.
    """
    # TODO Implement function
    return lf
