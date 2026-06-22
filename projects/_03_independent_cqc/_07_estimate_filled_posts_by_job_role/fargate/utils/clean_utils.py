import polars as pl

from polars_utils.filtering_utils import update_filtering_rule
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    CategoricalColumnTypes as CatColType,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    JobGroupLabels,
    JobRoleFilteringRule,
    PrimaryServiceType,
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
    id_column: str = IndCQC.location_id,
    min_workers_threshold: int = 50,
    include_direct_care_lower_bound: bool = True,
) -> pl.LazyFrame:
    """
    Null job role counts at locations, providers or brands where job group distribution is
    out of bounds and they have the same or more workers than the min workers threshold.

    This function nulls outliers based on job group distribution at either brand, provider
    or location level, as specified. Their distribution is calculated per brand, provider,
    or location and their import date, then compared against fixed bounds per service type.
    Values are nulled where there are the same or more workers than the min workers threshold
    and either:

    - direct care is outside the upper bound, or outside the lower bound if
      include_direct_care_lower_bound is True
    - managers, regulated professions, or other are outside their upper bound

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.
        id_column (str): The id column on which the filter should be applied. Possible
            values: location id, provider id, brand id. Defaults to location id.
        min_workers_threshold (int): Minimum number of workers at a location/ provider/
            brand to be included in filtering. If below this threshold, location/ provider/brand
            bypasses the filter and is automatically included. Defaults to 50.
        include_direct_care_lower_bound (bool): Whether to include the lower bound for
            direct care in the filtering. Defaults to True.

    Returns:
        pl.LazyFrame: LazyFrame with outliers in job role groups filtered.

    Raises:
        ValueError: If the id_column argument is not one of: location id, provider id,
            or brand id.
    """
    if id_column not in [IndCQC.brand_id, IndCQC.provider_id, IndCQC.location_id]:
        raise ValueError(
            f"Value must be one of {IndCQC.brand_id}, {IndCQC.provider_id}, or {IndCQC.location_id}"
        )
    temp_out_of_bounds_col: str = f"{id_column}_out_of_bounds"

    Exprs = FilterJobRoleGroupExpressions(include_direct_care_lower_bound)

    if id_column == IndCQC.location_id:
        splits_for_pivot = [
            id_column,
            IndCQC.cqc_location_import_date,
            IndCQC.primary_service_type,
        ]
    else:
        splits_for_pivot = [
            id_column,
            IndCQC.cqc_location_import_date,
        ]
    piv_lf = (
        lf.pivot(
            on=IndCQC.main_job_group_labelled,
            on_columns=Exprs.job_group_cols,
            index=splits_for_pivot,
            values=IndCQC.ascwds_job_role_counts,
            aggregate_function="sum",
        )
        .with_columns(
            [
                Exprs.id_column_sum_expr,
                *[
                    (pl.col(c) / pl.col(Exprs.temp_id_column_sum)).alias(
                        f"{id_column}_{c}_pct"
                    )
                    for c in Exprs.job_group_cols
                ],
            ]
        )
        .filter(pl.col(Exprs.temp_id_column_sum) >= min_workers_threshold)
        .with_columns(Exprs.job_group_percentage_expr)
    )

    # Build a LazyFrame of the fixed bounds from the magic numbers dict
    if id_column == IndCQC.location_id:
        bounds_lf = pl.LazyFrame(
            [
                {IndCQC.primary_service_type: service_type, **bounds}
                for service_type, bounds in Exprs.job_role_group_bounds_locations.items()
            ]
        ).with_columns(
            pl.col(IndCQC.primary_service_type).cast(CatColType.PrimaryServiceEnumType)
        )
    else:
        bounds_lf = pl.LazyFrame(Exprs.job_role_group_bounds_brand_prov)

    if id_column == IndCQC.location_id:
        piv_lf = piv_lf.join(
            bounds_lf,
            on=IndCQC.primary_service_type,
            how="left",
        )
    else:
        piv_lf = piv_lf.join(bounds_lf, how="cross")

    piv_lf = piv_lf.with_columns(
        Exprs.evaluation_expr.alias(temp_out_of_bounds_col)
    ).select(
        *splits_for_pivot,
        temp_out_of_bounds_col,
        Exprs.temp_id_column_sum,
        *[f"{id_column}_{c}_pct" for c in Exprs.job_group_cols],
    )

    lf = lf.join(
        piv_lf,
        on=splits_for_pivot,
        how="left",
    )

    lf = lf.with_columns(
        pl.when(
            (pl.col(temp_out_of_bounds_col) == True)
            & (pl.col(id_column).is_not_null())  # brand_id can be null
        )
        .then(None)
        .otherwise(pl.col(IndCQC.ascwds_job_role_counts))
        .alias(IndCQC.ascwds_job_role_counts)
    )
    # .drop(temp_out_of_bounds_col)

    if id_column == IndCQC.brand_id:
        new_rule = JobRoleFilteringRule.job_role_group_is_outlier_at_brand_level
    elif id_column == IndCQC.provider_id:
        new_rule = JobRoleFilteringRule.job_role_group_is_outlier_at_provider_level
    elif id_column == IndCQC.location_id:
        new_rule = JobRoleFilteringRule.job_role_group_is_outlier_at_location_level

    lf = update_filtering_rule(
        lf,
        filter_rule_col_name=IndCQC.job_role_filtering_rule,
        raw_col_name=IndCQC.ascwds_job_role_counts,
        clean_col_name=IndCQC.ascwds_job_role_counts,
        populated_rule=JobRoleFilteringRule.populated,
        new_rule_name=new_rule,
        categorical_type=CatColType.JobRoleFilteringRuleCatType,
    )

    return lf


class FilterJobRoleGroupExpressions:
    """
    Collection of Polars expressions for filtering job group outliers.

    This class defines reusable expressions for filtering outlying job role
    group distributions. It also defines column names
    used by these expressions.

    Args:
        include_direct_care_lower_bound (bool): Whether to include the lower bound
            for direct care in the evaluation expression.

    Attributes:
        temp_id_column_sum (str): Temporary column name for the total number of workers.
        job_group_cols (list[str]): List of job group column names.
        upper_bound_suffix (str): A column suffix for denoting upper bounds.
        lower_bound_suffix (str): A column suffix for denoting lower bounds.
        job_group_bounds_cols (list[str]): List of job group bounds columns.
        job_role_group_bounds_locations (dict[str, dict[str, float]]): A dictionary mapping primary service
            types to their respective job role group bounds for locations.
        job_role_group_bounds_brand_prov (dict[str, float]): A dictionary mapping primary service
            types to their respective job role group bounds for brands and providers.
        id_column_sum_expr (pl.Expr): Expression to calculate the total workers.
        job_group_percentage_expr (pl.Expr): Expression to calculate the percentage of job roles.
        evaluation_expr (pl.Expr): Expression to evaluate whether a value is out of bounds.
    """

    temp_id_column_sum: str
    job_group_cols: list[str]
    upper_bound_suffix: str
    lower_bound_suffix: str
    job_group_bounds_cols: list[str]
    job_role_group_bounds_locations: dict[str, dict[str, float]]
    job_role_group_bounds_brand_prov: dict[str, float]
    id_column_sum_expr: pl.Expr
    job_group_percentage_expr: pl.Expr
    evaluation_expr: pl.Expr

    def __init__(self, include_direct_care_lower_bound: bool = True):
        self.temp_id_column_sum = "id_column_sum"
        self.job_group_cols = [
            JobGroupLabels.direct_care,
            JobGroupLabels.managers,
            JobGroupLabels.regulated_professions,
            JobGroupLabels.other,
        ]
        self.upper_bound_suffix = "_upper"
        self.lower_bound_suffix = "_lower"
        self.job_role_group_bounds_cols = [
            JobGroupLabels.direct_care + self.upper_bound_suffix,
            JobGroupLabels.managers + self.upper_bound_suffix,
            JobGroupLabels.regulated_professions + self.upper_bound_suffix,
            JobGroupLabels.other + self.upper_bound_suffix,
            JobGroupLabels.direct_care + self.lower_bound_suffix,
        ]
        self.job_role_group_bounds_locations: dict[str, dict[str, float]] = {
            PrimaryServiceType.care_home_only: {
                f"{JobGroupLabels.direct_care}{self.upper_bound_suffix}": 0.985761,
                f"{JobGroupLabels.managers}{self.upper_bound_suffix}": 0.307057,
                f"{JobGroupLabels.regulated_professions}{self.upper_bound_suffix}": 0.161988,
                f"{JobGroupLabels.other}{self.upper_bound_suffix}": 0.569972,
                f"{JobGroupLabels.direct_care}{self.lower_bound_suffix}": 0.264068,
            },
            PrimaryServiceType.care_home_with_nursing: {
                f"{JobGroupLabels.direct_care}{self.upper_bound_suffix}": 0.943761,
                f"{JobGroupLabels.managers}{self.upper_bound_suffix}": 0.222222,
                f"{JobGroupLabels.regulated_professions}{self.upper_bound_suffix}": 0.350631,
                f"{JobGroupLabels.other}{self.upper_bound_suffix}": 0.964286,
                f"{JobGroupLabels.direct_care}{self.lower_bound_suffix}": 0.012821,
            },
            PrimaryServiceType.non_residential: {
                f"{JobGroupLabels.direct_care}{self.upper_bound_suffix}": 0.995851,
                f"{JobGroupLabels.managers}{self.upper_bound_suffix}": 0.335846,
                f"{JobGroupLabels.regulated_professions}{self.upper_bound_suffix}": 0.338843,
                f"{JobGroupLabels.other}{self.upper_bound_suffix}": 0.576850,
                f"{JobGroupLabels.direct_care}{self.lower_bound_suffix}": 0.233974,
            },
        }
        self.job_role_group_bounds_brand_prov: dict[str, float] = {
            f"{JobGroupLabels.direct_care}{self.upper_bound_suffix}": 0.995851,
            f"{JobGroupLabels.managers}{self.upper_bound_suffix}": 0.335846,
            f"{JobGroupLabels.regulated_professions}{self.upper_bound_suffix}": 0.350631,
            f"{JobGroupLabels.other}{self.upper_bound_suffix}": 0.964286,
            f"{JobGroupLabels.direct_care}{self.lower_bound_suffix}": 0.012821,
        }

        self.id_column_sum_expr = pl.sum_horizontal(self.job_group_cols).alias(
            self.temp_id_column_sum
        )
        # Explicitly handle NULL and zero denominators to avoid ambiguous
        # truthiness when comparing to 0. If the location sum is NULL or 0
        # the resulting percentage should be NULL so downstream logic can
        # distinguish missing data from a valid zero ratio.
        self.job_group_percentage_expr = (
            pl.when(
                pl.col(self.temp_id_column_sum).is_not_null()
                & (pl.col(self.temp_id_column_sum) != 0)
            )
            .then(
                pl.col(self.job_group_cols).cast(pl.Float32)
                / pl.col(self.temp_id_column_sum).cast(pl.Float32)
            )
            .otherwise(pl.lit(None).cast(pl.Float32))
        )

        self.evaluation_expr = (
            (
                pl.col(JobGroupLabels.direct_care)
                > pl.col(JobGroupLabels.direct_care + self.upper_bound_suffix)
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

        lower_bound_check = pl.col(JobGroupLabels.direct_care) < pl.col(
            JobGroupLabels.direct_care + self.lower_bound_suffix
        )

        if include_direct_care_lower_bound:
            self.evaluation_expr = self.evaluation_expr | lower_bound_check


def filter_job_role_group_equal_zero(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Null job role counts at locations with zero direct care or managers +
    regulated professions is zero.

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.

    Returns:
        pl.LazyFrame: LazyFrame with 'ascwds_job_role_counts' conditionally nulled.
    """
    temp_equals_zero_col = "job_role_groups_equal_zero"
    job_group_cols = [
        JobGroupLabels.direct_care,
        JobGroupLabels.managers,
        JobGroupLabels.regulated_professions,
    ]

    # 1. Pivot job roles
    piv_lf = lf.pivot(
        on=IndCQC.main_job_group_labelled,
        on_columns=job_group_cols,
        index=IndCQC.id_per_locationid_import_date,
        values=IndCQC.ascwds_job_role_counts,
        aggregate_function="sum",
    )

    # 2. Evaluate rows
    piv_lf = piv_lf.with_columns(
        (
            (pl.col(JobGroupLabels.direct_care) == pl.lit(0))
            | (
                (pl.col(JobGroupLabels.regulated_professions) == pl.lit(0))
                & (pl.col(JobGroupLabels.managers) == pl.lit(0))
            )
        ).alias(temp_equals_zero_col)
    ).select(IndCQC.id_per_locationid_import_date, temp_equals_zero_col)

    lf = lf.join(piv_lf, on=IndCQC.id_per_locationid_import_date, how="left")
    lf = lf.with_columns(
        pl.when(pl.col(temp_equals_zero_col))
        .then(None)
        .otherwise(pl.col(IndCQC.ascwds_job_role_counts))
        .alias(IndCQC.ascwds_job_role_counts)
    ).drop(temp_equals_zero_col)

    # 3. Update rules column
    lf = update_filtering_rule(  # 7. Add filtering rule
        lf,
        filter_rule_col_name=IndCQC.job_role_filtering_rule,
        raw_col_name=IndCQC.ascwds_job_role_counts,
        clean_col_name=IndCQC.ascwds_job_role_counts,
        populated_rule=JobRoleFilteringRule.populated,
        new_rule_name=JobRoleFilteringRule.missing_direct_care_or_managers_and_profs,
        categorical_type=CatColType.JobRoleFilteringRuleCatType,
    )
    return lf
