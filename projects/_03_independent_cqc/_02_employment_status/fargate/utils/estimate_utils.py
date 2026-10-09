import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc._02_employment_status.fargate.utils.impute_utils import (
    FULL_IMPUTED_PERCENTAGE_COLUMNS,
    PERCENTAGE_COLUMNS,
    ROLLING_AVERAGE_PERCENTAGE_COLUMNS,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EmploymentStatusEstimateSource

ESTIMATED_PERCENTAGE_COLUMNS: list[str] = [
    EmpStatus.estimated_permanent_percentage,
    EmpStatus.estimated_temporary_percentage,
    EmpStatus.estimated_bank_or_pool_percentage,
    EmpStatus.estimated_agency_percentage,
    EmpStatus.estimated_other_percentage,
]

ESTIMATED_COUNT_COLUMNS: list[str] = [
    EmpStatus.estimated_emp_stat_permanent,
    EmpStatus.estimated_emp_stat_temporary,
    EmpStatus.estimated_emp_stat_bank_or_pool,
    EmpStatus.estimated_emp_stat_agency,
    EmpStatus.estimated_emp_stat_other,
]


def add_estimated_employment_status_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds estimated employment status percentages, their source, counts and estimated employees.

    Each estimated percentage takes the first populated of the cleaned, short-term imputed and
    rolling average percentages, and the source column records which was used. Each estimated
    count is the estimated percentage multiplied by the filled-post metric. Estimated employees is
    the permanent plus temporary counts. Rows with no populated percentage, or no metric, are null.

    Args:
        lf (pl.LazyFrame): dataset with the cleaned, imputed and rolling average percentage
            columns and the job role filled-post metric.

    Returns:
        pl.LazyFrame: dataset with 5 "estimated_emplstat_<status>_percentage" columns, a
            percentage estimate source column, 5 "estimated_emp_stat_<status>" columns and an
            "estimated_employees" column added.
    """
    metric = IndCQC.estimate_filled_posts_by_job_role

    lf = lf.with_columns(
        pl.coalesce(cleaned, imputed, rolling_avg).alias(estimated)
        for cleaned, imputed, rolling_avg, estimated in zip(
            PERCENTAGE_COLUMNS,
            FULL_IMPUTED_PERCENTAGE_COLUMNS,
            ROLLING_AVERAGE_PERCENTAGE_COLUMNS,
            ESTIMATED_PERCENTAGE_COLUMNS,
        )
    )

    # a tier's 5 percentages are all populated or all null, so permanent stands in for all 5
    lf = lf.with_columns(
        pl.when(pl.col(EmpStatus.permanent_percentage).is_not_null())
        .then(pl.lit(EmploymentStatusEstimateSource.cleaned))
        .when(pl.col(EmpStatus.permanent_percentage_full_imputed).is_not_null())
        .then(pl.lit(EmploymentStatusEstimateSource.imputed))
        .when(pl.col(EmpStatus.permanent_percentage_rolling_avg).is_not_null())
        .then(pl.lit(EmploymentStatusEstimateSource.rolling_avg))
        .cast(CatColType.EmploymentStatusEstimateSourceEnumType)
        .alias(EmpStatus.estimated_percentage_source)
    )

    lf = lf.with_columns(
        (pl.col(metric) * pl.col(estimated_percentage)).alias(estimated_count)
        for estimated_percentage, estimated_count in zip(
            ESTIMATED_PERCENTAGE_COLUMNS, ESTIMATED_COUNT_COLUMNS
        )
    )

    return lf.with_columns(
        (
            pl.col(EmpStatus.estimated_emp_stat_permanent)
            + pl.col(EmpStatus.estimated_emp_stat_temporary)
        ).alias(EmpStatus.estimated_employees)
    )
