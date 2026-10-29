import polars as pl

from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc._02_employment_status.fargate.utils.impute_utils import (
    IMPUTED_PCT_COLUMNS,
    PCT_COLUMNS,
    ROLLING_AVG_PCT_COLUMNS,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EmploymentStatusEstimateSource

ESTIMATED_PCT_COLUMNS: list[str] = [
    EmpStatus.estimated_permanent_pct,
    EmpStatus.estimated_temporary_pct,
    EmpStatus.estimated_bank_or_pool_pct,
    EmpStatus.estimated_agency_pct,
    EmpStatus.estimated_other_pct,
]

SOURCE_BY_COLUMN: dict[str, str] = {
    col: source
    for cols, source in (
        (PCT_COLUMNS, EmploymentStatusEstimateSource.cleaned),
        (IMPUTED_PCT_COLUMNS, EmploymentStatusEstimateSource.imputed),
        (
            ROLLING_AVG_PCT_COLUMNS,
            EmploymentStatusEstimateSource.rolling_avg,
        ),
    )
    for col in cols
}

ESTIMATED_COUNT_COLUMNS: list[str] = [
    EmpStatus.estimated_permanent_count,
    EmpStatus.estimated_temporary_count,
    EmpStatus.estimated_bank_or_pool_count,
    EmpStatus.estimated_agency_count,
    EmpStatus.estimated_other_count,
]


def add_estimated_employment_status_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds estimated employment status percentages, their source, counts and estimated employees.

    Each estimated percentage takes the first populated of the cleaned, imputed and
    rolling average percentages, and the source column records which was used. Each estimated
    count is the estimated percentage multiplied by the filled-post metric. Estimated employees is
    the permanent plus temporary counts. Rows with no populated percentage, or no metric, are null.

    Args:
        lf (pl.LazyFrame): dataset with the cleaned, imputed and rolling average percentage
            columns and the job role filled-post metric.

    Returns:
        pl.LazyFrame: dataset with 5 "estimated_emp_stat_<status>_pct" columns, a
            percentage estimate source column, 5 "estimated_emp_stat_<status>_count" columns and an
            "estimated_employees" column added.
    """
    metric = IndCQC.estimate_filled_posts_by_job_role

    percentage_and_source_exprs = [
        utils.coalesce_with_source_labels(
            cols=[cleaned, imputed, rolling_avg], name=estimated
        )
        for cleaned, imputed, rolling_avg, estimated in zip(
            PCT_COLUMNS,
            IMPUTED_PCT_COLUMNS,
            ROLLING_AVG_PCT_COLUMNS,
            ESTIMATED_PCT_COLUMNS,
        )
    ]
    status_source_columns = [f"{col}_source" for col in ESTIMATED_PCT_COLUMNS]

    lf = lf.with_columns(
        expr for exprs in percentage_and_source_exprs for expr in exprs
    )

    lf = lf.with_columns(
        pl.coalesce(status_source_columns)
        .replace_strict(SOURCE_BY_COLUMN)
        .cast(CatColType.EmploymentStatusEstimateSourceEnumType)
        .alias(EmpStatus.estimated_pct_source)
    ).drop(status_source_columns)

    lf = lf.with_columns(
        (pl.col(metric) * pl.col(estimated_percentage)).alias(estimated_count)
        for estimated_percentage, estimated_count in zip(
            ESTIMATED_PCT_COLUMNS, ESTIMATED_COUNT_COLUMNS
        )
    )

    return lf.with_columns(
        (
            pl.col(EmpStatus.estimated_permanent_count)
            + pl.col(EmpStatus.estimated_temporary_count)
        ).alias(EmpStatus.estimated_employees)
    )
