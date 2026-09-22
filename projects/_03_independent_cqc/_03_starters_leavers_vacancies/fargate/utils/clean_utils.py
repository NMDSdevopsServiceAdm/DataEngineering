import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)


def create_slv_rate_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds turnover, starter, and vacancy rate columns.

    Rates are simple fractions (not percentages). Turnover and starter rate have no
    upper bound, since leavers/starters can exceed employees for a small team in a
    period; vacancy rate is mathematically bounded to [0, 1].

    Employees can legitimately be 0 for a job role, so turnover_rate and
    starter_rate are null whenever employees is 0 (the rate is undefined,
    regardless of the numerator). vacancy_rate is still computed when there
    are vacancies with 0 employees (e.g. 1 vacancy and 0 employees is a
    meaningful 100% vacant), and is only null when employees and vacancies
    are both 0.

    Args:
        lf (pl.LazyFrame): dataset containing employees and deduplicated starters,
            leavers and vacancies

    Returns:
        pl.LazyFrame: dataset with turnover_rate, starter_rate and vacancy_rate added
    """
    employees_plus_vacancies = pl.col(EmpStatus.employee_count) + pl.col(
        SLVCols.vacancies_dedup
    )

    return lf.with_columns(
        pl.when(pl.col(EmpStatus.employee_count) == 0)
        .then(None)
        .otherwise(pl.col(SLVCols.leavers_dedup) / pl.col(EmpStatus.employee_count))
        .cast(pl.Float32)
        .alias(SLVCols.turnover_rate),
        pl.when(pl.col(EmpStatus.employee_count) == 0)
        .then(None)
        .otherwise(pl.col(SLVCols.starters_dedup) / pl.col(EmpStatus.employee_count))
        .cast(pl.Float32)
        .alias(SLVCols.starter_rate),
        pl.when(employees_plus_vacancies == 0)
        .then(None)
        .otherwise(pl.col(SLVCols.vacancies_dedup) / employees_plus_vacancies)
        .cast(pl.Float32)
        .alias(SLVCols.vacancy_rate),
    )
