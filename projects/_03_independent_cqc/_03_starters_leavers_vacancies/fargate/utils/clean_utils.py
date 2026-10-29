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

    Rates are fractions (not percentages) of estimated employees, which can be
    fractional. Turnover and starter rate have no upper bound, as leavers/starters
    can exceed employees for a small team; vacancy rate is bounded to [0, 1].

    All rates are null when estimated employees is null. Turnover and starter rate
    are also null when employees is 0 (undefined). Vacancy rate is still computed
    for vacancies with 0 employees (100% vacant), and is null only when both are 0.

    Args:
        lf (pl.LazyFrame): dataset containing estimated employees and cleaned
            starters, leavers and vacancies

    Returns:
        pl.LazyFrame: dataset with turnover_rate, starter_rate and vacancy_rate added
    """
    employees_plus_vacancies = pl.col(EmpStatus.estimated_employees) + pl.col(
        SLVCols.ascwds_vacancies_clean
    )

    return lf.with_columns(
        pl.when(pl.col(EmpStatus.estimated_employees) == 0)
        .then(None)
        .otherwise(
            pl.col(SLVCols.ascwds_leavers_clean) / pl.col(EmpStatus.estimated_employees)
        )
        .cast(pl.Float32)
        .alias(SLVCols.ascwds_turnover_rate),
        pl.when(pl.col(EmpStatus.estimated_employees) == 0)
        .then(None)
        .otherwise(
            pl.col(SLVCols.ascwds_starters_clean)
            / pl.col(EmpStatus.estimated_employees)
        )
        .cast(pl.Float32)
        .alias(SLVCols.ascwds_starter_rate),
        pl.when(employees_plus_vacancies == 0)
        .then(None)
        .otherwise(pl.col(SLVCols.ascwds_vacancies_clean) / employees_plus_vacancies)
        .cast(pl.Float32)
        .alias(SLVCols.ascwds_vacancy_rate),
    )
