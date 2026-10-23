import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)


def add_estimated_slv_counts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds estimated starters, leavers and vacancies counts.

    Employees are filled posts, so the vacancy rate is vacancies as a share of
    employees plus vacancies, which gives vacancies = employees * rate / (1 - rate).

    All counts are 0 when estimated employees is 0 (a vacancy rate of 1 is
    otherwise undefined), and null when employees or the rate is null.

    Args:
        lf (pl.LazyFrame): dataset containing estimated employees and estimated
            starter, turnover and vacancy rates

    Returns:
        pl.LazyFrame: dataset with estimated starters, leavers and vacancies added
    """
    employees = pl.col(EmpStatus.estimated_employees)
    vacancy_rate = pl.col(SLVCols.estimated_vacancy_rate)

    return lf.with_columns(
        pl.when(employees == 0)
        .then(0.0)
        .otherwise(employees * pl.col(SLVCols.estimated_starter_rate))
        .alias(SLVCols.estimated_starters),
        pl.when(employees == 0)
        .then(0.0)
        .otherwise(employees * pl.col(SLVCols.estimated_turnover_rate))
        .alias(SLVCols.estimated_leavers),
        pl.when(employees == 0)
        .then(0.0)
        .otherwise((employees * vacancy_rate) / (1 - vacancy_rate))
        .alias(SLVCols.estimated_vacancies),
    )
