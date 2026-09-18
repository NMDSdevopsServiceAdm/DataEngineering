import polars as pl

from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols

NOT_KNOWN_CODE = 999  # '999' is used elsewhere in ASCWDS to represent not known.


def null_not_known_slv_values(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls starters, leavers and vacancies values that use 999 as a not known code.

    Args:
        lf (pl.LazyFrame): dataset containing starters, leavers and vacancies

    Returns:
        pl.LazyFrame: dataset with 999 starters, leavers and vacancies values nulled
    """
    slv_columns = [SLVCols.starters, SLVCols.leavers, SLVCols.vacancies]
    return lf.with_columns(
        pl.when(pl.col(column) != NOT_KNOWN_CODE)
        .then(pl.col(column))
        .otherwise(None)
        .alias(column)
        for column in slv_columns
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
        lf (pl.LazyFrame): dataset containing employees, starters, leavers and vacancies

    Returns:
        pl.LazyFrame: dataset with turnover_rate, starter_rate and vacancy_rate added
    """
    employees_plus_vacancies = pl.col(SLVCols.employees) + pl.col(SLVCols.vacancies)

    return lf.with_columns(
        pl.when(pl.col(SLVCols.employees) == 0)
        .then(None)
        .otherwise(pl.col(SLVCols.leavers) / pl.col(SLVCols.employees))
        .cast(pl.Float32)
        .alias(SLVCols.turnover_rate),
        pl.when(pl.col(SLVCols.employees) == 0)
        .then(None)
        .otherwise(pl.col(SLVCols.starters) / pl.col(SLVCols.employees))
        .cast(pl.Float32)
        .alias(SLVCols.starter_rate),
        pl.when(employees_plus_vacancies == 0)
        .then(None)
        .otherwise(pl.col(SLVCols.vacancies) / employees_plus_vacancies)
        .cast(pl.Float32)
        .alias(SLVCols.vacancy_rate),
    )
