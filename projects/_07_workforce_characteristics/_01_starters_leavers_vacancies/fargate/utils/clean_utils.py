import polars as pl

from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols


def create_slv_rate_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds turnover, starter, and vacancy rate columns.

    Rates are simple fractions (not percentages). Turnover and starter rate have no
    upper bound, since leavers/starters can exceed employees for a small team in a
    period; vacancy rate is mathematically bounded to [0, 1].

    Args:
        lf (pl.LazyFrame): dataset containing employees, starters, leavers and vacancies

    Returns:
        pl.LazyFrame: dataset with turnover_rate, starter_rate and vacancy_rate added
    """
    return lf.with_columns(
        (pl.col(SLVCols.leavers) / pl.col(SLVCols.employees))
        .cast(pl.Float32)
        .alias(SLVCols.turnover_rate),
        (pl.col(SLVCols.starters) / pl.col(SLVCols.employees))
        .cast(pl.Float32)
        .alias(SLVCols.starter_rate),
        (
            pl.col(SLVCols.vacancies)
            / (pl.col(SLVCols.employees) + pl.col(SLVCols.vacancies))
        )
        .cast(pl.Float32)
        .alias(SLVCols.vacancy_rate),
    )
