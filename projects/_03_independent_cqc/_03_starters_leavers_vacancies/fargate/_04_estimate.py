import polars as pl

from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes as CatColType
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


def main(
    imputed_data_source: str,
    estimated_data_destination: str,
) -> None:
    """Creates estimated starter, turnover and vacancy rates and counts.

    Each estimated rate is the first populated of the listed columns.

    Args:
        imputed_data_source (str): Path to the imputed data.
        estimated_data_destination (str): Destination for output.
    """
    lf = utils.scan_parquet(imputed_data_source)

    lf = lf.with_columns(
        *utils.coalesce_with_source_labels(
            cols=[SLVCols.starter_rate],
            name=SLVCols.estimated_starter_rate,
            source_dtype=CatColType.StarterRateEstimateSourceEnumType,
        ),
        *utils.coalesce_with_source_labels(
            cols=[SLVCols.turnover_rate],
            name=SLVCols.estimated_turnover_rate,
            source_dtype=CatColType.TurnoverRateEstimateSourceEnumType,
        ),
        *utils.coalesce_with_source_labels(
            cols=[SLVCols.vacancy_rate],
            name=SLVCols.estimated_vacancy_rate,
            source_dtype=CatColType.VacancyRateEstimateSourceEnumType,
        ),
    )

    lf = add_estimated_slv_counts(lf)

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=estimated_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--imputed_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--estimated_data_destination",
            "Destination s3 directory for estimated data",
        ),
    )
    main(
        imputed_data_source=args.imputed_data_source,
        estimated_data_destination=args.estimated_data_destination,
    )
