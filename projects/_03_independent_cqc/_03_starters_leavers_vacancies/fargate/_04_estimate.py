from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)


def main(
    imputed_data_source: str,
    estimated_data_destination: str,
) -> None:
    """Creates estimated starter, turnover and vacancy rates.

    Each estimated rate is the first populated of the listed columns, or null
    if none are populated. The coalesce lists a single column for now, as it
    will grow (imputed, rolling average, modelled).

    Adds, per rate, a Float32 estimated rate and an Enum source column holding
    the name of the column it came from (null when the rate is null). Input
    columns are kept.

    Args:
        imputed_data_source (str): Path to the imputed data.
        estimated_data_destination (str): Destination for output.

    Returns:
        None: Writes the data with these columns added:
            - estimated_starter_rate, estimated_starter_rate_source
            - estimated_turnover_rate, estimated_turnover_rate_source
            - estimated_vacancy_rate, estimated_vacancy_rate_source
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
