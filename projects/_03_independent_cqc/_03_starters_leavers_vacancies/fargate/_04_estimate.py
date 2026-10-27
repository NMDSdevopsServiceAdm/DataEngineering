from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)


def main(
    imputed_data_source: str,
    estimated_data_destination: str,
) -> None:
    """Creates estimated starters, leavers and vacancies.

    Each estimate is the first populated of the listed columns.

    Args:
        imputed_data_source (str): Path to the imputed data.
        estimated_data_destination (str): Destination for output.
    """
    lf = utils.scan_parquet(imputed_data_source)

    lf = lf.with_columns(
        *utils.coalesce_with_source_labels(
            cols=[SLVCols.starters_cleaned],
            name=SLVCols.estimated_starters,
            source_dtype=CatColType.StartersEstimateSourceEnumType,
        ),
        *utils.coalesce_with_source_labels(
            cols=[SLVCols.leavers_cleaned],
            name=SLVCols.estimated_leavers,
            source_dtype=CatColType.LeaversEstimateSourceEnumType,
        ),
        *utils.coalesce_with_source_labels(
            cols=[SLVCols.vacancies_cleaned],
            name=SLVCols.estimated_vacancies,
            source_dtype=CatColType.VacanciesEstimateSourceEnumType,
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
