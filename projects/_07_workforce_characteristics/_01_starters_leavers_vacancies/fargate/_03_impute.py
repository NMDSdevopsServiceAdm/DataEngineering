from dataclasses import dataclass

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.impute_utils as imputeUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols


@dataclass
class NumericalValues:
    """Numerical constants used in this process."""

    # Initial time-limited imputation pass (ticket 2011): a flat 6-month
    # forward-fill window (no size-tiering yet), to review real outputs
    # before deciding whether a longer or size-tiered window is warranted.
    forward_fill_time_limit: str = "6mo"


def main(
    cleaned_data_source: str,
    imputed_data_destination: str,
) -> None:
    """
    Forward-fills gaps in the deduplicated count and rate columns.

    Each deduplicated column's last known value is carried forward into
    later null rows, but only within a bounded time window
    (`NumericalValues.forward_fill_time_limit`) per location and published
    job role. This does not backward-fill or interpolate between known
    values.

    Args:
        cleaned_data_source (str): path to the cleaned data
        imputed_data_destination (str): destination for output
    """
    lf = utils.scan_parquet(cleaned_data_source)

    lf = imputeUtils.forward_fill_within_time_limit(
        lf,
        columns_to_fill={
            SLVCols.starters_dedup: SLVCols.starters_imputed,
            SLVCols.leavers_dedup: SLVCols.leavers_imputed,
            SLVCols.vacancies_dedup: SLVCols.vacancies_imputed,
            SLVCols.turnover_rate_dedup: SLVCols.turnover_rate_imputed,
            SLVCols.starter_rate_dedup: SLVCols.starter_rate_imputed,
            SLVCols.vacancy_rate_dedup: SLVCols.vacancy_rate_imputed,
        },
        partition_by_columns=[IndCQC.location_id, SLVCols.published_job_role_label],
        date_column=IndCQC.cqc_location_import_date,
        time_limit=NumericalValues.forward_fill_time_limit,
    )

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=imputed_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--imputed_data_destination",
            "Destination s3 directory for imputed data",
        ),
    )
    main(
        cleaned_data_source=args.cleaned_data_source,
        imputed_data_destination=args.imputed_data_destination,
    )
