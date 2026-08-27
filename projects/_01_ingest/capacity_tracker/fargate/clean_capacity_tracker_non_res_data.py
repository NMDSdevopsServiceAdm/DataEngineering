import polars as pl

from polars_utils import utils
from polars_utils.cleaning_utils import column_to_date
from polars_utils.column_types import CategoricalColumnTypes
from projects._01_ingest.capacity_tracker.fargate.utils import (
    clean_capacity_tracker_utils as ctUtils,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResColumns as CTNR,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome

CAPACITY_TRACKER_NON_RES_COLUMNS = [
    CTNR.cqc_id,
    CTNR.cqc_care_workers_employed,
    CTNR.service_user_count,
    Keys.import_date,
]
COLUMNS_TO_CAST_TO_INT = [CTNR.cqc_care_workers_employed, CTNR.service_user_count]
COLUMNS_TO_BOUND = [CTNR.cqc_care_workers_employed, CTNR.service_user_count]
MIN_BOUND: int = 1
MAX_BOUND: int = 3000


def main(
    capacity_tracker_non_res_source: str,
    cleaned_capacity_tracker_non_res_destination: str,
) -> None:
    """
    Clean raw Capacity Tracker non-residential data.

    Args:
        capacity_tracker_non_res_source (str): Source S3 directory for the raw parquet
            capacity tracker non-residential dataset.
        cleaned_capacity_tracker_non_res_destination (str): Destination S3 directory for
            the cleaned parquet capacity tracker non-residential dataset.
    """
    schema = {column: pl.String for column in CAPACITY_TRACKER_NON_RES_COLUMNS}
    non_res_lf = utils.scan_parquet(
        capacity_tracker_non_res_source,
        schema=schema,
        selected_columns=CAPACITY_TRACKER_NON_RES_COLUMNS,
    )

    non_res_lf = non_res_lf.with_columns(
        pl.lit(CareHome.not_care_home).alias(CTNRClean.care_home)
    )

    non_res_lf = non_res_lf.with_columns(
        pl.col(COLUMNS_TO_CAST_TO_INT).cast(pl.Int32, strict=False)
    )

    non_res_lf = column_to_date(
        non_res_lf, Keys.import_date, CTNRClean.ct_non_res_import_date
    ).drop(Keys.import_date)

    non_res_lf = non_res_lf.with_columns(
        ctUtils.bound_columns(
            COLUMNS_TO_BOUND, lower_limit=MIN_BOUND, upper_limit=MAX_BOUND
        )
    )

    # Cast to Categorical/Enum here so it's saved in the output parquet file.
    non_res_lf = non_res_lf.with_columns(
        pl.col(CTNRClean.care_home).cast(CategoricalColumnTypes.CareHomeEnumType)
    )

    utils.sink_to_parquet(non_res_lf, cleaned_capacity_tracker_non_res_destination)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--capacity_tracker_non_res_source",
            "Source s3 directory for parquet capacity tracker non residential dataset",
        ),
        (
            "--cleaned_capacity_tracker_non_res_destination",
            "Destination s3 directory for cleaned parquet capacity tracker non residential dataset",
        ),
    )
    main(
        args.capacity_tracker_non_res_source,
        args.cleaned_capacity_tracker_non_res_destination,
    )
