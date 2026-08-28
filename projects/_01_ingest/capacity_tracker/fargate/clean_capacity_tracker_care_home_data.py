import polars as pl

from polars_utils import utils
from polars_utils.cleaning_utils import column_to_date
from polars_utils.column_types import CategoricalColumnTypes
from projects._01_ingest.capacity_tracker.fargate.utils import (
    clean_capacity_tracker_utils as ctUtils,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome

CAPACITY_TRACKER_CARE_HOME_COLUMNS = [
    CTCH.cqc_id,
    CTCH.nurses_employed,
    CTCH.care_workers_employed,
    CTCH.non_care_workers_employed,
    CTCH.agency_nurses_employed,
    CTCH.agency_care_workers_employed,
    CTCH.agency_non_care_workers_employed,
    Keys.import_date,
]
COLUMNS_TO_CAST_TO_INT = [
    CTCH.nurses_employed,
    CTCH.care_workers_employed,
    CTCH.non_care_workers_employed,
    CTCH.agency_nurses_employed,
    CTCH.agency_care_workers_employed,
    CTCH.agency_non_care_workers_employed,
]
COLUMNS_TO_BOUND = [
    CTCH.nurses_employed,
    CTCH.care_workers_employed,
    CTCH.non_care_workers_employed,
    CTCH.agency_non_care_workers_employed,
]
MAX_BOUND_DIRECTLY_EMPLOYED: int = 1000


def main(
    capacity_tracker_care_home_source: str,
    cleaned_capacity_tracker_care_home_destination: str,
) -> None:
    """
    Clean raw Capacity Tracker care home data.

    Args:
        capacity_tracker_care_home_source (str): Source S3 directory for the raw parquet
            capacity tracker care home dataset.
        cleaned_capacity_tracker_care_home_destination (str): Destination S3 directory for
            the cleaned parquet capacity tracker care home dataset.
    """
    schema = {column: pl.String for column in CAPACITY_TRACKER_CARE_HOME_COLUMNS}
    care_home_lf = utils.scan_parquet(
        capacity_tracker_care_home_source,
        schema=schema,
        selected_columns=CAPACITY_TRACKER_CARE_HOME_COLUMNS,
    )

    care_home_lf = care_home_lf.with_columns(
        pl.lit(CareHome.care_home).alias(CTCHClean.care_home)
    )

    care_home_lf = care_home_lf.with_columns(
        pl.col(COLUMNS_TO_CAST_TO_INT).cast(pl.Int32, strict=False)
    )

    care_home_lf = column_to_date(
        care_home_lf, Keys.import_date, CTCHClean.ct_care_home_import_date
    ).drop(Keys.import_date)

    care_home_lf = care_home_lf.filter(
        ctUtils.agency_and_non_agency_values_differ_filter()
    )

    care_home_lf = care_home_lf.with_columns(
        ctUtils.bound_columns(COLUMNS_TO_BOUND, upper_limit=MAX_BOUND_DIRECTLY_EMPLOYED)
    )

    care_home_lf = ctUtils.add_total_employed_columns(care_home_lf)

    # Cast to Categorical/Enum here so it's saved in the output parquet file.
    care_home_lf = care_home_lf.with_columns(
        pl.col(CTCHClean.care_home).cast(CategoricalColumnTypes.CareHomeEnumType)
    )

    utils.sink_to_parquet(care_home_lf, cleaned_capacity_tracker_care_home_destination)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--capacity_tracker_care_home_source",
            "Source s3 directory for parquet capacity tracker care home dataset",
        ),
        (
            "--cleaned_capacity_tracker_care_home_destination",
            "Destination s3 directory for cleaned parquet capacity tracker care home dataset",
        ),
    )
    main(
        args.capacity_tracker_care_home_source,
        args.cleaned_capacity_tracker_care_home_destination,
    )
