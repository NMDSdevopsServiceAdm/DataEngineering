import polars as pl

from polars_utils import logger, utils
from projects._01_ingest.cqc_api.fargate.utils import (
    convert_delta_to_full_utils as cUtils,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

logger = logger.get_logger(__name__)

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def convert_delta_to_full(
    delta_source: str, full_destination: str, dataset: str
) -> None:
    """
    Builds a full dataset from delta files for named datasets.

    Only processes import_dates not already present in the destination.

    Args:
        delta_source (str): S3 URI to read delta flattened CQC locations data from
        full_destination (str): S3 URI to save full flattened CQC locations data to
        dataset (str): Dataset name ('locations' or 'providers').

    Raises:
        ValueError: If the dataset is not supported
    """
    match dataset:
        case "locations":
            primary_key = CQCLClean.location_id
        case "providers":
            primary_key = CQCLClean.provider_id
        case _:
            raise ValueError(
                f"Unknown dataset name: {dataset}. Must be either 'locations' or 'providers'."
            )

    # Scan delta flattened data in LazyFrame format
    entire_delta_lf = utils.scan_parquet(delta_source)
    expected_schema = entire_delta_lf.collect_schema()
    logger.info("Delta dataset LazyFrame read in")

    dates_to_process, processed_dates = cUtils.allocate_import_dates(
        delta_source, full_destination
    )

    if not dates_to_process:
        logger.info("No new import_dates require processing. Job complete.")
        return

    full_lf = cUtils.load_latest_snapshot(full_destination, processed_dates)

    for delta_import_date in sorted(dates_to_process):
        logger.info(f"Processing import_date={delta_import_date}")

        delta_lf = entire_delta_lf.filter(pl.col(Keys.import_date) == delta_import_date)

        merged_lf = cUtils.create_full_snapshot(full_lf, delta_lf, primary_key)
        merged_lf = cUtils.apply_partitions(merged_lf, delta_import_date)
        merged_lf = merged_lf.cast(expected_schema)

        utils.sink_to_parquet(
            merged_lf,
            full_destination,
            logger=logger,
            partition_cols=cqc_partition_keys,
            append=False,
        )

        full_lf = merged_lf
