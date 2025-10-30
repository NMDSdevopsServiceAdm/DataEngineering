import polars as pl

from polars_utils import logger, utils
from projects._01_ingest.cqc_api.fargate.utils import (
    locations_3_full_flattened_utils as fUtils,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

logger = logger.get_logger(__name__)

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    delta_flattened_source: str,
    full_flattened_destination: str,
) -> None:
    """
    Builds a full flattened CQC locations dataset from delta files.

    Only processes import_dates not already present in the destination.

    Args:
        delta_flattened_source (str): S3 URI to read delta flattened CQC locations data from
        full_flattened_destination (str): S3 URI to save full flattened CQC locations data to
    """
    # Scan delta flattened data in LazyFrame format
    entire_delta_lf = utils.scan_parquet(delta_flattened_source)
    expected_schema = entire_delta_lf.collect_schema()
    logger.info("CQC Location delta flattened LazyFrame read in")

    existing_full_import_dates = utils.list_s3_parquet_import_dates(
        full_flattened_destination
    )

    import_dates_to_process = fUtils.get_import_dates_to_process(
        entire_delta_lf, existing_full_import_dates
    )

    if not import_dates_to_process:
        logger.info("No new import_dates require processing. Job complete")
        return

    logger.info(f"existing_full_import_dates: {existing_full_import_dates}")
    logger.info(f"import_dates_to_process: {import_dates_to_process}")

    full_lf = fUtils.load_latest_snapshot(
        full_flattened_destination, existing_full_import_dates
    )

    # for delta_import_date in sorted(import_dates_to_process):
    #     logger.info(f"Processing import_date={delta_import_date}")

    #     delta_lf = entire_delta_lf.filter(pl.col(Keys.import_date) == delta_import_date)

    #     merged_lf = fUtils.create_full_snapshot(full_lf, delta_lf)
    #     merged_lf = fUtils.apply_partitions(merged_lf, delta_import_date)
    #     merged_lf = merged_lf.cast(expected_schema)

    #     utils.sink_to_parquet(
    #         merged_lf,
    #         full_flattened_destination,
    #         logger=logger,
    #         partition_cols=cqc_partition_keys,
    #         append=False,
    #     )

    #     full_lf = merged_lf


if __name__ == "__main__":
    logger.info("Running Full Flattened CQC Locations job")

    args = utils.get_args(
        (
            "--delta_flattened_source",
            "S3 URI to read delta flattened CQC locations data from",
        ),
        (
            "--full_flattened_destination",
            "S3 URI to save full flattened CQC locations data to",
        ),
    )

    main(
        delta_flattened_source=args.delta_flattened_source,
        full_flattened_destination=args.full_flattened_destination,
    )

    logger.info("Finished Full Flattened CQC Locations job")
