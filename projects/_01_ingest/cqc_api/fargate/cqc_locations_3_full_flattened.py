import polars as pl

from polars_utils import logger, utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
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
    logger.info("CQC Location delta flattened LazyFrame read in")

    # Identify unique import_dates in source delta data, sorted in order
    source_import_dates = (
        entire_delta_lf.select(Keys.import_date)
        .unique()
        .sort(Keys.import_date)
        .collect()
        .to_series()
        .to_list()
    )
    logger.info(f"Found {len(source_import_dates)} import_dates in delta data source.")

    # Identify import_dates already present in destination
    dest_import_dates = utils.list_s3_parquet_import_dates(full_flattened_destination)
    logger.info(f"Found {len(dest_import_dates)} import_dates already in destination.")

    # Determine which import_dates to process, if any
    import_dates_to_process = [
        d for d in source_import_dates if d not in dest_import_dates
    ]

    if not import_dates_to_process:
        logger.info("No new import_dates require processing.")
        return

    logger.info(
        f"Processing {len(import_dates_to_process)} new import_dates: {import_dates_to_process}"
    )

    # If there is an existing full snapshot in destination, load the latest one.
    latest_existing_date = None
    full_lf = None
    if dest_import_dates:
        latest_existing_date = max(dest_import_dates)
        logger.info(
            f"Loading latest existing full snapshot import_date={latest_existing_date}"
        )
        # Lazy scan and filter down to that snapshot (keeps it lazy)
        full_lf = utils.scan_parquet(full_flattened_destination).filter(
            pl.col(Keys.import_date) == latest_existing_date
        )

    # Loop through new import_dates in chronological order
    for index, delta_import_date in enumerate(sorted(import_dates_to_process)):
        logger.info(
            f"Processing import_date={delta_import_date} ({index+1} of {len(import_dates_to_process)})"
        )

        # Filter delta for this import_date
        delta_lf = entire_delta_lf.filter(pl.col(Keys.import_date) == delta_import_date)

        # If there is no previous full_lf then the merged snapshot is the delta for the first ever snapshot.
        # If there is, merge delta data with previous full snapshot, keeping the latest record for each location_id.
        if full_lf is None:
            merged_lf = delta_lf
        else:
            merged_lf = pl.concat([full_lf, delta_lf]).unique(
                subset=[CQCLClean.location_id], keep="last"
            )

        # Assign partition columns based on current import_date
        date_str = str(delta_import_date)
        merged_lf = merged_lf.with_columns(
            [
                pl.lit(date_str).alias(Keys.import_date),
                pl.lit(date_str[:4]).alias(Keys.year),
                pl.lit(date_str[4:6]).alias(Keys.month),
                pl.lit(date_str[6:8]).alias(Keys.day),
            ]
        )

        # Store this snapshot in data in S3
        utils.sink_to_parquet(
            merged_lf,
            full_flattened_destination,
            logger=logger,
            partition_cols=cqc_partition_keys,
            append=False,
        )

        # Reassign this merged snapshot for next delta if required
        full_lf = merged_lf

    logger.info("All new import_dates processed successfully")


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
