from pathlib import Path

import polars as pl

from polars_utils import logger, utils
from schemas.cqc_locations_delta_flattened_schema import LOCATIONS_FLATTENED_SCHEMA
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
    # Scan parquet to get CQC locations delta flattened data in LazyFrame format
    cqc_lf = utils.scan_parquet(delta_flattened_source)
    # schema=LOCATIONS_FLATTENED_SCHEMA, # TODO - update and include schema once finalised
    logger.info("CQC Location delta flattened LazyFrame read in")

    # ADD CODE HERE

    # Store flattened data in s3
    utils.sink_to_parquet(
        cqc_lf,
        full_flattened_destination,
        logger=logger,
        partition_cols=cqc_partition_keys,
        append=False,
    )


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
