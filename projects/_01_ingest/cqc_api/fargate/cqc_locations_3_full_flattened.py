from polars_utils import logger, utils
import polars as pl
from datetime import datetime
from re import match, search
from s3fs import S3FileSystem
from typing import Generator, Optional
from schemas import cqc_locations_cleaned_schema_polars as LocationsSchemaCleaned
from schemas import cqc_locations_schema_polars as LocationsSchema
from schemas import cqc_provider_schema_polars as ProvidersSchema
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CqcLocations,
)
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CqcProviders,
)


logger = logger.get_logger(__name__)

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


class DataError(Exception):
    pass


def main(
    dataset: str,
    latest: bool,
    input_uri: str,
    output_uri: str,
    partition: Optional[str] = None,
) -> None:
    fs = S3FileSystem()
    input_parse = match(
        "s3://(?P<bucket>[\w\-=.]+)/(?P<read_folder>[\w/-=.]+)", input_uri
    )

    bucket_prefix = f"{input_parse.group('bucket')}/{input_parse.group('read_folder')}"

    # If partition provided, process just that one
    if partition:
        partitions = [partition]
    else:
        # Otherwise discover all partitions
        base_paths = fs.find(f"s3://{bucket_prefix}")
        partitions = [
            "/".join(p.removeprefix(bucket_prefix).split("/")[:-1])
            for p in base_paths
            if "import_date=" in p
        ]
        partitions = sorted(list(set(partitions)))
        if not partitions:
            logger.warning("No partitions found under %s", bucket_prefix)
            return

        # Sort by import_date extracted from path
        partitions.sort(key=lambda p: int(search(r"import_date=(\d{8})", p).group(1)))

        if latest:
            # pick only the latest one
            latest_partition = partitions[-1]
            logger.info(f"Latest partition selected: {latest_partition}")
            partitions = [latest_partition]

    logger.info("Partitions to process: %s", partitions)

    for partition in partitions:
        logger.info("Processing partition: %s", partition)
        snapshot_df = build_snapshot_table_from_delta(
            bucket=input_parse.group("bucket"),
            read_folder=input_parse.group("read_folder"),
            dataset=dataset,
            partition=partition,
        )
        if snapshot_df is None:
            continue

        output_path = f"{output_uri.rstrip('/')}/{partition.lstrip('/')}/file.parquet"
        with fs.open(output_path, "wb") as destination:
            snapshot_df.drop(
                [Keys.year, Keys.month, Keys.day, Keys.import_date]
            ).write_parquet(destination, compression="snappy")
        logger.info(f"Wrote snapshot to {output_path}")


def build_snapshot_table_from_delta(
    bucket: str,
    read_folder: str,
    dataset: str,
    partition: str,
) -> Optional[pl.DataFrame]:
    """
    Gets full snapshot of data at a given timepoint
    Args:
        bucket (str): delta dataset bucket
        read_folder (str): delta dataset folder
        dataset (str): CQC organisation type (locations or providers)
        partition (str): partition string of the base path which is used to get data for timepoint (yyyymmdd)
    Returns:
        Optional[pl.DataFrame]: Snapshot pl.DataFrame, if one exists, else None
    """
    # get import date from partition str and save as timepoint
    timepoint = int(search(r"import_date=(\d{8})", partition).group(1))
    for snapshot in get_snapshots(bucket, read_folder, dataset):
        if snapshot.item(1, Keys.import_date) == timepoint:
            return snapshot

    logger.info(
        f"No snapshot found for partition {partition} (import_date={timepoint})"
    )
    return None


def get_snapshots(
    bucket: str,
    read_folder: str,
    dataset: str,
) -> Generator[pl.DataFrame, None, None]:
    """
    Generator for all snapshots, in order
    Args:
        bucket (str): delta dataset bucket
        read_folder (str): delta dataset folder
        dataset (str): CQC organisation type (locations or providers)
    Yields:
        pl.DataFrame: Generator of snapshots
    Raises:
        ValueError: If the dataset is not supported
        DataError: If the base snapshot is not found
    """
    match dataset:
        case "locations":
            primary_key = CQCLClean.location_id
            schema = LocationsSchema.POLARS_LOCATION_SCHEMA
        case "providers":
            primary_key = CqcProviders.provider_id
            schema = ProvidersSchema.POLARS_PROVIDER_SCHEMA
        case "locations-cleaned":
            primary_key = CqcLocations.location_id
            schema = LocationsSchemaCleaned.POLARS_CLEANED_LOCATIONS_SCHEMA
        case _:
            raise ValueError(
                f"Unknown organisation type: {dataset}. Must be either locations, providers or locations-cleaned"
            )
    delta_df = pl.scan_parquet(
        f"s3://{bucket}/{read_folder}",
        schema=schema,
        cast_options=pl.ScanCastOptions(
            missing_struct_fields="insert", extra_struct_fields="ignore"
        ),
        missing_columns="insert",
    ).collect()
    previous_ss = None
    for import_date, delta_data in delta_df.group_by(
        Keys.import_date, maintain_order=True
    ):
        date_pattern = r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})"
        date = match(date_pattern, f"{import_date[0]}")
        if import_date[0] == 20130301:
            previous_ss = delta_data
        else:
            try:
                unchanged = previous_ss.remove(
                    pl.col(primary_key).is_in(delta_data[primary_key])
                )
            except AttributeError:
                raise DataError(
                    "There is no initial snapshot - there should be a base snapshot dated 01/03/2013"
                )
            changed = delta_data.filter(
                pl.col(primary_key).is_in(previous_ss[primary_key])
            )
            new = delta_data.remove(pl.col(primary_key).is_in(previous_ss[primary_key]))
            previous_ss = pl.concat([unchanged, changed, new], how="diagonal")
            previous_ss = previous_ss.with_columns(
                pl.lit(date.group("year")).alias(Keys.year).cast(pl.Int64),
                pl.lit(date.group("month")).alias(Keys.month).cast(pl.Int64),
                pl.lit(date.group("day")).alias(Keys.day).cast(pl.Int64),
                pl.lit(import_date[0]).alias(Keys.import_date).cast(pl.Int64),
            )
            if dataset == "locations-cleaned":
                previous_ss = previous_ss.with_columns(
                    pl.lit(datetime.strptime(str(import_date[0]), "%Y%m%d"))
                    .alias(CQCLClean.cqc_location_import_date)
                    .cast(pl.Date),
                )
        yield previous_ss


if __name__ == "__main__":
    logger.info("Running Build Full Flatten CQC Locations job")

    args = utils.get_args(
        (
            "--dataset",
            "dataset type",
        ),
        (
            "--latest",
            "latest flag",
        ),
        (
            "--input_uri",
            "S3 URI of input data, in this case the flattened CQC locations data",
        ),
        (
            "--output_uri",
            "S3 URI of output data, in this case the full flattened CQC locations data",
        ),
    )

    main(
        dataset=args.dataset,
        latest=args.latest,
        input_uri=args.input_uri,
        output_uri=args.output_uri,
    )

    logger.info("Finished Build Full Flatten CQC Locations job")
