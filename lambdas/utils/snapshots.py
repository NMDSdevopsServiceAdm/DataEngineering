from re import match
from typing import Generator, Optional
import logging

import polars as pl
import boto3

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CqcProviders,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CqcLocations,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")


def _get_prefixes(bucket: str, prefix: str) -> list[str]:
    """Recursively get all folder prefixes under given S3 prefix."""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    prefixes = response.get("CommonPrefixes", [])

    if not prefixes:
        return [prefix]

    all_folders = []
    for p in prefixes:
        sub_prefixes = _get_prefixes(bucket, p["Prefix"])
        all_folders.extend(sub_prefixes)

    return all_folders


def build_snapshot_table_from_delta(
    bucket: str,
    read_folder: str,
    organisation_type: str,
    timepoint: int,
) -> Optional[pl.DataFrame]:
    """
    Gets full snapshot of data at a given timepoint.
    """
    for snapshot in get_snapshots(bucket, read_folder, organisation_type):
        if snapshot.item(1, Keys.import_date) == timepoint:
            return snapshot
    else:
        return None


def get_snapshots(
    bucket: str,
    read_folder: str,
    organisation_type: str,
    schema: Optional[pl.Schema] = None,
) -> Generator[pl.DataFrame, None, None]:
    """
    Generator for all snapshots, safely reading mismatched schema Parquet files.
    """

    if organisation_type == "locations":
        primary_key = CqcLocations.location_id
    elif organisation_type == "providers":
        primary_key = CqcProviders.provider_id
    else:
        raise ValueError(
            f"Unknown organisation type: {organisation_type}. Must be either locations or providers"
        )

    # Gather all partition folders recursively
    all_folders = _get_prefixes(bucket, read_folder)

    # Lazy scan each folder individually
    all_scans = [
        pl.scan_parquet(
            f"s3://{bucket}/{folder}*.parquet",
            schema=schema,
            glob=True,
            missing_columns="insert",
            cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
        )
        for folder in all_folders
    ]

    # Concatenate all scans while allowing schema mismatches
    delta_df = pl.concat(all_scans, how="diagonal").collect()

    logger.info(f"The schema for delta_df is: {delta_df.collect_schema().names()}")

    previous_ss = None

    for import_date, delta_data in delta_df.group_by(
        Keys.import_date, maintain_order=True
    ):
        date_pattern = r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})"
        date = match(date_pattern, f"{import_date[0]}")

        if import_date[0] == 20130301:
            previous_ss = delta_data
            logger.info(
                f"The schema for previous is: {delta_df.collect_schema().names()}"
            )

        else:
            unchanged = previous_ss.remove(
                pl.col(primary_key).is_in(delta_data[primary_key])
            )
            changed = delta_data.filter(
                pl.col(primary_key).is_in(previous_ss[primary_key])
            )
            new = delta_data.remove(pl.col(primary_key).is_in(previous_ss[primary_key]))

            previous_ss = pl.concat(
                [unchanged, changed, new], how="diagonal"
            ).with_columns(
                pl.lit(date.group("year")).alias(Keys.year).cast(pl.Int64),
                pl.lit(date.group("month")).alias(Keys.month).cast(pl.Int64),
                pl.lit(date.group("day")).alias(Keys.day).cast(pl.Int64),
                pl.lit(import_date[0]).alias(Keys.import_date).cast(pl.Int64),
            )

        yield previous_ss
