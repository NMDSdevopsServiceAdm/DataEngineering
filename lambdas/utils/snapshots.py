from re import match
from typing import Generator, Optional

import polars as pl

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CqcProviders,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CqcLocations,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


def build_snapshot_table_from_delta(
    bucket: str,
    read_folder: str,
    organisation_type: str,
    timepoint: int,
) -> Optional[pl.DataFrame]:
    """
    Gets full snapshot of data at a given timepoint
    Args:
        bucket (str): delta dataset bucket
        read_folder (str): delta dataset folder
        organisation_type (str): CQC organisation type (locations or providers)
        timepoint (int): timepoint to get data for (yyyymmdd)

    Returns:
        Optional[pl.DataFrame]: Snapshot pl.DataFrame, if one exists, else None

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
    Generator for all snapshots, in order
    Args:
        bucket (str): delta dataset bucket
        read_folder (str): delta dataset folder
        organisation_type (str): CQC organisation type (locations or providers)
        schema(Optional[pl.Schema]): Optional schema of the dataset

    Yields:
        pl.DataFrame: Generator of snapshots

    Raises:
        ValueError: If the organisation_type is not supported

    """
    delta_df = pl.read_parquet(
        f"s3://{bucket}/{read_folder}",
        schema=schema,
    )

    if organisation_type == "locations":
        primary_key = CqcLocations.location_id
    elif organisation_type == "providers":
        primary_key = CqcProviders.provider_id
    else:
        raise ValueError(
            f"Unknown organisation type: {organisation_type}. Must be either locations or providers"
        )

    previous_ss = None

    for import_date, delta_data in delta_df.group_by(
        Keys.import_date, maintain_order=True
    ):
        date_pattern = r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})"
        date = match(date_pattern, f"{import_date[0]}")

        if import_date[0] == 20130301:
            previous_ss = delta_data
        else:
            unchanged = previous_ss.remove(
                pl.col(primary_key).is_in(delta_data[primary_key])
            )
            changed = delta_data.filter(
                pl.col(primary_key).is_in(previous_ss[primary_key])
            )
            new = delta_data.remove(pl.col(primary_key).is_in(previous_ss[primary_key]))

            previous_ss = pl.concat([unchanged, changed, new])
            previous_ss = previous_ss.with_columns(
                pl.lit(date.group("year")).alias(Keys.year).cast(pl.Int64),
                pl.lit(date.group("month")).alias(Keys.month).cast(pl.Int64),
                pl.lit(date.group("day")).alias(Keys.day).cast(pl.Int64),
                pl.lit(import_date[0]).alias(Keys.import_date).cast(pl.Int64),
            )
        yield previous_ss
