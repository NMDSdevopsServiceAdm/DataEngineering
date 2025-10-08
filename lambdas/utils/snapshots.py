from re import match
from typing import Generator, Optional
from datetime import datetime
import logging
import polars as pl

from schemas import (
    cqc_locations_schema_polars as LocationsSchema,
    cqc_provider_schema_polars as ProvidersSchema,
    cqc_locations_cleaned_schema_polars as LocationsSchemaCleaned,
)
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CqcProviders,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CqcLocations,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CqcLocationsCleaned,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class DataError(Exception):
    pass


def build_snapshot_table_from_delta(
    bucket: str,
    read_folder: str,
    dataset: str,
    timepoint: int,
) -> Optional[pl.DataFrame]:
    """
    Gets full snapshot of data at a given timepoint
    Args:
        bucket (str): delta dataset bucket
        read_folder (str): delta dataset folder
        dataset (str): CQC organisation type (locations or providers)
        timepoint (int): timepoint to get data for (yyyymmdd)

    Returns:
        Optional[pl.DataFrame]: Snapshot pl.DataFrame, if one exists, else None

    Raises:
        DataError: if no snapshot is found for the specified date

    """
    for snapshot in get_snapshots(bucket, read_folder, dataset):
        if snapshot.item(1, Keys.import_date) == timepoint:
            return snapshot
        latest = snapshot
    else:
        logger.info(f"No snapshot found for {timepoint}, returning most recent")
        return latest


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
            primary_key = CqcLocations.location_id
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
                    .alias(CqcLocationsCleaned.cqc_location_import_date)
                    .cast(pl.Date),
                )
        yield previous_ss
