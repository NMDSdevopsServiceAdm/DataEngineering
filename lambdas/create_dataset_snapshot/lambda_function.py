import logging

from re import match
from typing import Optional, Generator
from datetime import datetime

import polars as pl
import s3fs

# from raw_providers_schema import raw_providers_schema


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def build_snapshot_table_from_delta(
    bucket: str, read_folder: str, timepoint: int
) -> Optional[pl.DataFrame]:
    """
    Gets full snapshot of data at a given timepoint
    Args:
        bucket (str): delta dataset bucket
        read_folder (str): delta dataset folder
        timepoint (int): timepoint to get data for (yyyymmdd)

    Returns:
        Optional[pl.DataFrame]: Snapshot pl.DataFrame, if one exists, else None

    """
    for snapshot in snapshots(bucket, read_folder):
        if snapshot.item(1, "import_date") == timepoint:
            return snapshot
        else:
            logger.debug(snapshot.item(1, "import_date"))
    else:
        return None


def snapshots(bucket: str, read_folder: str) -> Generator[pl.DataFrame, None, None]:
    """
    Generator for all snapshots, in order
    Args:
        bucket (str): delta dataset bucket
        read_folder (str): delta dataset folder

    Yields:
        pl.DataFrame: Generator of snapshots

    """
    delta_df = pl.read_parquet(
        f"s3://{bucket}/{read_folder}",
        # schema=raw_providers_schema,
    )

    previous_ss = None

    for import_date, delta_data in delta_df.group_by(
        "import_date", maintain_order=True
    ):
        date_pattern = r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})"
        date = match(date_pattern, f"{import_date[0]}")

        if import_date[0] == 20130301:
            previous_ss = delta_data
        else:
            unchanged = previous_ss.remove(
                pl.col("providerId").is_in(delta_data["providerId"])
            )
            changed = delta_data.filter(
                pl.col("providerId").is_in(previous_ss["providerId"])
            ).remove(pl.col("deregistrationDate").ne(""))
            new = delta_data.remove(
                pl.col("providerId").is_in(previous_ss["providerId"])
            )

            previous_ss = pl.concat([unchanged, changed, new])
            previous_ss = previous_ss.with_columns(
                pl.lit(date.group("year")).alias("year").cast(pl.Int64),
                pl.lit(date.group("month")).alias("month").cast(pl.Int64),
                pl.lit(date.group("day")).alias("day").cast(pl.Int64),
                pl.lit(import_date[0]).alias("import_date").cast(pl.Int64),
            )
        yield previous_ss


def main(input_uri, output_uri, snapshot_date):
    input_parse = match(
        "s3://(?P<bucket>[\w\-=.]+)/(?P<read_folder>[\w/-=.]+)", input_uri
    )

    date_int = int(
        datetime.strptime(snapshot_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y%m%d")
    )
    logger.debug(
        f"bucket={input_parse.group('bucket')}, read_folder={input_parse.group('read_folder')}"
    )
    logger.debug(f"{date_int=}")

    snapshot_df = build_snapshot_table_from_delta(
        bucket=input_parse.group("bucket"),
        read_folder=input_parse.group("read_folder"),
        timepoint=date_int,
    )

    fs = s3fs.S3FileSystem()
    with fs.open(output_uri, mode="wb") as destination:
        snapshot_df.write_parquet(destination, compression="snappy")


def lambda_handler(event, context):
    main(event["input_uri"], event["output_uri"], event["snapshot_date"])
    logger.info(
        f"Finished processing snapshot {event['snapshot_date']}. The files can be found at {event['output_uri']}"
    )
