from re import match
from typing import Generator, Optional

import boto3
from polars import read_parquet, concat, col, lit, Int64, DataFrame

from raw_providers_schema import raw_providers_schema


def list_bucket_objects(bucket: str, prefix: str) -> list[str]:
    """
    Lists subfiles in an S3 bucket. Returns list of subfiles that are two steps away from the end file
    Args:
        bucket: bucket name
        prefix: file path (excluding bucket name)

    Returns: List of subfiles in the S3 bucket

    """
    s3 = boto3.client("s3")
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return list(set([o["Key"].rsplit("/", 2)[0] for o in objects["Contents"]]))


def build_full_table_from_delta(
    bucket: str, read_folder: str, timepoint_limit: int = None
) -> DataFrame:
    """
    Builds full dataset from delta dataset
    Args:
        bucket: bucket name
        read_folder: file path (excluding bucket name)
        timepoint_limit: date before which you want the full dataset (eg 20141231) inclusive

    Returns: Dataframe including all data for each timepoint,

    """
    ss = []
    if not timepoint_limit:
        timepoint_limit = 300000000

    for t in snapshots(bucket, read_folder):
        if t.item(1, "import_date") > timepoint_limit:
            break
        ss.append(t)

    full_df = concat(ss)
    return full_df


def build_snapshot_table_from_delta(
    bucket: str, read_folder: str, timepoint: int
) -> Optional[DataFrame]:
    """
    Gets full snapshot of data at a given timepoint
    Args:
        bucket: delta dataset bucket
        read_folder: delta dataset folder
        timepoint: timepoint to get data for (yyyymmdd)

    Returns: Snapshot dataframe, if one exists, else None

    """
    for snapshot in snapshots(bucket, read_folder):
        if snapshot.item(1, "import_date") == timepoint:
            return snapshot
    else:
        return None


def snapshots(bucket: str, read_folder: str) -> Generator[DataFrame, None, None]:
    """
    Generator for all snapshots, in order
    Args:
        bucket: delta dataset bucket
        read_folder: delta dataset folder

    Returns: Generator of snapshots

    """
    delta_df = read_parquet(
        f"s3://{bucket}/{read_folder}",
        schema=raw_providers_schema,
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
                col("providerId").is_in(delta_data["providerId"])
            )
            changed = delta_data.filter(
                col("providerId").is_in(previous_ss["providerId"])
            ).remove(col("deregistrationDate").ne(""))
            new = delta_data.remove(col("providerId").is_in(previous_ss["providerId"]))

            previous_ss = concat([unchanged, changed, new])
            previous_ss = previous_ss.with_columns(
                lit(date.group("year")).alias("year").cast(Int64),
                lit(date.group("month")).alias("month").cast(Int64),
                lit(date.group("day")).alias("day").cast(Int64),
                lit(import_date[0]).alias("import_date").cast(Int64),
            )
        yield previous_ss
