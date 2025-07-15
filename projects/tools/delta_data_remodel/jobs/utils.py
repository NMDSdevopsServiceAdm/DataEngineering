from re import match
from typing import Generator, Optional

import boto3
from polars import read_parquet, concat, col, lit, Int64, DataFrame
from polars.testing import assert_frame_equal

from projects.tools.delta_data_remodel.jobs.raw_providers_schema import (
    raw_providers_schema,
)


def list_bucket_objects(bucket: str, prefix: str) -> list[str]:
    """
    Lists subfiles in an S3 bucket. Returns list of subfiles that are two steps away from the end file
    Args:
        bucket (str): bucket name
        prefix (str): file path (excluding bucket name)

    Returns:
         list[str]: List of subfiles in the S3 bucket

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
        bucket (str): bucket name
        read_folder (str): file path (excluding bucket name)
        timepoint_limit (int): date before which you want the full dataset (eg 20141231) inclusive

    Returns:
        DataFrame: Dataframe including all data for each timepoint,

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
        bucket (str): delta dataset bucket
        read_folder (str): delta dataset folder
        timepoint (int): timepoint to get data for (yyyymmdd)

    Returns:
        Optional[DataFrame]: Snapshot dataframe, if one exists, else None

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
        bucket (str): delta dataset bucket
        read_folder (str): delta dataset folder

    Yields:
        DataFrame: Generator of snapshots

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


def get_diffs(
    base_df: DataFrame,
    snapshot_df: DataFrame,
    snapshot_date: str,
    primary_key: str,
    change_cols: list,
) -> DataFrame:
    """
    Creates delta dataframe for the new snapshot by:
     - stripping out repeated information
     - tagging changed/deleted information with the snapshot date

    Args:
        base_df (DataFrame): Dataframe with the "base" information. IE the dataframe that has the 'truth' for the previous timepoint
        snapshot_df (DataFrame): Dataframe with the information for the new timepoint
        snapshot_date (str): Date of the new snapshot
        primary_key (str): Primary key of the dataframes
        change_cols (list): list of column names in which a change should be marked as a delta

    Returns:
        DataFrame: Delta dataframe of the snapshot

    """
    removed_entries = base_df.join(snapshot_df, how="anti", on=primary_key)
    new_entries = snapshot_df.join(base_df, how="anti", on=primary_key)

    joined_df = snapshot_df.join(
        base_df, on=primary_key, how="left", suffix="_base", maintain_order="right"
    )
    unchanged_conditions = []
    for col_name in change_cols:
        unchanged_conditions.append(
            (col(f"{col_name}").eq_missing(col(f"{col_name}_base")))
        )

    rows_without_changes = joined_df.filter(unchanged_conditions)
    rows_with_changes = joined_df.remove(
        unchanged_conditions
    )  # either new rows or rows where one or more field has changed

    changed_entries = rows_with_changes.select(base_df.columns)
    unchanged_entries = rows_without_changes.select(base_df.columns)

    print(f"Removed entries: {removed_entries.shape[0]}")
    print(f"New entries: {new_entries.shape[0]}")
    print(f"Unchanged entries: {unchanged_entries.shape[0]}")
    print(f"Changed entries: {changed_entries.shape[0]}")
    print(f"Total = {changed_entries.shape[0] + unchanged_entries.shape[0]}")

    assert (
        changed_entries.shape[0]
        + unchanged_entries.shape[0]
        - new_entries.shape[0]
        + removed_entries.shape[0]
        == base_df.shape[0]
    )

    assert (
        snapshot_df.shape[0] + removed_entries.shape[0] - new_entries.shape[0]
        == base_df.shape[0]
    )

    removed_entries = removed_entries.with_columns(
        lit(snapshot_date).alias("import_date").cast(Int64),
        lit(snapshot_date).alias("deregistrationDate"),
    )

    new_base_df = concat([changed_entries, unchanged_entries])
    assert_frame_equal(snapshot_df, new_base_df, check_row_order=False)

    changed_entries = concat([changed_entries, removed_entries])

    assert changed_entries["import_date"].n_unique() == 1

    return changed_entries
