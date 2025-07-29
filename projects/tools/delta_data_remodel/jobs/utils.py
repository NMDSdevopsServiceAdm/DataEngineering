import boto3
import polars as pl
import polars.testing as pl_testing

from lambdas.utils import snapshots


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
    bucket: str, read_folder: str, organisation_type: str, timepoint_limit: int = None
) -> pl.DataFrame:
    """
    Builds full dataset from delta dataset
    Args:
        bucket (str): bucket name
        read_folder (str): file path (excluding bucket name)
        organisation_type (str): CQC organisation type (locations or providers)
        timepoint_limit (int): date before which you want the full dataset (e.g. 20141231) inclusive

    Returns:
        pl.DataFrame: pl.DataFrame including all data for each timepoint,

    """
    ss = []
    if not timepoint_limit:
        timepoint_limit = 300000000

    for t in snapshots.get_snapshots(
        bucket, read_folder, organisation_type=organisation_type
    ):
        if t.item(1, "import_date") > timepoint_limit:
            break
        ss.append(t)

    full_df = pl.concat(ss)
    return full_df


def get_diffs(
    base_df: pl.DataFrame,
    snapshot_df: pl.DataFrame,
    snapshot_date: str,
    primary_key: str,
    change_cols: list,
) -> pl.DataFrame:
    """
    Creates delta pl.DataFrame for the new snapshot by:
     - stripping out repeated information
     - tagging changed/deleted information with the snapshot date

    Args:
        base_df (pl.DataFrame): pl.DataFrame with the "base" information. IE the pl.DataFrame that has the 'truth' for the previous timepoint
        snapshot_df (pl.DataFrame): pl.DataFrame with the information for the new timepoint
        snapshot_date (str): Date of the new snapshot
        primary_key (str): Primary key of the pl.DataFrames
        change_cols (list): list of column names in which a change should be marked as a delta

    Returns:
        pl.DataFrame: Delta pl.DataFrame of the snapshot

    """
    removed_entries = base_df.join(snapshot_df, how="anti", on=primary_key)
    new_entries = snapshot_df.join(base_df, how="anti", on=primary_key)

    joined_df = snapshot_df.join(
        base_df, on=primary_key, how="left", suffix="_base", maintain_order="right"
    )
    unchanged_conditions = []
    for col_name in change_cols:
        unchanged_conditions.append(
            (pl.col(f"{col_name}").eq_missing(pl.col(f"{col_name}_base")))
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
        pl.lit(snapshot_date).alias("import_date").cast(pl.Int64),
        pl.lit(snapshot_date).alias("deregistrationDate"),
    )

    new_base_df = pl.concat([changed_entries, unchanged_entries])
    pl_testing.assert_frame_equal(snapshot_df, new_base_df, check_row_order=False)

    changed_entries = pl.concat([changed_entries, removed_entries])

    assert changed_entries["import_date"].n_unique() == 1

    return changed_entries
