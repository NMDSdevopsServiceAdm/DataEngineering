from polars import DataFrame, col, concat, lit, Int64
from polars.testing import assert_frame_equal, assert_series_equal


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
        base_df: Dataframe with the "base" information. IE the dataframe that has the 'truth' for the previous timepoint
        snapshot_df: Dataframe with the information for the new timepoint
        snapshot_date: Date of the new snapshot
        primary_key: Primary key of the dataframes
        change_cols: list of column names in which a change should be marked as a delta

    Returns: Delta dataframe of the snapshot

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
