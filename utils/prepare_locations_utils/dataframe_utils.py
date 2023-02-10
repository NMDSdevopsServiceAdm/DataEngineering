import pyspark.sql.functions as F
from dataclasses import dataclass


@dataclass
class SnapshotConstants:
    snapshot_year_column_name: str = "snapshot_year"
    start_of_year_substring: int = 1
    length_of_year_substring: int = 4
    snapshot_month_column_name: str = "snapshot_month"
    start_of_month_substring: int = 5
    length_of_month_substring: int = 2
    snapshot_day_column_name: str = "snapshot_day"
    start_of_day_substring: int = 7
    length_of_day_substring: int = 2


def add_three_columns_with_snapshot_date_substrings(df):
    df = add_column_with_snaphot_date_substring(
        df,
        SnapshotConstants.snapshot_year_column_name,
        SnapshotConstants.start_of_year_substring,
        SnapshotConstants.length_of_year_substring,
    )
    df = add_column_with_snaphot_date_substring(
        df,
        SnapshotConstants.snapshot_month_column_name,
        SnapshotConstants.start_of_month_substring,
        SnapshotConstants.length_of_month_substring,
    )
    df = add_column_with_snaphot_date_substring(
        df,
        SnapshotConstants.snapshot_day_column_name,
        SnapshotConstants.start_of_day_substring,
        SnapshotConstants.length_of_day_substring,
    )
    return df


def add_column_with_snaphot_date_substring(
    df, column_name, start_of_substring, length_of_substring
):
    df_with_snapshot_date_substring_column = df.withColumn(
        column_name,
        F.col("snapshot_date").substr(start_of_substring, length_of_substring),
    )
    return df_with_snapshot_date_substring_column
