import pyspark.sql.functions as F

SNAPSHOT_YEAR_COLUMN_NAME = "snapshot_year"
START_OF_YEAR_SUBSTRING = 1
LENGTH_OF_YEAR_SUBSTRING = 4
SNAPSHOT_MONTH_COLUMN_NAME = "snapshot_month"
START_OF_MONTH_SUBSTRING = 5
LENGTH_OF_MONTH_SUBSTRING = 2
SNAPSHOT_DAY_COLUMN_NAME = "snapshot_day"
START_OF_DAY_SUBSTRING = 7
LENGTH_OF_DAY_SUBSTRING = 2


def add_column_with_snaphot_date_substring(df, column_name, start_of_substring, length_of_substring):
    df_with_snapshot_date_substring_column = df.withColumn(
        column_name,
        F.col("snapshot_date").substr(start_of_substring, length_of_substring),
    )
    return df_with_snapshot_date_substring_column
