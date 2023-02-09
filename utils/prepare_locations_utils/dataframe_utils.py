import pyspark.sql.functions as F

START_OF_YEAR_SUBSTRING = 1
LENGTH_OF_YEAR_SUBSTRING = 4
START_OF_MONTH_SUBSTRING = 5
LENGTH_OF_MONTH_SUBSTRING = 2
START_OF_DAY_SUBSTRING = 7
LENGTH_OF_DAY_SUBSTRING = 2


def add_column_with_snaphot_date_substring(df, column_name, start_of_substring, length_of_substring):
    df_with_snapshot_date_substring_column = df.withColumn(
        column_name,
        F.col("snapshot_date").substr(start_of_substring, length_of_substring),
    )
    return df_with_snapshot_date_substring_column
