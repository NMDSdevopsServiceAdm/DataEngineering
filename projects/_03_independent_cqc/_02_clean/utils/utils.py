from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


# converted to polars -> projects._03_independent_cqc._02_clean.fargate.utils.utils.create_column_with_repeated_values_removed
def create_column_with_repeated_values_removed(
    df: DataFrame,
    column_to_clean: str,
    new_column_name: Optional[str] = None,
    column_to_partition_by: str = IndCQC.location_id,
) -> DataFrame:
    """
    Some data we have (such as ASCWDS) repeats data until it is changed. This function creates a new column which converts repeated
    values to nulls, so we only see newly submitted values once. This also happens as a result of joining the same datafile multiple
    times as part of the align dates field.

    For each partition, this function iterates over the dataframe in date order and compares the current column value to the
    previously submitted value. If the value differs from the previously submitted value then enter that value into the new column.
    Otherwise null the value in the new column as it is a previously submitted value which has been repeated.

    Args:
        df (DataFrame): The dataframe to use
        column_to_clean (str): The name of the column to convert
        new_column_name (Optional [str]): If not provided, "_deduplicated" will be appended onto the original column name
        column_to_partition_by (str): A column to partition by when deduplicating. Defaults to 'locationid'.

    Returns:
        DataFrame: A DataFrame with an addional column with repeated values changed to nulls.
    """
    PREVIOUS_VALUE: str = "previous_value"

    if new_column_name is None:
        new_column_name = column_to_clean + "_deduplicated"

    w = Window.partitionBy(column_to_partition_by).orderBy(
        IndCQC.cqc_location_import_date
    )

    df_with_previously_submitted_value = df.withColumn(
        PREVIOUS_VALUE, F.lag(column_to_clean).over(w)
    )

    df_without_repeated_values = df_with_previously_submitted_value.withColumn(
        new_column_name,
        F.when(
            (F.col(PREVIOUS_VALUE).isNull())
            | (F.col(column_to_clean) != F.col(PREVIOUS_VALUE)),
            F.col(column_to_clean),
        ).otherwise(None),
    )

    df_without_repeated_values = df_without_repeated_values.drop(PREVIOUS_VALUE)

    return df_without_repeated_values
