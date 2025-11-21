from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def null_values_after_consecutive_repetition(
    df: DataFrame, col_with_repeats: str, rep_limit: int
) -> DataFrame:
    """
    Copies values into a new column and then nulls the values after a given number of repetitions.
    The new column is suffixed with "_repeats_nulled"

    Args:
        df(DataFrame): A dataframe with consecutive import dates.
        col_with_repeats(str): The column with repeated values.
        rep_limit(int): The limit to which repeated values are kept.

    Returns:
        DataFrame: The input with DataFrame with an additional column.
    """

    return df
