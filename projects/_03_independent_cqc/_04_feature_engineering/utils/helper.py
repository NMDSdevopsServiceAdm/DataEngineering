from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# converted to polars -> projects._03_independent_cqc._04_feature_engineering.fargate.utils.feature_utils.py
def add_squared_column(df: DataFrame, col_to_square: str) -> DataFrame:
    """
    Squares the values in a specified column and returns a new DataFrame with the squared values.

    Args:
        df (DataFrame): A dataframe with a column to be squared.
        col_to_square (str): The name of the column to be squared.

    Returns:
        DataFrame: A dataframe with an extra column with the squared values.
    """
    return df.withColumn(f"{col_to_square}_squared", F.pow(F.col(col_to_square), 2))
