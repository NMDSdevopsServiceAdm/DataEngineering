from typing import List, Dict

from pyspark.sql import DataFrame, functions as F
from pyspark.ml.feature import VectorAssembler

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


def vectorise_dataframe(df: DataFrame, list_for_vectorisation: List[str]) -> DataFrame:
    loc_df = VectorAssembler(
        inputCols=list_for_vectorisation,
        outputCol=IndCQC.features,
        handleInvalid="skip",
    ).transform(df)
    return loc_df


def column_expansion_with_dict(
    df: DataFrame, col_name: str, lookup_dict: Dict[str, str]
) -> DataFrame:
    for key in lookup_dict.keys():
        df = df.withColumn(
            f"{key}",
            F.array_contains(df[f"{col_name}"], lookup_dict[key]).cast("integer"),
        )
    return df


def convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
    df: DataFrame,
    categorical_col_name: str,
    lookup_dict: Dict[str, str],
) -> DataFrame:
    for key in lookup_dict.keys():
        df = df.withColumn(
            key, (F.col(categorical_col_name) == lookup_dict[key]).cast("integer")
        )
    return df


def add_array_column_count(
    df: DataFrame, new_col_name: str, col_to_check: str
) -> DataFrame:
    """
    Add a new column with the count of items in an array column.

    This function adds a new column to the given data frame which contains the count of items in the specified array column.
    If the array column is empty, the count will return 0 (by default, size returns -1 if the array is null).

    Args:
        df(DataFrame): A dataframe with an array column.
        new_col_name(str): A name for the new column with the count of items.
        col_to_check(str): The name of the array column.

    Returns:
        DataFrame: A dataframe with an extra column with the count of items in hte specified array.
    """
    return df.withColumn(
        new_col_name, F.greatest(F.size(F.col(col_to_check)), F.lit(0))
    )


def cap_integer_at_max_value(
    df: DataFrame, col_name: str, max_value: int, new_col_name: str
) -> DataFrame:
    """
    Caps the values in a specified column at a given maximum value and stores the result in a new column.
    Null values remain as null.

    Args:
        df (DataFrame): The input DataFrame.
        col_name (str): The name of the column to be capped.
        max_value (int): The maximum value allowed for the column.
        new_col_name (str): The name of the new column to store the capped values.

    Returns:
        DataFrame: A new DataFrame with the capped values stored in the new column, preserving null values.
    """
    df = df.withColumn(
        new_col_name,
        F.when(
            F.col(col_name).isNotNull(), F.least(F.col(col_name), F.lit(max_value))
        ).otherwise(None),
    )
    return df
