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


def add_array_column_count_to_data(
    df: DataFrame, new_col_name: str, col_to_check: str
) -> DataFrame:
    """
    Add a new column with the count of items in an array column.

    Args:
        df(DataFrame): A dataframe with an array column.
        new_col_name(str): A name for the new column with the count of items.
        col_to_check(str): The name of the array column.

    Returns:
        DataFrame: A dataframe with an extra column with the count of items in hte specified array.
    """
    return df.withColumn(new_col_name, F.size(F.col(col_to_check)))


def add_time_registered_into_df(df: DataFrame) -> DataFrame:
    """
    Adds a new column called time_registered.

    This function adds a new integer column to the given data frame which represents the length of time between the imputed
    registration date and the cqc location import date, split into 6 month time bands (rounded down).

    Args:
        df (DataFrame): A dataframe containing the columns: imputed_registration_date and cqc_location_import_date

    Returns:
        DataFrame: A dataframe with the new column of integers added.
    """
    six_months = 6
    loc_df = df.withColumn(
        IndCQC.time_registered,
        F.floor(
            F.months_between(
                F.col(IndCQC.cqc_location_import_date),
                F.col(IndCQC.imputed_registration_date),
            )
            / six_months
        ),
    )
    return loc_df
