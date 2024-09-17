from typing import List, Dict

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType
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


def add_date_diff_into_df(
    df: DataFrame, new_col_name: str, import_date_col: str
) -> DataFrame:
    max_d = df.agg(F.max(import_date_col)).first()[0]

    loc_df = df.withColumn(
        new_col_name, F.datediff(F.lit(max_d), F.col(import_date_col))
    )
    return loc_df


def add_time_registered_into_df(df: DataFrame) -> DataFrame:
    """
    Adds a new column called time_registered.

    This function adds a new integer column to the given data frame which is the length of time between the imputed registration date and the cqc location import date.

    Args:
        df (DataFrame): A dataframe containing the columns: imputed_registration_date and cqc_location_import_date

    Returns:
        DataFrame: A dataframe with the new column of integers added.
    """
    loc_df = df.withColumn(
        IndCQC.time_registered,
        F.datediff(
            F.col(IndCQC.cqc_location_import_date),
            F.col(IndCQC.imputed_registration_date),
        ),
    )
    return loc_df


def add_import_month_index_into_df(df: DataFrame) -> DataFrame:
    """
    Adds a column containing the number of whole months since the earliest cqc_location_import_date.

    This function adds a column containing the number of whole months since the earliest cqc_location_import_date. The files usually arrive on the first of each month, but the data refers to the previous month. Adjusting by -5 accounts for delays in acquiring the data.

    Args:
        df (DataFrame): A dataframe containing cqc_location_import_date.

    Returns:
        DataFrame: A dataframe with an additional column conatining the number of whole months since the earliest cqc_location_import_date.
    """
    first_date = df.agg(F.min(IndCQC.cqc_location_import_date)).first()[0]
    import_date_adjustment: int = -5
    df = df.withColumn(
        IndCQC.import_month_index,
        (
            F.months_between(
                F.date_add(
                    F.col(IndCQC.cqc_location_import_date), import_date_adjustment
                ),
                F.lit(first_date),
            )
        ).cast(IntegerType()),
    )
    return df
