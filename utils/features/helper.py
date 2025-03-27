from typing import Dict, List, Tuple

from datetime import date
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def vectorise_dataframe(df: DataFrame, list_for_vectorisation: List[str]) -> DataFrame:
    """
    Combines specified columns into a single feature vector for the modelling process.

    This function uses `VectorAssembler` to merge multiple input columns into a single vector column.
    Invalid values are skipped to prevent transformation errors.

    Args:
        df (DataFrame): Input DataFrame containing columns to be vectorised.
        list_for_vectorisation (List[str]): List of column names to be combined into the feature vector.

    Returns:
        DataFrame: A DataFrame with an additional 'features' column.
    """
    loc_df = VectorAssembler(
        inputCols=list_for_vectorisation,
        outputCol=IndCQC.features,
        handleInvalid="skip",
    ).transform(df)
    return loc_df


def expand_encode_and_extract_features(
    df: DataFrame, col_name: str, lookup_dict: Dict[str, str], is_array_col: bool
) -> Tuple[DataFrame, List[str]]:
    """
    Expands a categorical or array column and converts values into binary variables,
    returning the modified DataFrame and a list of feature names.

    This function iterates through a lookup dictionary and creates new binary columns
    where each column corresponds to a key in the dictionary. If the column is an array,
    it checks for array membership using `array_contains`. Otherwise, it performs an
    equality comparison.

    Args:
        df (DataFrame): Input DataFrame.
        col_name (str): Name of the column to be expanded and encoded.
        lookup_dict (Dict[str, str]): Dictionary where keys are new column names and
            values are the lookup values to compare against.
        is_array_col (bool): If True, treats the column as an array and checks for
            array membership. If False, performs equality comparison.

    Returns:
        Tuple[DataFrame, List[str]]:
            - DataFrame with new binary columns added.
            - List of feature names (keys) from the lookup dictionary.
    """
    for key, value in lookup_dict.items():
        if is_array_col:
            df = df.withColumn(
                key, F.array_contains(F.col(col_name), value).cast(IntegerType())
            )
        else:
            df = df.withColumn(key, (F.col(col_name) == value).cast(IntegerType()))

    key_list = list(lookup_dict.keys())

    return df, key_list


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


def add_date_index_column(df: DataFrame) -> DataFrame:
    """
    Creates an index column in the DataFrame based on the cqc_location_import_date column.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with an added index column.
    """
    windowSpec = Window.partitionBy(IndCQC.care_home).orderBy(
        IndCQC.cqc_location_import_date
    )

    df_with_index = df.withColumn(
        IndCQC.cqc_location_import_date_indexed, F.dense_rank().over(windowSpec)
    )

    return df_with_index


# TODO - Add tests for this function
def group_rural_urban_sparse_categories(df: DataFrame) -> DataFrame:
    """
    Copies the values in the rural urban indicator column into a new column and replaces all categories which contains the word "sparse" with "Sparse setting".

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with the new rural urban indicator column with recoded sparse categories.
    """
    sparse_identifier: str = "sparse"
    sparse_replacement_cateogry_name: str = "Sparse setting"

    df = df.withColumn(
        IndCQC.current_rural_urban_indicator_2011_for_non_res_model,
        F.when(
            F.col(IndCQC.current_rural_urban_indicator_2011).contains(
                sparse_identifier
            ),
            sparse_replacement_cateogry_name,
        ).otherwise(F.col(IndCQC.current_rural_urban_indicator_2011)),
    )

    return df


# TODO - Add tests for this function
def add_log_column(df: DataFrame, input_col: str, output_col: str) -> DataFrame:
    """
    Adds a new column to the DataFrame which is the logarithm of the specified input column.

    Args:
        df (DataFrame): Input DataFrame.
        input_col (str): Name of the column to take the logarithm of.
        output_col (str): Name of the new column to be added.

    Returns:
        DataFrame: DataFrame with the new column added.
    """
    return df.withColumn(output_col, F.log(F.col(input_col)))


# TODO - Add tests for this function
def filter_without_dormancy_features_to_pre_2025(
    without_dormancy_df: DataFrame,
) -> DataFrame:
    """
    Filters the DataFrame to include only rows with a cqc_location_import_date on or before 01/01/2025.

    The 'with_dormancy' model started in 2022 and is an improvement on the 'without_dormancy' model.
    In other to ensure a smooth transition between the two models, we predict both models for a 3 year period.
    We are filtering the features dataframe to be in line with the point at which the model was last retrained.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: Filtered DataFrame.
    """
    return without_dormancy_df.filter(
        F.col(IndCQC.cqc_location_import_date) <= date(2025, 1, 1)
    )
