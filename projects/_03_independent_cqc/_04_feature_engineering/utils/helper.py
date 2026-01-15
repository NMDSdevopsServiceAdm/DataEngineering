from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


# converted to polars -> projects._03_independent_cqc._04_feature_engineering.fargate.utils.feature_utils.py
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
        DataFrame: A dataframe with an extra column with the count of items in the specified array.
    """
    return df.withColumn(
        new_col_name, F.greatest(F.size(F.col(col_to_check)), F.lit(0))
    )


# converted to polars -> projects._03_independent_cqc._04_feature_engineering.fargate.utils.feature_utils.py
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


# converted to polars -> projects._03_independent_cqc._04_feature_engineering.fargate.utils.feature_utils.py
def add_date_index_column(df: DataFrame) -> DataFrame:
    """
    Creates an index column in the DataFrame based on the cqc_location_import_date column, partitioned by care_home.

    dense_rank has been used as it doesn't leave gaps in ranking sequence when there are ties.
    For example, if three rows have the same date then they would all receive the same index value.
    The first date after this would receive the next index value.
    The difference with rank is that it leaves gaps in the sequence, so the first three dates would be indexed at 1 and the next date would be 4.

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


# converted to polars -> projects._03_independent_cqc._04_feature_engineering.fargate.utils.feature_utils.py
def group_rural_urban_sparse_categories(df: DataFrame) -> DataFrame:
    """
    Copies the values in the rural urban indicator column into a new column and replaces all categories which contains the word "sparse" with "Sparse setting".

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with the new rural urban indicator column with recoded sparse categories.
    """
    sparse_identifier: str = "sparse"
    sparse_replacement_string: str = "Sparse setting"

    df = df.withColumn(
        IndCQC.current_rural_urban_indicator_2011_for_non_res_model,
        F.when(
            F.lower(F.col(IndCQC.current_rural_urban_indicator_2011)).contains(
                sparse_identifier
            ),
            sparse_replacement_string,
        ).otherwise(F.col(IndCQC.current_rural_urban_indicator_2011)),
    )

    return df


def filter_without_dormancy_features_to_pre_2025(df: DataFrame) -> DataFrame:
    """
    Filters the DataFrame to include only rows with a cqc_location_import_date on or before 01/01/2025.

    The 'with_dormancy' model started in 2022 and is an improvement on the 'without_dormancy' model.
    In order to ensure a smooth transition between the two models, we predict both models alongside each other for a 3 year period.
    The features dataframe will be filtered in line with the point at which the model was last retrained.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: Filtered DataFrame.
    """
    return df.filter(F.col(IndCQC.cqc_location_import_date) <= date(2025, 1, 1))


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
