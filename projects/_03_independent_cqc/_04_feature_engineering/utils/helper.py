from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


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
