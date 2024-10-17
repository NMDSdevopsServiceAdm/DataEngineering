from pyspark.sql import DataFrame, functions as F, Window
from typing import Tuple

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import CareHome
from utils.estimate_filled_posts.models.extrapolation_new import model_extrapolation
from utils.estimate_filled_posts.models.interpolation_new import model_interpolation


def model_imputation_with_extrapolation_and_interpolation(
    df: DataFrame,
    column_with_null_values: str,
    model_column_name: str,
    imputed_column_name: str,
    care_home: bool,
) -> DataFrame:
    """
    Create a new column of imputed values based on known values and null values being extrapolated and interpolated.

    This function first splits the dataset into two, one which is relevant for imputation (based on the care_home status of the
    location and only for locations who have at least one non-null value) and another which includes all other rows not relevant
    to imputation.
    The imputation model is carried out in two steps, extrapolation and interpolation, which both populate null values based on
    the rate of change of values in '<model_column_name>'. Values before the first known submission in 'column_with_null_values'
    and after the last known submission are extrapolated based on the rate of change of the '<model_column_name>'. Values in
    between the non-null values are interpolated using the rate of change of the '<model_column_name>' but the trend is adapted
    so that the end point matches the next non-null value. A new column is added with the name provided in '<imputed_column_name>'.

    Args:
        df (DataFrame): The input DataFrame containing the columns to be extrapolated and interpolated.
        column_with_null_values (str): The name of the column containing null values to be extrapolated and interpolated.
        model_column_name (str): The name of the column containing the model values used for extrapolation and interpolation.
        imputed_column_name (str): The name of the new imputated column.
        care_home (bool): True if imputation is for care homes, False if it is for non residential.

    Returns:
        DataFrame: The DataFrame with the added column for imputed values.
    """
    df = identify_locations_with_a_non_null_submission(df, column_with_null_values)

    imputed_df, non_imputed_df = split_dataset_for_imputation(df, care_home)

    imputed_df = model_extrapolation(
        imputed_df,
        column_with_null_values,
        model_column_name,
    )
    imputed_df = model_interpolation(
        imputed_df,
        column_with_null_values,
    )
    imputed_df = model_imputation(
        imputed_df, column_with_null_values, imputed_column_name
    )

    combined_df = imputed_df.unionByName(non_imputed_df, allowMissingColumns=True)

    combined_df = combined_df.drop(
        IndCqc.extrapolation_backwards,
        IndCqc.extrapolation_forwards,
        IndCqc.extrapolation_residual,
        IndCqc.proportion_of_time_between_submissions,
        IndCqc.extrapolation_model,
        IndCqc.interpolation_model,
        IndCqc.has_non_null_value,
    )

    return combined_df


def split_dataset_for_imputation(
    df: DataFrame, care_home: bool
) -> Tuple[DataFrame, DataFrame]:
    """
    Splits the dataset into two based on whether or not the rows meet the criteria for imputation.

    Splits the dataset into two based on the presence of non-null values in a specified column
    and whether the care_home column matches the provided argument.

    Args:
        df (DataFrame): The input DataFrame.
        care_home (bool): True if imputation is for care homes, False if it is for non residential.

    Returns:
        Tuple[DataFrame, DataFrame]: A tuple containing two DataFrames:
            - imputation_df: DataFrame with rows meeting the criteria for imputation.
            - non_imputation_df: DataFrame with rows not meeting the criteria.
    """
    if care_home == True:
        care_home_filter_value: str = CareHome.care_home
    else:
        care_home_filter_value: str = CareHome.not_care_home

    imputation_df = df.where(
        (F.col(IndCqc.care_home) == care_home_filter_value)
        & (F.col(IndCqc.has_non_null_value) == True)
    )
    non_imputation_df = df.where(
        (F.col(IndCqc.care_home) != care_home_filter_value)
        | (F.col(IndCqc.has_non_null_value) == False)
    )

    return imputation_df, non_imputation_df


def identify_locations_with_a_non_null_submission(
    df: DataFrame, column_with_null_values: str
) -> DataFrame:
    """
    Identifies locations with at least one non-null submission in the specified column.

    This function adds a new column to the DataFrame indicating whether each location
    has ever had a non-null value in the specified column. The new column will contain
    True if there is at least one non-null value at any point in time for the location
    and False otherwise.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        column_with_null_values (str): The name of the column to check for non-null values.

    Returns:
        DataFrame: A new DataFrame with an additional column indicating the presence of non-null values.
    """
    w = Window.partitionBy(IndCqc.location_id, IndCqc.care_home)

    df = df.withColumn(
        IndCqc.has_non_null_value,
        F.max(
            F.when(F.col(column_with_null_values).isNotNull(), True).otherwise(False)
        ).over(w),
    )

    return df


def model_imputation(
    df: DataFrame, column_with_null_values: str, imputation_model_column_name: str
) -> DataFrame:
    """
    Merges the original '<column_with_null_values>' with extrapolated and interpolated columns to create a new column.

    This function merges the original '<column_with_null_values>' with extrapolated and interpolated columns to create
    a new column called '<imputation_model_column_name>'.

    Args:
        df (DataFrame): A dataframe with the columns extrapolation_model and interpolation_model.
        column_with_null_values (str): The name of the column containing null values to be extrapolated and interpolated.
        imputation_model_column_name (str): The name of the new column.

    Returns:
        Dataframe: A dataframe with a new merged column called '<imputation_model_column_name>'.
    """
    df = df.withColumn(
        imputation_model_column_name,
        F.coalesce(
            F.col(column_with_null_values),
            F.col(IndCqc.extrapolation_model),
            F.col(IndCqc.interpolation_model),
        ),
    )
    return df
