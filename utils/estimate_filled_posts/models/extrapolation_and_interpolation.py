from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.estimate_filled_posts.models.extrapolation_new import model_extrapolation
from utils.estimate_filled_posts.models.interpolation_new import model_interpolation


def model_imputation_with_extrapolation_and_interpolation(
    df: DataFrame,
    column_with_null_values: str,
    model_column_name: str,
) -> DataFrame:
    """
    Create a new column of imputed values based on known values and null values being extrapolated and interpolated.

    This function creates a new column of data which takes non-null values from a column containing null values, and populates
    the null values based on the rate of change of values in '<model_column_name>'. Values before the first known submission and
    after the last known submission are extrapolated based on the rate of change of the '<model_column_name>'. Values in between
    non-null values are interpolated using the rate of change of the '<model_column_name>' but the trend is adapted so that the
    end point matches the next non-null value. A new column is added which includes the name of the '<column_with_null_values>'
    and the '<model_column_name>'.

    Args:
        df (DataFrame): The input DataFrame containing the columns to be extrapolated and interpolated.
        column_with_null_values (str): The name of the column containing null values to be extrapolated and interpolated.
        model_column_name (str): The name of the column containing the model values used for extrapolation and interpolation.

    Returns:
        DataFrame: The DataFrame with the added column for imputed values.
    """
    imputation_model_column_name = create_imputation_model_name(
        column_with_null_values, model_column_name
    )

    df = model_extrapolation(
        df,
        column_with_null_values,
        model_column_name,
    )
    df = model_interpolation(
        df,
        column_with_null_values,
    )
    df = model_imputation(df, column_with_null_values, imputation_model_column_name)
    df = df.drop(
        IndCqc.extrapolation_backwards,
        IndCqc.extrapolation_forwards,
        IndCqc.extrapolation_residual,
        IndCqc.proportion_of_time_between_submissions,
        IndCqc.extrapolation_model,
        IndCqc.interpolation_model,
    )

    return df


def create_imputation_model_name(
    column_with_null_values: str, model_column_name: str
) -> str:
    """
    Generate a new column name imputation model output.

    Generate a new column names for imputation model outputs which includes the name of the column with
    null values and the model name which trend line the process is following. For example:
        - 'filled_posts' using the 'rolling_average_model' trend would become
          'imputatation_filled_posts_rolling_average_model".
        - 'filled_posts_per_bed_ratio' using the 'care_home_model' trend would become
          'imputatation_filled_posts_per_bed_ratio_care_home_model".


    Args:
        column_with_null_values (str): The name of the column containing null values to be extrapolated and interpolated.
        model_column_name (str): The name of the model column to use.

    Returns:
        str: A string with the imputation model column name.
    """
    imputation_model_column_name = (
        "imputation_" + column_with_null_values + "_" + model_column_name
    )
    return imputation_model_column_name


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
