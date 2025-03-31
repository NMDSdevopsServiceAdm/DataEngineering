from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import CareHome, PrimaryServiceType


def insert_predictions_into_pipeline(
    locations_df: DataFrame,
    predictions_df: DataFrame,
    model_column_name: str,
) -> DataFrame:
    """
    Inserts model predictions into locations dataframe.

    This function renames the model prediction column and performs a left join
    to merge it into the locations dataframe based on matching 'location_id'
    and 'cqc_location_import_date' values.

    Args:
        locations_df (DataFrame): A dataframe containing independent CQC data.
        predictions_df (DataFrame): A dataframe containing model predictions.
        model_column_name (str): The name of the column containing the model predictions.

    Returns:
        DataFrame: A dataframe with model predictions added.
    """
    predictions_df = predictions_df.select(
        IndCqc.location_id, IndCqc.cqc_location_import_date, IndCqc.prediction
    ).withColumnRenamed(IndCqc.prediction, model_column_name)

    locations_with_predictions = locations_df.join(
        predictions_df, [IndCqc.location_id, IndCqc.cqc_location_import_date], "left"
    )

    return locations_with_predictions


def set_min_value(df: DataFrame, col_name: str, min_value: float = 1.0) -> DataFrame:
    """
    Sets the minimum value of a specified column to a specified value (default is 1.0).

    Args:
        df (DataFrame): A dataframe containing the specified column.
        col_name (str): The name of the column to set the minimum value for.
        min_value (float): The minimum value allowed in the specified column.

    Returns:
        DataFrame: A dataframe with the specified column set to the minimum value.
    """
    return df.withColumn(
        col_name,
        F.when(
            F.col(col_name).isNotNull(),
            F.greatest(F.col(col_name), F.lit(min_value)),
        ).otherwise(F.lit(None)),
    )


def combine_care_home_ratios_and_non_res_posts(
    df: DataFrame,
    ratio_column_with_values: str,
    posts_column_with_values: str,
    new_column_name: str,
) -> DataFrame:
    """
    Creates one column which inputs the ratio value if the location is a care home and the filled post value if not.

    Args:
        df (DataFrame): The input DataFrame.
        ratio_column_with_values (str): The name of the filled posts per bed ratio column to average (for care homes only).
        posts_column_with_values (str): The name of the filled posts column to average.
        new_column_name (str): The name of the new column with combined values.

    Returns:
        DataFrame: The input DataFrame with the new column containing a single column with the relevant combined column.
    """
    df = df.withColumn(
        new_column_name,
        F.when(
            F.col(IndCqc.care_home) == CareHome.care_home,
            F.col(ratio_column_with_values),
        ).otherwise(F.col(posts_column_with_values)),
    )
    return df


def clean_number_of_beds_banded(df: DataFrame) -> DataFrame:
    """
    Cleans the number_of_beds_banded column by merging 'care home with nursing' bands 1 & 2 due to low bases.

    The bands are combined by replacing the band 1 value with band 2 if the location has the service 'care home with nursing'.
    All other bands remain as they were.

    Args:
        df (DataFrame): The input DataFrame containing the 'number_of_beds_banded' column.

    Returns:
        DataFrame: The input DataFrame with the new 'number_of_beds_banded' column updated.
    """
    band_one: int = 1.0
    band_two: int = 2.0

    df = df.withColumn(
        IndCqc.number_of_beds_banded,
        F.when(
            (F.col(IndCqc.number_of_beds_banded) == band_one)
            & (
                F.col(IndCqc.primary_service_type)
                == PrimaryServiceType.care_home_with_nursing
            ),
            F.lit(band_two),
        ).otherwise(F.col(IndCqc.number_of_beds_banded)),
    )
    return df
