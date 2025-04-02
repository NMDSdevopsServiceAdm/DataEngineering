from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import CareHome


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
    The function takes the greatest value between the existing value and the specified min_value which defaults to 1.0.

    Args:
        df (DataFrame): A dataframe containing the specified column.
        col_name (str): The name of the column to set the minimum value for.
        min_value (float): The minimum value allowed in the specified column.

    Returns:
        DataFrame: A dataframe with the specified column set to greatest value of the original value or the min_value.
    """
    return df.withColumn(
        col_name,
        F.when(
            F.col(col_name).isNotNull(),
            F.greatest(F.col(col_name), F.lit(min_value)),
        ).otherwise(F.lit(None)),
    )


def convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values(
    df: DataFrame,
    ratio_column: str,
    posts_column: str,
) -> DataFrame:
    """
    Multiplies the filled posts per bed ratio values by the number of beds at each care home location to create a filled posts figure.

    If the location is not a care home, the original filled posts figure is kept.

    Args:
        df (DataFrame): The input DataFrame.
        ratio_column (str): The name of the filled posts per bed ratio column (for care homes only).
        posts_column (str): The name of the filled posts column.

    Returns:
        DataFrame: The input DataFrame with the new column containing a single column with the relevant combined column.
    """
    df = df.withColumn(
        posts_column,
        F.when(
            F.col(IndCqc.care_home) == CareHome.care_home,
            F.col(ratio_column) * F.col(IndCqc.number_of_beds),
        ).otherwise(F.col(posts_column)),
    )
    return df
