from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects._03_independent_cqc._04_model.utils.paths import generate_predictions_path
from utils import utils
from utils.cleaning_utils import calculate_filled_posts_from_beds_and_ratio
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


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


def merge_model_predictions(
    ind_cqc_df: DataFrame, data_bucket: str, model_name: str
) -> DataFrame:
    """
    Loads model predictions, applies transformations and joins into the input dataframe.

    This function calls utility functions to:
    - load model predictions from the specified data bucket
    - convert predicted ratios to filled posts (care home specific)
    - set minimum values for predictions
    - join the model predictions into the input dataframe
    - return the updated dataframe.

    Args:
        ind_cqc_df (DataFrame): The input DataFrame.
        data_bucket (str): The data bucket containing the model predictions.
        model_name (str): The name of the model.

    Returns:
        DataFrame: The input DataFrame with the model predictions merged in.
    """
    predictions_path = generate_predictions_path(data_bucket, model_name)

    predictions_df = utils.read_from_parquet(predictions_path)

    if model_name == IndCqc.care_home_model:
        predictions_df = calculate_filled_posts_from_beds_and_ratio(
            predictions_df, IndCqc.prediction, IndCqc.prediction
        )

    predictions_df = set_min_value(predictions_df, IndCqc.prediction, 1.0)

    predictions_df = prepare_predictions_for_join(predictions_df, model_name)

    ind_cqc_with_predictions_df = ind_cqc_df.join(
        predictions_df, [IndCqc.location_id, IndCqc.cqc_location_import_date], "left"
    )

    return ind_cqc_with_predictions_df


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


def prepare_predictions_for_join(
    predictions_df: DataFrame, model_name: str
) -> DataFrame:
    """
    Prepares the predictions dataframe for joining by selecting required columns and renaming them.

    Selects location ID and import date for joining on plus the predicted values and run ID.
    The generic prediction columns are renamed to be model-specific so it is clear which model they relate to after the join.

    Args:
        predictions_df (DataFrame): The input DataFrame containing model predictions.
        model_name (str): The name of the model to use for renaming columns.

    Returns:
        DataFrame: The prepared DataFrame ready for joining.
    """
    return predictions_df.select(
        IndCqc.location_id,
        IndCqc.cqc_location_import_date,
        IndCqc.prediction,
        IndCqc.prediction_run_id,
    ).withColumnsRenamed(
        {
            IndCqc.prediction: model_name,
            IndCqc.prediction_run_id: f"{model_name}_run_id",
        }
    )
