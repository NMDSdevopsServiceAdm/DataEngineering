from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects._03_independent_cqc._04_model.utils.paths import generate_predictions_path
from utils import utils
from utils.cleaning_utils import calculate_filled_posts_from_beds_and_ratio
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def enrich_with_model_predictions(
    ind_cqc_df: DataFrame, bucket_name: str, model_name: str
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
        bucket_name (str): the bucket (name only) in which to source the model predictions dataset from.
        model_name (str): The name of the model.

    Returns:
        DataFrame: The input DataFrame with the model predictions merged in.
    """
    predictions_path = generate_predictions_path(bucket_name, model_name)

    predictions_df = utils.read_from_parquet(predictions_path)

    if model_name == IndCqc.care_home_model:
        predictions_df = calculate_filled_posts_from_beds_and_ratio(
            predictions_df, IndCqc.prediction, IndCqc.prediction
        )

    ind_cqc_with_predictions_df = join_model_predictions(
        ind_cqc_df, predictions_df, model_name, include_run_id=True
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


def join_model_predictions(
    df: DataFrame,
    predictions_df: DataFrame,
    model_name: str,
    include_run_id: bool = True,
) -> DataFrame:
    """
    Prepares the predictions dataframe then joins them into the input dataframe on location ID and import date.

    Selects location ID and import date for joining on plus the predicted values and run ID, if required.
    The generic prediction columns are renamed to be model-specific so it is clear which model they relate to after the join.
    The prepared predictions dataframe is then left-joined into the input dataframe.

    Args:
        df (DataFrame): The input DataFrame to join predictions into.
        predictions_df (DataFrame): The input DataFrame containing model predictions.
        model_name (str): The name of the model to use for renaming columns.
        include_run_id (bool): Whether to include the prediction run ID column.

    Returns:
        DataFrame: The input DataFrame with the model predictions joined in.
    """
    cols = [
        IndCqc.location_id,
        IndCqc.cqc_location_import_date,
        IndCqc.prediction,
    ]

    rename_map = {
        IndCqc.prediction: model_name,
    }

    if include_run_id:
        cols.append(IndCqc.prediction_run_id)
        rename_map[IndCqc.prediction_run_id] = f"{model_name}_run_id"

    prepared_predictions_df = predictions_df.select(*cols).withColumnsRenamed(
        rename_map
    )

    return df.join(
        prepared_predictions_df,
        [IndCqc.location_id, IndCqc.cqc_location_import_date],
        "left",
    )
