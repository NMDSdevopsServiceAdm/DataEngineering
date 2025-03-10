from pyspark.sql import DataFrame

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
