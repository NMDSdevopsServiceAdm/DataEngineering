from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import DataFrame

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.utils import (
    insert_predictions_into_pipeline,
    set_min_value,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.ml_model_metrics import (
    save_model_metrics,
)


def model_non_res_without_dormancy(
    locations_df: DataFrame,
    features_df: DataFrame,
    model_source: str,
    metrics_destination: str,
) -> DataFrame:
    """
    Runs the non residential without dormancy model.

    This function loads and runs the non residential without dormancy
    predictions model on non residential without dormancy features data.
    It then saves the model metrics and adds the predictions into the main dataset.

    Predictions are capped at a minimum of one.

    Args:
        locations_df (DataFrame): A dataframe containing cleaned independent CQC data.
        features_df (DataFrame): A dataframe containing model features for the non res without dormancy model. This should only contain rows where the primary service type is non-residential.
        model_source (str): The file path to the non residential without dormancy model.
        metrics_destination (str): The file path to the destination for saving metrics.

    Returns:
        DataFrame: A dataframe with non residential without dormancy model estimates added.
    """
    trained_model = LinearRegressionModel.load(model_source)

    predictions_df = trained_model.transform(features_df)

    predictions_df = set_min_value(predictions_df, IndCqc.prediction, 1.0)

    save_model_metrics(
        predictions_df,
        IndCqc.ascwds_pir_merged,
        model_source,
        metrics_destination,
    )

    locations_df = insert_predictions_into_pipeline(
        locations_df,
        predictions_df,
        IndCqc.non_res_without_dormancy_model,
    )

    return locations_df
