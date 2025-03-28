from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import DataFrame

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.estimate_filled_posts.models.utils import insert_predictions_into_pipeline
from utils.estimate_filled_posts.ml_model_metrics import save_model_metrics


def model_non_res_pir_linear_regression(
    locations_df: DataFrame,
    features_df: DataFrame,
    model_source: str,
    metrics_destination: str,
) -> DataFrame:
    """
    Runs the non residential model to convert people directly employed values to estimated filled posts.

    This function loads and runs the non residential PIR linear regression model on non residential with
    PIR features data. It then saves the model metrics and adds the predictions into the main dataset.

    Args:
        locations_df (DataFrame): A dataframe containing cleaned independent CQC data.
        features_df (DataFrame): A dataframe containing model features for the non res with PIR model (where PIR is not null and primary service type is non-res).
        model_source (str): The file path to the non residential pir linear regression model.
        metrics_destination (str): The file path to the destination for saving metrics.

    Returns:
        DataFrame: A dataframe with non residential PIR linear regression model estimates added.
    """
    lr_trained_model = LinearRegressionModel.load(model_source)

    non_res_predictions = lr_trained_model.transform(features_df)

    save_model_metrics(
        non_res_predictions,
        IndCqc.ascwds_pir_merged,
        model_source,
        metrics_destination,
    )

    locations_df = insert_predictions_into_pipeline(
        locations_df,
        non_res_predictions,
        IndCqc.non_res_pir_linear_regression_model,
    )

    return locations_df
