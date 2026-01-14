from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import DataFrame

from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.utils import (
    prepare_predictions_and_join_into_df,
    set_min_value,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def model_non_res_without_dormancy(
    locations_df: DataFrame,
    features_df: DataFrame,
    model_source: str,
) -> DataFrame:
    """
    Runs the non residential without dormancy model.

    This function loads and runs the non residential without dormancy
    predictions model on non residential without dormancy features data.
    It then adds the predictions into the main dataset.

    Predictions are capped at a minimum of one.

    Args:
        locations_df (DataFrame): A dataframe containing cleaned independent CQC data.
        features_df (DataFrame): A dataframe containing model features for the non res without dormancy model. This should only contain rows where the primary service type is non-residential.
        model_source (str): The file path to the non residential without dormancy model.

    Returns:
        DataFrame: A dataframe with non residential without dormancy model estimates added.
    """
    trained_model = LinearRegressionModel.load(model_source)

    predictions_df = trained_model.transform(features_df)

    predictions_df = set_min_value(predictions_df, IndCqc.prediction, 1.0)

    locations_df = prepare_predictions_and_join_into_df(
        locations_df,
        predictions_df,
        IndCqc.non_res_without_dormancy_model,
        include_run_id=False,
    )

    return locations_df
