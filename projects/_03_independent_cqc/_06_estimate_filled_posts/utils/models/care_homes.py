from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import DataFrame

from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.utils import (
    prepare_predictions_and_join_into_df,
    set_min_value,
)
from utils.cleaning_utils import calculate_filled_posts_from_beds_and_ratio
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def model_care_homes(
    locations_df: DataFrame,
    features_df: DataFrame,
    model_source: str,
) -> DataFrame:
    trained_model = LinearRegressionModel.load(model_source)

    ratio_predictions_df = trained_model.transform(features_df)

    filled_post_predictions_df = calculate_filled_posts_from_beds_and_ratio(
        ratio_predictions_df, IndCqc.prediction, IndCqc.prediction
    )
    filled_post_predictions_df = set_min_value(
        filled_post_predictions_df, IndCqc.prediction, 1.0
    )

    locations_df = prepare_predictions_and_join_into_df(
        locations_df,
        filled_post_predictions_df,
        IndCqc.care_home_model,
        include_run_id=False,
    )

    return locations_df
