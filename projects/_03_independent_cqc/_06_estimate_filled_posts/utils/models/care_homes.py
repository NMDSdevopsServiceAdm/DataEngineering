from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import DataFrame

from utils.cleaning_utils import calculate_filled_posts_from_beds_and_ratio
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.utils import (
    insert_predictions_into_pipeline,
    set_min_value,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.ml_model_metrics import (
    save_model_metrics,
)


def model_care_homes(
    locations_df: DataFrame,
    features_df: DataFrame,
    model_source: str,
    metrics_destination: str,
) -> DataFrame:
    trained_model = LinearRegressionModel.load(model_source)

    ratio_predictions_df = trained_model.transform(features_df)

    filled_post_predictions_df = calculate_filled_posts_from_beds_and_ratio(
        ratio_predictions_df, IndCqc.prediction, IndCqc.prediction
    )
    filled_post_predictions_df = set_min_value(
        filled_post_predictions_df, IndCqc.prediction, 1.0
    )

    save_model_metrics(
        filled_post_predictions_df,
        IndCqc.ascwds_filled_posts_dedup_clean,
        model_source,
        metrics_destination,
    )

    locations_df = insert_predictions_into_pipeline(
        locations_df, filled_post_predictions_df, IndCqc.care_home_model
    )

    return locations_df
