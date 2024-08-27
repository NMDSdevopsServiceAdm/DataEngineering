from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql import DataFrame, functions as F

from utils.estimate_filled_posts.insert_predictions_into_locations import (
    insert_predictions_into_locations,
)
from utils.estimate_filled_posts.ml_model_metrics import save_model_metrics
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_care_homes(
    locations_df: DataFrame,
    features_df: DataFrame,
    model_source: str,
    metrics_destination: str,
) -> DataFrame:
    gbt_trained_model = GBTRegressionModel.load(model_source)
    features_df = features_df.where(features_df[IndCqc.number_of_beds].isNotNull())

    care_home_predictions = gbt_trained_model.transform(features_df)

    save_model_metrics(
        care_home_predictions,
        IndCqc.ascwds_filled_posts_dedup_clean,
        model_source,
        metrics_destination,
    )

    locations_df = insert_predictions_into_locations(
        locations_df, care_home_predictions, IndCqc.care_home_model
    )

    # multiply ratio by beds to get final prediction
    locations_df = locations_df.withColumn(
        IndCqc.care_home_model,
        F.col(IndCqc.care_home_model) * F.col(IndCqc.number_of_beds),
    )

    return locations_df
