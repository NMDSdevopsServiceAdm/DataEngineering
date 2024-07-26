from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql import DataFrame

from utils.estimate_filled_posts.insert_predictions_into_locations import (
    insert_predictions_into_locations,
)
from utils.estimate_filled_posts.ml_model_metrics import save_model_metrics
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils.column_values.categorical_column_values import CareHome


def model_non_res_with_dormancy(
    locations_df: DataFrame,
    features_df: DataFrame,
    model_source: str,
    metrics_destination: str,
) -> DataFrame:
    gbt_trained_model = GBTRegressionModel.load(model_source)
    features_df = features_df.where(
        (features_df[IndCqc.dormancy].isNotNull())
        & (features_df[IndCqc.care_home] == CareHome.care_home)
    )

    non_res_with_dormancy_predictions = gbt_trained_model.transform(features_df)

    save_model_metrics(
        non_res_with_dormancy_predictions,
        IndCqc.ascwds_filled_posts_dedup_clean,
        model_source,
        metrics_destination,
    )

    locations_df = insert_predictions_into_locations(
        locations_df,
        non_res_with_dormancy_predictions,
        IndCqc.non_res_with_dormancy_model,
    )

    return locations_df
