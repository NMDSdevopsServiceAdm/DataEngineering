from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql import DataFrame

from utils.estimate_filled_posts.insert_predictions_into_locations import (
    insert_predictions_into_locations,
)
from utils.estimate_filled_posts.r2_metric import generate_r2_metric
from utils.ind_cqc_filled_posts_utils.utils import (
    update_dataframe_with_identifying_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_care_homes(
    locations_df: DataFrame, features_df: DataFrame, model_path: str
) -> tuple:
    gbt_trained_model = GBTRegressionModel.load(model_path)
    features_df = features_df.where(features_df[IndCqc.care_home] == "Y")
    features_df = features_df.where(features_df[IndCqc.current_region].isNotNull())
    features_df = features_df.where(features_df[IndCqc.number_of_beds].isNotNull())

    care_home_predictions = gbt_trained_model.transform(features_df)

    non_null_filled_posts_df = care_home_predictions.where(
        care_home_predictions[IndCqc.ascwds_filled_posts_dedup_clean].isNotNull()
    )

    metrics_info = {
        IndCqc.r2: generate_r2_metric(
            non_null_filled_posts_df,
            IndCqc.prediction,
            IndCqc.ascwds_filled_posts_dedup_clean,
        ),
        IndCqc.percentage_data: (features_df.count() / locations_df.count()) * 100,
    }
    locations_df = insert_predictions_into_locations(
        locations_df, care_home_predictions, IndCqc.care_home_model
    )

    locations_df = update_dataframe_with_identifying_rule(
        locations_df, "model_care_homes", IndCqc.estimate_filled_posts
    )

    return locations_df, metrics_info
