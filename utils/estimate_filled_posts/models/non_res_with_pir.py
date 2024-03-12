from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql import DataFrame

from utils.estimate_filled_posts.insert_predictions_into_locations import (
    insert_predictions_into_locations,
)
from utils.estimate_filled_posts.r2_metric import generate_r2_metric
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_non_residential_with_pir(
    locations_df: DataFrame, features_df: DataFrame, model_path: str
):
    gbt_trained_model = GBTRegressionModel.load(model_path)

    features_df = features_df.where(features_df[IndCqc.care_home] == "N")
    features_df = features_df.where(features_df[IndCqc.current_region].isNotNull())
    features_df = features_df.where(features_df[IndCqc.people_directly_employed] > 0)

    non_residential_with_pir_predictions = gbt_trained_model.transform(features_df)

    non_null_filled_posts_df = non_residential_with_pir_predictions.where(
        non_residential_with_pir_predictions[
            IndCqc.ascwds_filled_posts_dedup_clean
        ].isNotNull()
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
        locations_df, non_residential_with_pir_predictions, IndCqc.non_res_model
    )
    locations_df = update_dataframe_with_identifying_rule(
        locations_df,
        "model_non_res_with_pir",
        IndCqc.estimate_filled_posts,
    )

    return locations_df, metrics_info
