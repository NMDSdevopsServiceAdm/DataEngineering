from pyspark.ml.regression import GBTRegressionModel

from utils.estimate_job_count.column_names import ESTIMATE_JOB_COUNT
from utils.estimate_job_count.insert_predictions_into_locations import (
    insert_predictions_into_locations,
)
from utils.estimate_job_count.r2_metric import generate_r2_metric
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)


def model_non_residential_with_pir(locations_df, features_df, model_path):
    gbt_trained_model = GBTRegressionModel.load(model_path)

    features_df = features_df.where("carehome = 'N'")
    features_df = features_df.where("ons_region is not null")
    features_df = features_df.where("people_directly_employed > 0")

    features_df = features_df.withColumnRenamed(
        "non_residential_inc_pir_features", "features"
    )

    non_residential_with_pir_predictions = gbt_trained_model.transform(features_df)

    non_null_job_count_df = non_residential_with_pir_predictions.where(
        "job_count is not null"
    )

    metrics_info = {
        "r2": generate_r2_metric(non_null_job_count_df, "prediction", "job_count"),
        "data_percentage": (features_df.count() / locations_df.count()) * 100,
    }

    locations_df = insert_predictions_into_locations(
        locations_df, non_residential_with_pir_predictions, "non_res_with_pir_model"
    )
    locations_df = update_dataframe_with_identifying_rule(
        locations_df,
        "model_non_res_with_pir",
        ESTIMATE_JOB_COUNT,
    )

    return locations_df, metrics_info
