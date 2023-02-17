from utils.estimate_job_count.column_names import ESTIMATE_JOB_COUNT

from pyspark.sql import functions as F


def insert_predictions_into_locations(locations_df, predictions_df):
    locations_with_predictions = locations_df.join(
        predictions_df,
        (locations_df["locationid"] == predictions_df["locationid"])
        & (locations_df["snapshot_date"] == predictions_df["snapshot_date"]),
        "left",
    )

    locations_with_predictions = locations_with_predictions.select(
        locations_df["*"], predictions_df["prediction"]
    )

    locations_with_predictions = locations_with_predictions.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            F.col(ESTIMATE_JOB_COUNT).isNotNull(), F.col(ESTIMATE_JOB_COUNT)
        ).otherwise(F.col("prediction")),
    )

    locations_df = locations_with_predictions.drop(F.col("prediction"))
    return locations_df
