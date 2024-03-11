import pyspark.sql

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)

from pyspark.sql import functions as F


def insert_predictions_into_locations(
    locations_df: pyspark.sql.DataFrame,
    predictions_df: pyspark.sql.DataFrame,
    model_column_name: str,
) -> pyspark.sql.DataFrame:
    locations_with_predictions = locations_df.join(
        predictions_df,
        (locations_df["locationid"] == predictions_df["locationid"])
        & (locations_df["snapshot_date"] == predictions_df["snapshot_date"]),
        "left",
    )

    locations_with_predictions = locations_with_predictions.select(
        locations_df["*"], predictions_df["prediction"]
    )

    locations_with_prediction_model_column = locations_with_predictions.withColumn(
        model_column_name, F.col("prediction")
    )
    locations_with_prediction_model_column = (
        locations_with_prediction_model_column.withColumn(
            ESTIMATE_JOB_COUNT,
            F.when(
                F.col(ESTIMATE_JOB_COUNT).isNotNull(), F.col(ESTIMATE_JOB_COUNT)
            ).otherwise(F.col("prediction")),
        )
    )

    locations_df = locations_with_prediction_model_column.drop(F.col("prediction"))
    return locations_df
