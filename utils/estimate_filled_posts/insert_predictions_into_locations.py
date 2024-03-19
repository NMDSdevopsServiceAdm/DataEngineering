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
        (locations_df[IndCqc.location_id] == predictions_df[IndCqc.location_id])
        & (
            locations_df[IndCqc.cqc_location_import_date]
            == predictions_df[IndCqc.cqc_location_import_date]
        ),
        "left",
    )

    locations_with_predictions = locations_with_predictions.select(
        locations_df["*"], predictions_df[IndCqc.prediction]
    )

    locations_with_prediction_model_column = locations_with_predictions.withColumn(
        model_column_name, F.col(IndCqc.prediction)
    )
    locations_with_prediction_model_column = (
        locations_with_prediction_model_column.withColumn(
            IndCqc.estimate_filled_posts,
            F.when(
                F.col(IndCqc.estimate_filled_posts).isNotNull(),
                F.col(IndCqc.estimate_filled_posts),
            ).otherwise(F.col(IndCqc.prediction)),
        )
    )

    locations_df = locations_with_prediction_model_column.drop(F.col(IndCqc.prediction))
    return locations_df
