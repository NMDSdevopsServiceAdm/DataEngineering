from pyspark.sql import DataFrame

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def insert_predictions_into_locations(
    locations_df: DataFrame,
    predictions_df: DataFrame,
    model_column_name: str,
) -> DataFrame:
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

    locations_with_predictions = locations_with_predictions.withColumnRenamed(
        IndCqc.prediction, model_column_name
    )

    return locations_with_predictions
