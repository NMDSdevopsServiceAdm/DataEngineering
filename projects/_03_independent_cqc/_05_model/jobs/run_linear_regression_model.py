from utils.cleaning_utils import calculate_filled_posts_from_beds_and_ratio
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
    PartitionKeys as Keys,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models import (
    utils as mUtils,
)
from utils import utils

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    branch_name: str,
    model_name: str,
    model_version: str,
) -> None:
    """
    Loads and runs a linear regression model then saves the predictions as parquet files.

    This function loads and runs a linear regression model on features dataset.
    If the model is a care home model (which predicts the filled posts per bed ratio), it
    multiplies the prediction by the number of beds to get the filled post equivalent prediction.
    Predictions are capped at a minimum of one.
    The predictions are then saved as parquet files.

    Args:
        branch_name (str): The name of the branch currently being used.
        model_name (str): The name of the model to run.
        model_version (str): The version of the model to load (e.g. '1.0.0').
    """
    print(f"Running model: {model_name} version: {model_version}")

    care_home_identifier: str = "care_home"

    features_source = mUtils.generate_model_features_s3_path(branch_name, model_name)
    predictions_destination = mUtils.generate_model_predictions_s3_path(
        branch_name, model_name
    )
    model_s3_location = mUtils.generate_model_s3_path(
        branch_name, model_name, model_version
    )

    trained_model = mUtils.load_latest_model_from_s3(model_s3_location)

    features_df = utils.read_from_parquet(features_source)

    predictions_df = trained_model.transform(features_df)

    if care_home_identifier in model_name:
        predictions_df = calculate_filled_posts_from_beds_and_ratio(
            predictions_df, model_name, model_name
        )

    predictions_df = mUtils.set_min_value(predictions_df, model_name, 1.0)

    predictions_df = predictions_df.select(
        IndCqc.location_id, IndCqc.cqc_location_import_date, model_name
    )

    utils.write_to_parquet(
        predictions_df,
        predictions_destination,
        "overwrite",
        PartitionKeys,
    )


if __name__ == "__main__":
    (
        branch_name,
        model_name,
        model_version,
    ) = utils.collect_arguments(
        (
            "--branch_name",
            "The name of the branch currently being used",
        ),
        (
            "--model_name",
            "The name of the model to run",
        ),
        (
            "--model_version",
            "The version of the model to load (e.g. '1.0.0')",
        ),
    )
    main(
        branch_name,
        model_name,
        model_version,
    )
