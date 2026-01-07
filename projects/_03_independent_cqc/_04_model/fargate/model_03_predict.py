import polars as pl

from polars_utils import utils
from projects._03_independent_cqc._04_model.registry.model_registry import (
    model_registry,
)
from projects._03_independent_cqc._04_model.utils import model_utils as mUtils
from projects._03_independent_cqc._04_model.utils import paths
from projects._03_independent_cqc._04_model.utils import training_utils as tUtils
from projects._03_independent_cqc._04_model.utils import versioning as vUtils
from projects._03_independent_cqc._04_model.utils.validate_model_definitions import (
    validate_model_definition,
)
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys


def main(bucket_name: str, model_name: str) -> None:
    """
    Loads the latest trained model and runs predictions on the full feature dataset.

    The steps in this function are:
        1. Create paths for model specific features dataset
        2. Validate model and model definitions exist, then assign them to variables
        3. Load the features dataset
        4. Convert the features dataset to a NumPy array
        5. Load the latest trained model
        6. Run predictions using the loaded model
        7. Create a predictions DataFrame with relevant metadata
        8. Save the predictions DataFrame to parquet

    Note: the modelling process requires eager Polars DataFrames because scikit-learn
    operates on in-memory NumPy arrays.

    Args:
        bucket_name (str): the bucket (name only) in which to source the features dataset from
        model_name (str): the name of the saved model to load
    """
    print(f"Running predictions for {model_name} model...")

    features_source = paths.generate_features_path(bucket_name, model_name)

    validate_model_definition(
        model_name,
        required_keys=[
            MRKeys.version,
            MRKeys.auto_retrain,
            MRKeys.dependent,
            MRKeys.features,
        ],
        model_registry=model_registry,
    )

    model_def = model_registry[model_name]

    model_version = model_def[MRKeys.version]
    dependent_col = model_def[MRKeys.dependent]
    feature_cols = model_def[MRKeys.features]

    df = utils.scan_parquet(features_source).collect()

    X, _ = tUtils.convert_dataframe_to_numpy(df, feature_cols, dependent_col)

    model_path = paths.generate_model_path(bucket_name, model_name, model_version)
    run_number = vUtils.get_run_number(model_path)

    model = vUtils.load_latest_model(model_path, run_number)

    predictions = model.predict(X)

    predictions_df = mUtils.create_predictions_dataframe(
        df, predictions, model_name, model_version, run_number
    )

    predictions_path = paths.generate_predictions_path(bucket_name, model_name)

    utils.write_to_parquet(predictions_df, predictions_path, append=False)

    print(f"Predictions saved to {predictions_path}")


if __name__ == "__main__":

    args = utils.get_args(
        ("--bucket_name", "The bucket to source the datasets and models from"),
        ("--model_name", "The name of the model to run predictions with"),
    )

    main(bucket_name=args.bucket_name, model_name=args.model_name)
