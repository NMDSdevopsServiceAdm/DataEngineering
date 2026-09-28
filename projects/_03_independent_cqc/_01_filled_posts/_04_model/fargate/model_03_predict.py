from polars_utils import utils
from projects._03_independent_cqc._01_filled_posts._04_model.registry.model_registry import (
    model_registry,
)
from projects._03_independent_cqc._01_filled_posts._04_model.utils import paths
from projects._03_independent_cqc._01_filled_posts._04_model.utils import (
    versioning as vUtils,
)
from projects._03_independent_cqc._01_filled_posts._04_model.utils.model_utils import (
    add_predictions_into_df,
)
from projects._03_independent_cqc._01_filled_posts._04_model.utils.training_utils import (
    convert_dataframe_to_numpy,
)
from projects._03_independent_cqc._01_filled_posts._04_model.utils.validate_model_definitions import (
    validate_model_definition,
)
from utils.column_names.ind_cqc_pipeline_columns import ModelMetadataKeys as MMKeys
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys


def main(bucket_name: str, model_name: str) -> None:
    """
    Loads the latest trained model and runs predictions on the full feature dataset.

    The steps in this function are:
        1. Create paths for model specific features dataset
        2. Validate model and model definitions exist, then assign them to variables
        3. Check the latest trained model was trained on the registry's features
        4. Load the features dataset
        5. Convert the features dataset to a NumPy array
        6. Load the latest trained model
        7. Run predictions using the loaded model
        8. Create a predictions DataFrame with relevant metadata
        9. Save the predictions DataFrame to parquet

    The feature check is needed because scikit-learn matches features by position, not
    name, so predicting with a changed feature list could silently misalign them. It
    runs before the features dataset is loaded so that a mismatch fails fast.

    Note: the modelling process requires eager Polars DataFrames because scikit-learn
    operates on in-memory NumPy arrays.

    Args:
        bucket_name (str): the bucket (name only) in which to source the features dataset from
        model_name (str): the name of the saved model to load

    Raises:
        ValueError: If the latest trained model's saved features differ from the registry's.
    """
    print(f"Running predictions for {model_name}...")

    features_source = paths.generate_features_path(bucket_name, model_name)

    validate_model_definition(
        model_name,
        required_keys=[
            MRKeys.version,
            MRKeys.dependent,
            MRKeys.features,
        ],
        model_registry=model_registry,
    )

    model_def = model_registry[model_name]

    model_version = model_def[MRKeys.version]
    dependent_col = model_def[MRKeys.dependent]
    feature_cols = model_def[MRKeys.features]

    model_path = paths.generate_model_path(bucket_name, model_name, model_version)
    run_number = vUtils.get_run_number(model_path)

    metadata = vUtils.load_metadata(model_path, run_number)
    saved_feature_cols = metadata[MMKeys.feature_columns]
    if saved_feature_cols != feature_cols:
        raise ValueError(
            f"{model_name} run {run_number} was trained on different features to the model registry.\n"
            f"Saved features: {saved_feature_cols}\n"
            f"Registry features: {feature_cols}"
        )

    df = utils.scan_parquet(features_source).collect()

    X, _ = convert_dataframe_to_numpy(df, feature_cols, dependent_col)

    model = vUtils.load_model(model_path, run_number)

    predictions = model.predict(X)

    predictions_df = add_predictions_into_df(df, predictions, model_version, run_number)

    predictions_path = paths.generate_predictions_path(bucket_name, model_name)
    predictions_file = f"{predictions_path}predictions.parquet"

    utils.write_to_parquet(predictions_df, predictions_file, append=False)


if __name__ == "__main__":

    args = utils.get_args(
        ("--bucket_name", "The bucket to source the datasets and models from"),
        ("--model_name", "The name of the model to run predictions with"),
    )

    main(bucket_name=args.bucket_name, model_name=args.model_name)
