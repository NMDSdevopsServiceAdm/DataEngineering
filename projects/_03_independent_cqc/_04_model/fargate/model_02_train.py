import polars as pl

from polars_utils import utils
from projects._03_independent_cqc._04_model.registry.model_registry import (
    model_registry,
)
from projects._03_independent_cqc._04_model.utils import paths
from projects._03_independent_cqc._04_model.utils import training_utils as tUtils
from projects._03_independent_cqc._04_model.utils import versioning as vUtils
from projects._03_independent_cqc._04_model.utils.build_model import build_model
from projects._03_independent_cqc._04_model.utils.metrics import calculate_metrics
from projects._03_independent_cqc._04_model.utils.validate_model_definitions import (
    validate_model_definition,
)
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys


def main(bucket_name: str, model_name: str) -> None:
    """
    Loads a features dataset then trains, tests and saves a specified model.

    The steps in this function are:
        1. Create paths for model specific features dataset
        2. Validate model and model definitions exist, then assign them to variables
        3. Load the features dataset
        4. Split the dataset into train and test sets
        5. Convert the train and test sets to NumPy arrays
        6. Train the model
        7. Test the model
        8. Save the trained model and model metadata with an updated run number

    Note: the modelling process requires eager Polars DataFrames because scikit-learn
    operates on in-memory NumPy arrays.

    Args:
        bucket_name (str): the bucket (name only) in which to source the features dataset from
        model_name (str): the name of the model to train
    """
    print(f"Training {model_name} model...")

    features_source = paths.generate_features_path(bucket_name, model_name)

    validate_model_definition(
        model_name,
        required_keys=[
            MRKeys.version,
            MRKeys.auto_retrain,
            MRKeys.model_type,
            MRKeys.model_params,
            MRKeys.dependent,
            MRKeys.features,
        ],
        model_registry=model_registry,
    )

    model_def = model_registry[model_name]

    model_version = model_def[MRKeys.version]
    auto_retrain_model = model_def[MRKeys.auto_retrain]
    model_type = model_def[MRKeys.model_type]
    model_params = model_def[MRKeys.model_params]
    dependent_col = model_def[MRKeys.dependent]
    feature_cols = model_def[MRKeys.features]

    if not auto_retrain_model:
        print(f"Auto-retraining is disabled for model {model_name}. Skipping training.")
        return

    df = (
        utils.scan_parquet(features_source)
        .filter(pl.col(dependent_col).is_not_null())
        .collect()
    )

    train_df, test_df = tUtils.split_train_test(df, fraction=0.8, seed=42)

    X_train, y_train = tUtils.convert_dataframe_to_numpy(
        train_df, feature_cols, dependent_col
    )
    X_test, y_test = tUtils.convert_dataframe_to_numpy(
        test_df, feature_cols, dependent_col
    )

    model = build_model(model_type, model_params)

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    metrics = calculate_metrics(y_test, predictions)

    metadata = {
        "name": model_name,
        "type": model_type,
        "parameters": model_params,
        "version": model_version,
        "feature_columns": feature_cols,
        "dependent_column": dependent_col,
        "metrics": metrics,
    }

    model_path = paths.generate_model_path(bucket_name, model_name, model_version)
    new_run_number = vUtils.get_run_number(model_path) + 1
    vUtils.save_model_and_metadata(model_path, new_run_number, model, metadata)

    print(f"{model_name} model trained and saved with run number {new_run_number}.")


if __name__ == "__main__":

    args = utils.get_args(
        ("--bucket_name", "The bucket to source and save the datasets to"),
        ("--model_name", "The name of the model to create features for"),
    )

    main(bucket_name=args.bucket_name, model_name=args.model_name)
