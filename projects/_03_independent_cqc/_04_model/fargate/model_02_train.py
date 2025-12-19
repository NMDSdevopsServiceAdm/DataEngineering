import polars as pl

import projects._03_independent_cqc._04_model.utils.paths as pUtils
import projects._03_independent_cqc._04_model.utils.training_utils as tUtils
import projects._03_independent_cqc._04_model.utils.validate_model_definitions as vUtils
from polars_utils import utils
from projects._03_independent_cqc._04_model.registry.model_registry import (
    model_registry,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
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
        8. Save the trained model with an updated run number

    Note: the modelling process requires DataFrames instead of LazyFrames.

    Args:
        bucket_name (str): the bucket (name only) in which to source and save files to
        model_name (str): the name of the model to train
    """
    print(f"Training {model_name} model...")

    source = pUtils.generate_features_path(bucket_name, model_name)

    vUtils.validate_model_definition(
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
    model_version = model_registry[model_name][MRKeys.version]
    auto_retrain_model = model_registry[model_name][MRKeys.auto_retrain]
    model_type = model_registry[model_name][MRKeys.model_type]
    model_params = model_registry[model_name][MRKeys.model_params]
    dependent_col = model_registry[model_name][MRKeys.dependent]
    feature_cols = model_registry[model_name][MRKeys.features]

    df = (
        utils.scan_parquet(source).filter(pl.col(dependent_col).is_not_null()).collect()
    )

    train_df, test_df = tUtils.split_train_test(df, frac=0.8)

    X_train, y_train = tUtils.convert_dataframe_to_numpy(
        train_df, feature_cols, dependent_col
    )
    X_test, y_test = tUtils.convert_dataframe_to_numpy(
        test_df, feature_cols, dependent_col
    )

    # Make a model based on the specified type (remember to scale if lasso)

    # model.fit(X_train, y_train)

    # predictions = model.predict(X_test)
    # calculate and store metrics

    # Save the model with an incremented run number
