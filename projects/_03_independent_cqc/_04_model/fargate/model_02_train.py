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
        1.
        2.
        3.
        4.
        5.
        6.

    Note: the modelling process requires DataFrames instead of LazyFrames.

    Args:
        bucket_name (str): the bucket (name only) in which to source and save files to
        model_name (str): the name of the model to train
    """
    print(f"Training {model_name} model...")

    source = pUtils.generate_features_path(bucket_name, model_name)

    vUtils.validate_model_definition(
        model_name,
        required_keys=[MRKeys.dependent, MRKeys.features],
        model_registry=model_registry,
    )
    dependent_col = model_registry[model_name][MRKeys.dependent]
    feature_cols = model_registry[model_name][MRKeys.features]

    df = (
        utils.scan_parquet(source).filter(pl.col(dependent_col).is_not_null()).collect()
    )

    train_df, test_df = tUtils.split_train_test(df, frac=0.8)
