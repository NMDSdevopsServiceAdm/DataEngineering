"""
TO DO

Started this process but wondering if it's a lot easier to just do

filled_posts = people_directly_employed * filled_posts_per_person_ratio

Come back to this once the job is up and running to compare outputs to main

If it is, delete the model registry and simplify the process
"""

import polars as pl

import projects._03_independent_cqc._04_model.utils.validate_model_definitions as vUtils
from projects._03_independent_cqc._04_model.registry.model_registry import (
    model_registry,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys
from utils.column_values.categorical_column_values import CareHome


def model_pir_filled_posts(
    lf: pl.LazyFrame, bucket_name: str, model_name: str
) -> pl.LazyFrame:
    print("Modelling PIR filled posts...")

    model_name = IndCQC.pir_filled_posts_model

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

    model_def = model_registry[model_name]

    model_version = model_def[MRKeys.version]
    auto_retrain_model = model_def[MRKeys.auto_retrain]
    model_type = model_def[MRKeys.model_type]
    model_params = model_def[MRKeys.model_params]
    dependent_col = model_def[MRKeys.dependent]
    feature_col = model_def[MRKeys.features]

    pir_lf = lf.filter(
        (pl.col(IndCQC.care_home) == CareHome.not_care_home)
        & pl.col(feature_col).is_not_null()
        & (pl.col(feature_col) > 0)
    )

    features_lf = create_pir_features(pir_lf, feature_col, dependent_col)

    if auto_retrain_model:
        print(f"Retraining {model_name}.")
        train_model(features_lf)

    predict_and_merge()


def create_pir_features(
    lf: pl.LazyFrame, feature_col: str, dependent_col: str, abs_diff_cutoff: int = 500
) -> pl.LazyFrame:
    """
    Creates a features dataset specific to the PIR filled posts model.

    Only rows with known dependent values and reasonable absolute differences
    between the feature and dependent values are included in the features
    dataset.

    Args:
        lf (pl.LazyFrame): the input dataset
        feature_col (str): the name of the feature column in the model
        dependent_col (str): the name of the dependent column in the model
        abs_diff_cutoff (int): the maximum absolute difference allowed between the
            feature and dependent values for a row to be included in the features
            dataset. This is to exclude outliers which could negatively impact
            model training.

    Returns:
        pl.LazyFrame: the features dataset for the PIR filled posts model
    """
    dependent = pl.col(dependent_col)
    feature = pl.col(feature_col)

    return lf.filter(
        dependent.is_not_null()
        & (dependent > 0)
        & ((feature - dependent).abs() < abs_diff_cutoff)
    )


def train_model(features_lf: pl.LazyFrame):
    pass


def predict_and_merge():
    pass
