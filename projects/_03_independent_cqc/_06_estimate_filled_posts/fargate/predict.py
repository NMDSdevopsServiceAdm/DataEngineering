import json
import os
import pickle
import sys

import boto3
import polars as pl

from polars_utils import utils
from projects._03_independent_cqc._05_model.model_registry import model_definitions
from projects._03_independent_cqc._05_model.utils.model import Model

DATA_BUCKET = os.environ.get("S3_SOURCE_BUCKET", "test_data_bucket")
RESOURCES_BUCKET = os.environ.get("RESOURCES_BUCKET", "test_resources_bucket")


class TargetsNotFound(Exception):
    pass


class FeaturesNotFound(Exception):
    pass


def predict(source_data: pl.DataFrame, model: Model) -> pl.DataFrame:
    """
    Carries out the model prediction based on the feature columns and then populates the targets

    Args:
        source_data (pl.DataFrame): the source data
        model (Model): the model

    Returns:
        pl.DataFrame: the modified dataframe including predicted targets

    Raises:
        TargetsNotFound: if the target columns do not exist
        FeaturesNotFound: if the feature columns do not exist
    """
    if not (set(model.target_columns) <= set(source_data.columns)):
        print(
            "ERROR: Not all target columns %s exist in source_data",
            set(model.target_columns),
        )
        raise TargetsNotFound(
            "Not all target columns %s exist in source_data", set(model.target_columns)
        )
    elif not (set(model.feature_columns) <= set(source_data.columns)):
        print(
            "ERROR: Not all feature columns %s exist in source_data",
            set(model.target_columns),
        )
        raise FeaturesNotFound(
            "Not all feature columns %s exist in source_data", set(model.target_columns)
        )
    else:
        df_predict = model.predict(source_data)
        return source_data.with_columns(df_predict)


if __name__ == "__main__":
    parsed = utils.get_args(
        (
            "--model_name",
            "The name of the model being used",
        ),
    )
    data_source_prefix = model_definitions[parsed.model_name]["source_prefix"]
    prediction_destination = model_definitions[parsed.model_name][
        "prediction_destination"
    ]
    version_location = model_definitions[parsed.model_name][
        "version_parameter_location"
    ]
    ssm = boto3.client("ssm")
    s3 = boto3.client("s3")
    print("Getting the version information from Parameter Store")
    version_param = ssm.get_parameter(Name=version_location)
    version = json.loads(version_param["Parameter"]["Value"])["Current Version"]
    model_location = f"models/{parsed.model_name}/{version}/model.pkl"
    source_data = pl.read_parquet(f"s3://{DATA_BUCKET}/{data_source_prefix}/")
    destination = (
        f"s3://{DATA_BUCKET}/{prediction_destination}/{version}/result.parquet"
    )
    print("Retrieving the model from %s", model_location)
    resp = s3.get_object(Bucket=RESOURCES_BUCKET, Key=model_location)
    loaded_model = pickle.load(resp["Body"])
    print("Predicting the targets")
    predicted = predict(source_data, loaded_model)
    print("Saving predictions to %s", destination)
    predicted.write_parquet(destination)
