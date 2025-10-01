from projects._03_independent_cqc._05_model.utils.model import Model
from projects._03_independent_cqc._05_model.model_registry import (
    model_definitions,
)
from polars_utils import utils
import boto3
import json
import os
import polars as pl
import pickle

DATA_BUCKET = os.environ.get("DATA_BUCKET", "test_data_bucket")
RESOURCES_BUCKET = os.environ.get("RESOURCES_BUCKET", "test_resources_bucket")


def predict(source_data: pl.DataFrame, model: Model) -> pl.DataFrame:
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
    version_param = ssm.get_parameters(Name=version_location)
    version = json.loads(version_param["Parameter"]["Value"])["Current Version"]
    model_location = f"models/{parsed.model_name}/{version}/model.pkl"
    source_data = pl.read_parquet(f"s3://{DATA_BUCKET}/{data_source_prefix}")
    destination = f"{prediction_destination}/{version}/result.parquet"
    resp = s3.get_object(Bucket=DATA_BUCKET, Key=model_location)
    loaded_model = pickle.load(resp["Body"])
    predicted = predict(source_data, loaded_model)
    predicted.write_parquet(destination)
