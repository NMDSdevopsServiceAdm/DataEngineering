import polars as pl

# from sklearn.linear_model import LinearRegression
# from sklearn.metrics import r2_score
# import matplotlib.pyplot as plt
# import pickle
# import io
import boto3

# import re
# import os
# from enum import Enum
from sklearn.base import BaseEstimator
from io import BytesIO

# from utils.version_manager import ModelVersionManager
from utils import utils
from enum import Enum


class ModelType(Enum):
    SIMPLE_LINEAR = "simple_linear"
    LASSO = "lasso"
    RIDGE = "ridge"


def main(branch_name: str, model_name: str, data_source: str) -> None:
    lf = get_training_data(branch_name, data_source)

    # get model - need arg for model type, model location
    # filter - ?
    # filter and exclude - ?
    # create training and test datasets - need default arg for sample size
    # train - output r2 - pass model and training set
    # test - output r2
    # verify r2 - alert if not
    # serialise using version manager
    # alert success and serialisation result
    pass


def get_training_data(
    branch_name: str, data_source: str, s3_client=None
) -> pl.LazyFrame:
    s3_bucket = f"sfc-{branch_name}-datasets"

    if s3_client is not None:
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=s3_bucket, Key=data_source)
        parquet_data = response["Body"].read()
        return pl.read_parquet(BytesIO(parquet_data)).lazy()
    else:
        s3_url = f"s3://{s3_bucket}/{data_source}"
        return pl.scan_parquet(s3_url)


def generate_model(model_type: ModelType) -> BaseEstimator:
    pass


if __name__ == "__main__":
    (branch_name, model_name, data_source) = utils.collect_arguments(
        (
            "--branch_name",
            "The name of the branch currently being used",
        ),
        (
            "--model_name",
            "The name of the model to train",
        ),
        (
            "--data_source",
            "The prefix of the data source to use",
        ),
    )
    main(branch_name, model_name, data_source)
