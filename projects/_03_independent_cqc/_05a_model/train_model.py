import polars as pl

# from sklearn.linear_model import LinearRegression
# from sklearn.metrics import r2_score
# import matplotlib.pyplot as plt
# import pickle
# import io
# import boto3
# import re
# import os
# from enum import Enum
# from sklearn.base import BaseEstimator
# from utils.version_manager import ModelVersionManager
from utils import utils


def main(branch_name: str, model_name: str, data_source: str) -> None:
    # get data - need arg for dataset
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


def get_training_data(branch_name: str, data_source: str) -> pl.DataFrame:
    s3_bucket = f"sfc-{branch_name}-datasets"
    s3_url = f"s3://{s3_bucket}/{data_source}"
    return pl.scan_parquet(s3_url)


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
