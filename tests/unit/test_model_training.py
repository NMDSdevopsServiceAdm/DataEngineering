from projects._03_independent_cqc._05a_model.train_model import get_training_data
import unittest
import os
from moto import mock_aws
import boto3
import polars as pl

DATA_BUCKET_NAME = "test_data_bucket"
MODEL_BUCKET_NAME = "test_model_bucket"
SOURCE_DATA_PATH = "tests/test_models/non_res_pir_linear_regression_prediction/1.0.0/data/part-00000-62d5ccd3-b1d7-4a53-aeb7-93c8e53b4864-c000.snappy.parquet"


class TestModelTraining(unittest.TestCase):
    make_ssm_param = True

    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_REGION"] = "eu-west-2"
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        self.s3_client = boto3.client("s3", region_name="eu-west-2")
        self.ssm_client = boto3.client("ssm", region_name="eu-west-2")
        self.s3_client.create_bucket(
            Bucket=DATA_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        self.s3_client.put_object(
            Bucket=DATA_BUCKET_NAME,
            Key="sample_data/sample.parquet",
            Body=SOURCE_DATA_PATH,
        )
        self.s3_client.create_bucket(
            Bucket=MODEL_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        if self.make_ssm_param:
            self.ssm_client.put_parameter(
                Name="/model/test/version",
                Value='{"Current Version": "5.6.7"}',
                Type="String",
            )

    def test_get_training_data_returns_dataframe(self):
        data = get_training_data("test", "sample_data/sample.parquet")
        assert isinstance(data, pl.LazyFrame)
