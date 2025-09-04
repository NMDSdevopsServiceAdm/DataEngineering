from projects._03_independent_cqc._05a_model.train_model import (
    get_training_data,
    instantiate_model,
    ModelType,
)
import unittest
import os
import boto3
import polars as pl
from moto import mock_aws
from sklearn.base import BaseEstimator

DATA_BUCKET_NAME = "sfc-test-datasets"
MODEL_BUCKET_NAME = "test_model_bucket"
SOURCE_DATA_PATH = "tests/test_data/sample_parquet/sales1.parquet"
AWS_ENDPOINT_URL = "http://127.0.0.1:5000"


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
        with open(SOURCE_DATA_PATH, "rb") as f:
            self.s3_client.put_object(
                Bucket=DATA_BUCKET_NAME,
                Key="sample_data/sample.parquet",
                Body=f,
            )
        self.s3_client.create_bucket(
            Bucket=MODEL_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        self.ssm_client.put_parameter(
            Name="/model/test/version",
            Value='{"Current Version": "5.6.7"}',
            Type="String",
        )

    def tearDown(self):
        self.mock_aws.stop()

    def test_get_training_data_returns_dataframe(self):
        data = get_training_data("test", "sample_data/sample.parquet")
        self.assertTrue(isinstance(data, pl.LazyFrame))

    def test_get_training_data_has_correct_number_of_rows(self):
        lf = get_training_data(
            "test", "sample_data/sample.parquet", s3_client=self.s3_client
        )
        self.assertEqual(lf.select(pl.len()).collect().item(), 20000)

    def test_instantiate_model_returns_model(self):
        self.assertIsInstance(instantiate_model(ModelType.SIMPLE_LINEAR), BaseEstimator)
