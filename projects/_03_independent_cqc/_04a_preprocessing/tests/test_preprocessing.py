from projects._03_independent_cqc._04a_preprocessing.fargate.preprocessing import (
    NonResPIRPreprocessor,
)
import unittest
import os
import boto3
from moto import mock_aws


DUMMY_SOURCE_BUCKET = 'dummy-source-bucket'
DUMMY_DESTINATION_BUCKET = 'dummy-destination-bucket'

class TestPreprocessNonResPir(unittest.TestCase):

    def setUp(self):
        self.current_env = {
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", None),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", None),
            "AWS_SECURITY_TOKEN": os.environ.get("AWS_SECURITY_TOKEN", None),
            "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN", None),
            "AWS_REGION": os.environ.get("AWS_REGION", None),
        }
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_REGION"] = "eu-west-2"
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        self.s3_client = boto3.client("s3", region_name="eu-west-2")
        self.s3_client.create_bucket(
            Bucket=DUMMY_SOURCE_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        self.s3_client.create_bucket(Bucket=DUMMY_DESTINATION_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},)
        self.s3_client.put_object(Bucket=DUMMY_DESTINATION_BUCKET, Key='data_source.parquet', Body='testfile.parquet')

    def tearDown(self):
        self.mock_aws.stop()
        self.reset_env("AWS_ACCESS_KEY_ID")
        self.reset_env("AWS_SECRET_ACCESS_KEY")
        self.reset_env("AWS_SECURITY_TOKEN")
        self.reset_env("AWS_SESSION_TOKEN")
        self.reset_env("AWS_REGION")

    def reset_env(self, key):
        if self.current_env[key] is None:
            del os.environ[key]
        else:
            os.environ[key] = self.current_env[key]

    def test_preprocess_non_res_pir_reads_data(self):


    def test_non_res_pir_saves_to_parquet(self):
        pass

    def test_preprocess_non_res_pir_returns_correct_columns(self):
        pass

    def test_preprocess_non_res_pir_eliminates_nulls(self):
        pass

    def test_preprocess_non_res_pir_eliminates_negatives_or_zeros(self):
        pass

    def test_preprocess_non_res_pir_eliminates_small_residuals(self):
        pass

    def test_preprocess_non_res_pir_logs_failure(self):
        pass

    def test_preprocess_non_res_pir_logs_success(self):
        pass
