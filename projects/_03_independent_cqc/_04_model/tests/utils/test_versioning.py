import json
import unittest

import boto3
from moto import mock_aws

from projects._03_independent_cqc._04_model.utils import versioning as job


class GetRunNumberTests(unittest.TestCase):
    @mock_aws
    def test_get_run_number_returns_zero_when_no_runs(self):
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket="pipeline-resources",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_root = "s3://pipeline-resources/models/model_A/"

        run_number = job.get_run_number(s3_root)

        self.assertEqual(run_number, 0)

    @mock_aws
    def test_get_run_number_detects_existing_runs(self):
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket="pipeline-resources",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # simulate existing runs
        s3.put_object(
            Bucket="pipeline-resources",
            Key="models/model_A/1/metadata.json",
            Body=b"{}",
        )
        s3.put_object(
            Bucket="pipeline-resources",
            Key="models/model_A/3/metadata.json",
            Body=b"{}",
        )

        s3_root = "s3://pipeline-resources/models/model_A/"

        run_number = job.get_run_number(s3_root)

        self.assertEqual(run_number, 3)


class SaveMetadataTests(unittest.TestCase):
    @mock_aws
    def test_save_metadata_creates_metadata_file(self):
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket="pipeline-resources",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        s3_root = "s3://pipeline-resources/models/model_A/"

        metadata = {"metric": 0.99}
        job.save_metadata(s3_root, run_number=5, metadata=metadata)

        # Read back the object
        response = s3.get_object(
            Bucket="pipeline-resources", Key="models/model_A/5/metadata.json"
        )
        body = json.loads(response["Body"].read().decode("utf-8"))

        self.assertIn("timestamp", body)
        self.assertEqual(body["metric"], 0.99)
