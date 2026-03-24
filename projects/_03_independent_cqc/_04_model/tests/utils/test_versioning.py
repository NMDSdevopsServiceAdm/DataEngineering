import io
import json
import unittest

import boto3
import joblib
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


class SaveModelAndMetadataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.s3_root = "s3://pipeline-resources/models/model_A/"
        self.test_model = {"coef": [1.0, 2.0, 3.0]}

    @mock_aws
    def test_creates_metadata_file(self):
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket="pipeline-resources",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        metadata = {"metric": 0.99}

        job.save_model_and_metadata(
            self.s3_root, run_number=5, model=self.test_model, metadata=metadata
        )

        # Read back the object
        response = s3.get_object(
            Bucket="pipeline-resources", Key="models/model_A/5/metadata.json"
        )
        body = json.loads(response["Body"].read().decode("utf-8"))

        self.assertIn("timestamp", body)
        self.assertEqual(body["metric"], 0.99)
        self.assertEqual(body["run_number"], 5)

    @mock_aws
    def test_creates_model_file(self):
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket="pipeline-resources",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        job.save_model_and_metadata(
            s3_root=self.s3_root,
            run_number=7,
            model=self.test_model,
            metadata={},
        )

        response = s3.get_object(
            Bucket="pipeline-resources",
            Key="models/model_A/7/model.pkl",
        )

        model_bytes = response["Body"].read()
        loaded_model = joblib.load(io.BytesIO(model_bytes))

        self.assertEqual(loaded_model, self.test_model)

    @mock_aws
    def test_objects_are_saved_under_correct_run_prefix(self):
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket="pipeline-resources",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        job.save_model_and_metadata(
            s3_root=self.s3_root,
            run_number=42,
            model=self.test_model,
            metadata={},
        )

        objects = s3.list_objects_v2(
            Bucket="pipeline-resources",
            Prefix="models/model_A/42/",
        )

        keys = {obj["Key"] for obj in objects.get("Contents", [])}

        self.assertEqual(
            keys,
            {
                "models/model_A/42/model.pkl",
                "models/model_A/42/metadata.json",
            },
        )


class LoadModelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.s3_root = "s3://pipeline-resources/models/model_A/"
        self.run_number = 7
        self.test_model = {"coef": [1.0, 2.0, 3.0]}

    @mock_aws
    def test_loads_model_for_given_run_number(self):
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket="pipeline-resources",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        buffer = io.BytesIO()
        joblib.dump(self.test_model, buffer)
        buffer.seek(0)

        s3.put_object(
            Bucket="pipeline-resources",
            Key="models/model_A/7/model.pkl",
            Body=buffer.read(),
        )

        loaded_model = job.load_model(
            s3_root=self.s3_root,
            run_number=self.run_number,
        )

        self.assertEqual(loaded_model, self.test_model)
