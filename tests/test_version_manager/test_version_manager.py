from unittest.mock import Mock, patch
from utils.version_manager import ModelVersionManager, EnumChangeType, BaseEstimator
import unittest
import os
from moto import mock_aws
import boto3
from botocore.exceptions import ClientError
import io
import pickle

PATCH_STEM = "utils.version_manager.ModelVersionManager"

DUMMY_BUCKET_NAME = "my-model-bucket"


class DummyModel(BaseEstimator):
    def __init__(self, name: str, param1: int, param2: int):
        self.name = name
        self.param1 = param1
        self.param2 = param2

    def fit(self, *args, **kwargs):
        pass


class TestVersionManager(unittest.TestCase):
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
        self.glue_client = boto3.client("glue", region_name="eu-west-2")
        self.s3_client.create_bucket(
            Bucket=DUMMY_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        if self.make_ssm_param:
            self.ssm_client.put_parameter(
                Name="/model/test/version",
                Value='{"Current Version": "5.6.7"}',
                Type="String",
            )
        self.version_manager = ModelVersionManager(
            s3_bucket=DUMMY_BUCKET_NAME,
            s3_prefix="model/test/version",
            param_store_name="/model/test/version",
        )

    def tearDown(self):
        self.mock_aws.stop()

    def test_increment_major(self):
        self.assertEqual(
            self.version_manager.increment_version("1.2.3", EnumChangeType.MAJOR),
            "2.0.0",
        )

    def test_increment_minor(self):
        self.assertEqual(
            self.version_manager.increment_version("1.2.3", EnumChangeType.MAJOR),
            "2.0.0",
        )

    def test_increment_patch(self):
        self.assertEqual(
            self.version_manager.increment_version("1.2.3", EnumChangeType.PATCH),
            "1.2.4",
        )

    def test_invalid_change_type(self):
        with self.assertRaises(ValueError):
            self.version_manager.increment_version("1.0.0", 4)

    def test_get_current_version_gets_data_from_ssm(self):
        self.assertEqual(self.version_manager.get_current_version(), "5.6.7")

    def test_get_current_version_raises_error_if_no_parameter(self):
        self.tearDown()
        self.make_ssm_param = False
        self.setUp()
        self.version_manager.param_store_name = "model/new/version"
        with self.assertRaises(ClientError):
            self.version_manager.get_current_version()

    @patch(f"{PATCH_STEM}.get_current_version", return_value="1.2.3")
    def test_get_new_version_given_existing(self, mock_get_current):
        new_version = self.version_manager.get_new_version(EnumChangeType.MINOR)
        self.assertEqual(new_version, "1.3.0")

    @patch(f"{PATCH_STEM}.get_current_version")
    def test_get_new_version_given_no_existing(self, mock_get_current):
        self.version_manager.param_store_name = "model/new/version"

        def raise_error():
            raise self.version_manager.ssm_client.exceptions.ParameterNotFound(
                {}, "test"
            )

        mock_get_current.side_effect = raise_error
        new_version = self.version_manager.get_new_version(EnumChangeType.MINOR)
        self.assertEqual(new_version, "0.1.0")
        mock_get_current.assert_called_once()

    @patch("builtins.input", side_effect=["4", "5"])
    def test_prompt_change_when_incorrect_inputs(self, mock_input):
        with self.assertRaises(ValueError):
            result = self.version_manager.prompt_change()
        self.assertEqual(mock_input.call_count, 2)

    @patch("builtins.input", side_effect=["4", "3"])
    def test_prompt_change_when_single_incorrect_input(self, mock_input):
        result = self.version_manager.prompt_change()
        self.assertEqual(result, EnumChangeType.PATCH)
        self.assertEqual(mock_input.call_count, 2)

    def test_save_model_writes_to_s3(self):
        fitted_model = DummyModel(name="clever_model", param1=17, param2=26)
        self.version_manager.save_model(fitted_model, "1.2.3")
        response1 = self.version_manager.s3_client.list_objects_v2(
            Bucket=DUMMY_BUCKET_NAME
        )
        self.assertEqual(response1["KeyCount"], 1)
        self.assertEqual(
            response1["Contents"][0]["Key"], "model/test/version/1.2.3/model.pkl"
        )
        download = io.BytesIO()
        self.version_manager.s3_client.download_fileobj(
            DUMMY_BUCKET_NAME, "model/test/version/1.2.3/model.pkl", download
        )
        download.seek(0)
        loaded_model = pickle.load(download)
        self.assertEqual(loaded_model.name, "clever_model")
        self.assertEqual(loaded_model.param1, 17)
        self.assertEqual(loaded_model.param2, 26)

    def test_update_parameter_store(self):
        self.version_manager.update_parameter_store("7.8.9")
        self.assertEqual(self.version_manager.get_current_version(), "7.8.9")

    @patch("builtins.input", side_effect=["yes", "2"])
    @patch(f"{PATCH_STEM}.get_new_version", return_value="1.3.0")
    @patch(f"{PATCH_STEM}.save_model")
    @patch(f"{PATCH_STEM}.update_parameter_store")
    def test_prompt_and_save_success(
        self, mock_update, mock_save, mock_get_new, mock_input
    ):
        mock_model = Mock()
        self.version_manager.prompt_and_save(mock_model)
        mock_get_new.assert_called_with(EnumChangeType.MINOR)
        mock_save.assert_called_with(mock_model, "1.3.0")
        mock_update.assert_called_with("1.3.0")

    @patch("builtins.input", side_effect=["no"])
    @patch(f"{PATCH_STEM}.get_new_version")
    def test_prompt_and_save_no_save(
        self,
        mock_get_new,
        mock_input,
    ):
        mock_model = Mock()
        self.version_manager.prompt_and_save(mock_model)
        mock_get_new.assert_not_called()

    @patch("builtins.input", side_effect=["yes", "2"])
    def test_puts_new_version_if_none_available(self, mock_input):
        self.tearDown()
        self.make_ssm_param = False
        self.setUp()
        fitted_model = DummyModel(name="clever_model", param1=17, param2=26)
        response1 = self.version_manager.ssm_client.describe_parameters()
        names1 = [p["Name"] for p in response1["Parameters"]]
        self.assertEqual(len(names1), 0)
        self.version_manager.prompt_and_save(fitted_model)
        self.assertEqual(self.version_manager.get_current_version(), "0.1.0")
        response2 = self.version_manager.ssm_client.describe_parameters()
        names2 = [p["Name"] for p in response2["Parameters"]]
        self.assertIn("/model/test/version", names2)
        self.assertEqual(len(names2), 1)

    def test_version_manager_creation_validates_param_name(self):
        with self.assertRaises(ValueError):
            self.version_manager = ModelVersionManager(
                s3_bucket=DUMMY_BUCKET_NAME,
                s3_prefix="/model/test/version",
                param_store_name="model/test/version",
            )

    def test_version_manager_can_create_default_patch_version(self):
        self.version_manager = ModelVersionManager(
            s3_bucket=DUMMY_BUCKET_NAME,
            s3_prefix="model/test/version",
            param_store_name="/model/test/version",
            default_patch=True,
        )
        fitted_model = DummyModel(name="clever_model", param1=17, param2=26)
        response1 = self.version_manager.ssm_client.describe_parameters()
        names1 = [p["Name"] for p in response1["Parameters"]]
        self.assertEqual(len(names1), 1)
        self.assertEqual(self.version_manager.get_current_version(), "5.6.7")
        self.version_manager.prompt_and_save(fitted_model)
        self.assertEqual(self.version_manager.get_current_version(), "5.6.8")
        response2 = self.version_manager.ssm_client.describe_parameters()
        names2 = [p["Name"] for p in response2["Parameters"]]
        self.assertIn("/model/test/version", names2)
        self.assertEqual(len(names2), 1)
