import tempfile
import shutil
from projects._01_ingest.cqc_api.fargate.delta_download_cqc_providers import (
    main,
    InvalidTimestampArgumentError,
)
import unittest
from unittest.mock import patch
import os
import pathlib
import polars as pl

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.delta_download_cqc_providers"


class TestDeltaDownloadCQCProviders(unittest.TestCase):
    original_environ = {}
    test_environ = {"CQC_SECRET_NAME": "cqc-secret-name", "AWS_REGION": "us-east-1"}

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    @patch(f"{PATCH_PATH}.get_secret")
    @patch(f"{PATCH_PATH}.cqc.get_updated_objects")
    @patch(f"{PATCH_PATH}.SECRET_ID", new="cqc-secret-name")
    @patch(f"{PATCH_PATH}.AWS_REGION", new="us-east-1")
    def test_main_gets_secret(self, mock_objects, mock_get_secret):
        mock_get_secret.return_value = '{"Ocp-Apim-Subscription-Key": "abc1"}'
        mock_objects.return_value = {}
        dest = os.path.join(self.temp_dir, "test.parquet")
        start = "2025-07-20T15:40:23Z"
        end = "2025-07-25T14:23:40Z"
        main(dest, start, end)
        mock_get_secret.assert_called_once_with(
            secret_name="cqc-secret-name", region_name="us-east-1"
        )

    @patch(f"{PATCH_PATH}.get_secret")
    @patch(f"{PATCH_PATH}.cqc.get_updated_objects")
    @patch(f"{PATCH_PATH}.SECRET_ID", new="cqc-secret-name")
    @patch(f"{PATCH_PATH}.AWS_REGION", new="us-east-1")
    def test_main_traps_timestamp_error(self, mock_objects, mock_get_secret):
        mock_get_secret.return_value = '{"Ocp-Apim-Subscription-Key": "abc1"}'
        mock_objects.return_value = {}
        dest = os.path.join(self.temp_dir, "test.parquet")
        start = "2025-07-25T15:40:23Z"
        end = "2025-07-20T14:23:40Z"
        with self.assertRaises(InvalidTimestampArgumentError):
            main(dest, start, end)

    @patch(f"{PATCH_PATH}.utils.uuid")
    @patch(f"{PATCH_PATH}.get_secret")
    @patch(f"{PATCH_PATH}.cqc.get_updated_objects")
    @patch(f"{PATCH_PATH}.SECRET_ID", new="cqc-secret-name")
    @patch(f"{PATCH_PATH}.AWS_REGION", new="us-east-1")
    def test_main_writes_parquet(self, mock_objects, mock_get_secret, mock_uuid):
        mock_get_secret.return_value = '{"Ocp-Apim-Subscription-Key": "abc1"}'
        mock_objects.return_value = [
            {"providerId": 1},
            {"providerId": 2},
            {"providerId": 3},
        ]
        file_name = "abc"
        mock_uuid.uuid4.return_value = file_name
        dest = f"{self.temp_dir}/{file_name}.parquet"
        start = "2025-07-20T15:40:23Z"
        end = "2025-07-25T14:23:40Z"
        main(self.temp_dir + "/", start, end)
        self.assertTrue(pathlib.Path(dest).exists())
        self.assertTrue(pathlib.Path(dest).is_file())
        self.assertTrue(pathlib.Path(dest).suffix == ".parquet")
        result = pl.read_parquet(dest)
        self.assertEqual(result.height, 3)

    @patch(f"{PATCH_PATH}.utils.uuid")
    @patch(f"{PATCH_PATH}.get_secret")
    @patch(f"{PATCH_PATH}.cqc.get_updated_objects")
    @patch(f"{PATCH_PATH}.SECRET_ID", new="cqc-secret-name")
    @patch(f"{PATCH_PATH}.AWS_REGION", new="us-east-1")
    def test_main_copes_with_malformed_destination(
        self, mock_objects, mock_get_secret, mock_uuid
    ):
        mock_get_secret.return_value = '{"Ocp-Apim-Subscription-Key": "abc1"}'
        mock_objects.return_value = [
            {"providerId": 1},
            {"providerId": 2},
            {"providerId": 3},
        ]
        file_name = "abc"
        mock_uuid.uuid4.return_value = file_name
        dest = f"{self.temp_dir}/new_path/{file_name}.parquet"
        start = "2025-07-20T15:40:23Z"
        end = "2025-07-25T14:23:40Z"
        new_path = f"{self.temp_dir}/new_path"
        os.mkdir(new_path)
        main(new_path, start, end)
        self.assertTrue(pathlib.Path(dest).exists())
        self.assertTrue(pathlib.Path(dest).is_file())
        self.assertTrue(pathlib.Path(dest).suffix == ".parquet")
        result = pl.read_parquet(dest)
        self.assertEqual(result.height, 3)

    @patch(f"{PATCH_PATH}.utils.uuid")
    @patch(f"{PATCH_PATH}.get_secret")
    @patch(f"{PATCH_PATH}.cqc.get_updated_objects")
    @patch(f"{PATCH_PATH}.SECRET_ID", new="cqc-secret-name")
    @patch(f"{PATCH_PATH}.AWS_REGION", new="us-east-1")
    def test_main_writes_copes_with_multiple_runs(
        self, mock_objects, mock_get_secret, mock_uuid
    ):
        mock_get_secret.return_value = '{"Ocp-Apim-Subscription-Key": "abc1"}'
        mock_objects.side_effect = [
            [
                {"providerId": 1},
                {"providerId": 2},
                {"providerId": 3},
            ],
            [
                {"providerId": 4},
                {"providerId": 5},
                {"providerId": 6},
            ],
        ]
        uuids = ["abc", "def"]
        mock_uuid.uuid4.side_effect = uuids
        start = "2025-07-20T09:40:23Z"
        middle = "2025-07-20T12:23:40Z"
        end = "2025-07-20T19:40:23Z"

        expected_paths = [f"{self.temp_dir}/{uuid}.parquet" for uuid in uuids]

        main(self.temp_dir + "/", start, middle)
        main(self.temp_dir + "/", middle, end)

        self.assertTrue(pathlib.Path(expected_paths[0]).exists())
        self.assertTrue(pathlib.Path(expected_paths[1]).exists())
        self.assertTrue(pathlib.Path(expected_paths[0]).is_file())
        self.assertTrue(pathlib.Path(expected_paths[1]).is_file())
        self.assertTrue(pathlib.Path(expected_paths[0]).suffix == ".parquet")
        self.assertTrue(pathlib.Path(expected_paths[1]).suffix == ".parquet")

        result_a = pl.read_parquet(expected_paths[0])
        self.assertEqual(result_a["providerId"].str.to_integer().sum(), 6)
        result_b = pl.read_parquet(expected_paths[1])
        self.assertEqual(result_b["providerId"].str.to_integer().sum(), 15)
