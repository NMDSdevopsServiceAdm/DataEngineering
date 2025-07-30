import tempfile
import shutil
from projects._01_ingest.cqc_api.fargate.delta_download_cqc_providers import main
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
        with self.assertRaises(ValueError):
            main(dest, start, end)

    @patch(f"{PATCH_PATH}.get_secret")
    @patch(f"{PATCH_PATH}.cqc.get_updated_objects")
    @patch(f"{PATCH_PATH}.SECRET_ID", new="cqc-secret-name")
    @patch(f"{PATCH_PATH}.AWS_REGION", new="us-east-1")
    def test_main_writes_parquet(self, mock_objects, mock_get_secret):
        mock_get_secret.return_value = '{"Ocp-Apim-Subscription-Key": "abc1"}'
        mock_objects.return_value = [
            {"providerId": 1},
            {"providerId": 2},
            {"providerId": 3},
        ]
        dest = os.path.join(self.temp_dir, "test.parquet")
        start = "2025-07-20T15:40:23Z"
        end = "2025-07-25T14:23:40Z"
        main(dest, start, end)
        self.assertTrue(pathlib.Path(dest).exists())
        self.assertTrue(pathlib.Path(dest).is_file())
        self.assertTrue(pathlib.Path(dest).suffix == ".parquet")
        result = pl.read_parquet(dest)
        self.assertEqual(result.height, 3)
