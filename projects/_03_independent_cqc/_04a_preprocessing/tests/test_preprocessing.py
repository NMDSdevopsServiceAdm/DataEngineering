from projects._03_independent_cqc._04a_preprocessing.fargate.preprocessing import (
    preprocess_non_res_pir,
    pl as pol,
)
import unittest
import os
import boto3
from moto import mock_aws
import polars as pl
from unittest.mock import MagicMock, patch
import shutil
import tempfile


PATCH_STEM = "projects._03_independent_cqc._04a_preprocessing.fargate.preprocessing"
DUMMY_SOURCE_BUCKET = "dummy-source-bucket"
DUMMY_DESTINATION_BUCKET = "dummy-destination-bucket"


class TestPreprocessNonResPir(unittest.TestCase):
    df_test = pl.read_parquet(
        "projects/_03_independent_cqc/_04a_preprocessing/tests/testfile.parquet"
    )
    s3_uri = "s3://test_bucket/test_file.parquet"

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.destination = os.path.join(self.temp_dir, "test_dest.parquet")

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    @patch(f"{PATCH_STEM}.pl.read_parquet")
    def test_preprocess_non_res_pir_reads_dataframe(self, mock_read_parquet):
        preprocess_non_res_pir(self.s3_uri, self.destination)
        mock_read_parquet.assert_called_once_with(self.s3_uri)

    @patch(f"{PATCH_STEM}.pl.scan_parquet")
    def test_preprocess_non_res_pir_reads_lazyframe(self, mock_scan_parquet):
        preprocess_non_res_pir(self.s3_uri, self.destination, lazy=True)
        mock_scan_parquet.assert_called_once_with(self.s3_uri)

    def test_preprocess_non_res_pir_returns_correct_columns_from_read(self):
        with patch(f"{PATCH_STEM}.pl.read_parquet") as mock_read_parquet:
            mock_read_parquet.return_value = self.df_test
            preprocess_non_res_pir(self.s3_uri, self.destination, lazy=False)
        df = pl.read_parquet(self.destination)
        expected_columns = [
            "locationId",
            "cqc_location_import_date",
            "careHome",
            "ascwds_filled_posts_deduplicated_clean",
            "pir_people_directly_employed_deduplicated",
        ]
        self.assertListEqual(df.columns, expected_columns)

    def test_preprocess_non_res_pir_eliminates_nulls(self):
        with patch(f"{PATCH_STEM}.pl.read_parquet") as mock_read_parquet:
            mock_read_parquet.return_value = self.df_test
            preprocess_non_res_pir(self.s3_uri, self.destination, lazy=False)
        df = pl.read_parquet(self.destination)
        self.assertEqual(
            df.filter(pl.col("ascwds_filled_posts_deduplicated_clean").is_null()).shape[
                0
            ],
            0,
        )
        self.assertEqual(
            df.filter(
                pl.col("pir_people_directly_employed_deduplicated").is_null()
            ).shape[0],
            0,
        )

    @patch(f"{PATCH_STEM}.pl.read_parquet")
    def test_preprocess_non_res_pir_eliminates_negatives_or_zeros(
        self, mock_read_parquet
    ):
        pass

    @patch(f"{PATCH_STEM}.pl.read_parquet")
    def test_preprocess_non_res_pir_eliminates_small_residuals(self, mock_read_parquet):
        pass

    @patch(f"{PATCH_STEM}.pl.read_parquet")
    def test_preprocess_non_res_pir_logs_failure(self, mock_read_parquet):
        pass

    @patch(f"{PATCH_STEM}.pl.read_parquet")
    def test_preprocess_non_res_pir_logs_success(self, mock_read_parquet):
        pass

    @patch(f"{PATCH_STEM}.pl.read_parquet")
    def test_non_res_pir_saves_dataframe(self, mock_read_parquet):
        pass
