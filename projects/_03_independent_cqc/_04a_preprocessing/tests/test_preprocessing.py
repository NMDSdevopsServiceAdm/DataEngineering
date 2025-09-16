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


PATCH_STEM = "projects._03_independent_cqc._04a_preprocessing.fargate.preprocessing"
DUMMY_SOURCE_BUCKET = "dummy-source-bucket"
DUMMY_DESTINATION_BUCKET = "dummy-destination-bucket"


class TestPreprocessNonResPir(unittest.TestCase):
    df_test = pl.read_parquet(
        "projects/_03_independent_cqc/_04a_preprocessing/tests/testfile.parquet"
    )

    @patch(f"{PATCH_STEM}.pl.read_parquet")
    def test_preprocess_non_res_pir_reads_dataframe(self, mock_read_parquet):
        s3_uri = "s3://test_bucket/test_file.parquet"
        preprocess_non_res_pir(s3_uri)
        mock_read_parquet.assert_called_once_with(s3_uri)

    @patch(f"{PATCH_STEM}.pl.scan_parquet")
    def test_preprocess_non_res_pir_reads_lazyframe(self, mock_scan_parquet):
        s3_uri = "s3://test_bucket/test_file.parquet"
        preprocess_non_res_pir(s3_uri, lazy=True)
        mock_scan_parquet.assert_called_once_with(s3_uri)

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

    def test_non_res_pir_saves_dataframe(self):
        pass
