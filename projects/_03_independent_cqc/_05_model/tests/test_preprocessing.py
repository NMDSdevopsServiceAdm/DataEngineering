import io
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
from botocore.exceptions import ClientError

from projects._03_independent_cqc._05_model.fargate.preprocessing.preprocessing import (
    main_preprocessor,
    preprocess_non_res_pir,
    validate_model_definition,
)
from projects._03_independent_cqc._05_model.utils.model import ModelType

PATCH_STEM = (
    "projects._03_independent_cqc._05_model.fargate.preprocessing.preprocessing"
)
DUMMY_SOURCE_BUCKET = "dummy-source-bucket"
DUMMY_DESTINATION_BUCKET = "dummy-destination-bucket"
SAMPLE_DATA_PATH = Path(__file__).parent / "testfile.parquet"


class DummyProcessor(MagicMock):
    __name__ = "DummyProcessor"


fake_definition = {
    "dummy_model": {
        "preprocessor": "DummyProcessor",
        "preprocessor_kwargs": {"a": 1, "b": 2},
        "model_type": ModelType.SIMPLE_LINEAR.value,
        "model_identifier": "dummy_model",
        "source_prefix": "path/a",
        "processed_location": "path/b",
    }
}


@patch.dict("os.environ", {"S3_SOURCE_BUCKET": DUMMY_SOURCE_BUCKET})
class TestPreprocessing(unittest.TestCase):
    @patch.dict(f"{PATCH_STEM}.model_definitions", fake_definition)
    @patch(f"{PATCH_STEM}.boto3.client")
    def test_main_preprocessor_calls_processor_with_correct_arguments(
        self, mock_boto_client
    ):
        preprocessor = DummyProcessor()
        expected_kwargs = {
            "source": "s3://dummy-source-bucket/path/a",
            "destination": "s3://dummy-source-bucket/path/b",
            "a": 1,
            "b": 2,
        }
        main_preprocessor("dummy_model", preprocessor)
        preprocessor.assert_called_once_with(**expected_kwargs)

    @patch.dict(f"{PATCH_STEM}.model_definitions", fake_definition)
    @patch(f"{PATCH_STEM}.boto3.client")
    def test_main_preprocessor_logs_errors(self, mock_boto_client):
        f = io.StringIO()
        with redirect_stdout(f):
            with self.assertRaises(FileNotFoundError):
                preprocessor = DummyProcessor()
                preprocessor.__str__.return_value = "DummyProcessor at xyz"
                preprocessor.side_effect = FileNotFoundError("foo")
                main_preprocessor("dummy_model", preprocessor)
        output = f.getvalue()
        self.assertIn("foo", output)
        self.assertIn(
            f"ERROR: There was an unexpected exception while executing preprocessor DummyProcessor.",
            output,
        )

    def test_validate_raises_value_error_if_model_id_not_present(self):
        with self.assertRaises(ValueError):
            validate_model_definition("silly_model", fake_definition)

    def test_validate_raises_value_error_if_no_processor_kwargs_present(self):
        with self.assertRaises(ValueError):
            sample_definition = fake_definition.copy()
            sample_definition["dummy_model"].pop("preprocessor_kwargs")
            validate_model_definition("dummy_model", sample_definition)

    def test_validate_raises_value_error_if_no_source_present(self):
        with self.assertRaises(ValueError):
            sample_definition = fake_definition.copy()
            sample_definition["dummy_model"].pop("source_prefix")
            validate_model_definition("dummy_model", sample_definition)

    def test_validate_raises_value_error_if_no_destination_present(self):
        with self.assertRaises(ValueError):
            sample_definition = fake_definition.copy()
            sample_definition["dummy_model"].pop("processed_location")
            validate_model_definition("dummy_model", sample_definition)

    @patch.dict(f"{PATCH_STEM}.model_definitions", fake_definition)
    @patch(f"{PATCH_STEM}.boto3.client")
    def test_main_preprocessor_raises_value_error_on_validation_failures(
        self, mock_boto_client
    ):
        mock_client = MagicMock()
        mock_sender = MagicMock()
        mock_boto_client.return_value = mock_client
        mock_client.send_task_failure = mock_sender
        f = io.StringIO()
        with redirect_stdout(f):
            with self.assertRaises(ValueError):
                preprocessor = DummyProcessor()
                main_preprocessor("silly_model", preprocessor)
        output = f.getvalue()
        self.assertIn("silly_model", output)
        self.assertIn("invalid or missing", output)
        mock_sender.assert_called_once()

    @patch.dict(f"{PATCH_STEM}.model_definitions", fake_definition)
    @patch(f"{PATCH_STEM}.boto3.client")
    def test_main_preprocessor_raises_boto3_client_error_if_failure(
        self, mock_boto_client
    ):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        mock_client.send_task_success.side_effect = ClientError(
            {"Error": {"Message": ""}}, "x"
        )
        f = io.StringIO()
        with redirect_stdout(f):
            with self.assertRaises(ClientError):
                preprocessor = DummyProcessor()
                main_preprocessor("dummy_model", preprocessor)
        output = f.getvalue()
        self.assertIn("StepFunction AWS service", output)


class TestPreprocessNonResPir(unittest.TestCase):
    df_test = pl.read_parquet(SAMPLE_DATA_PATH)
    s3_uri = "s3://test_bucket/test_file.parquet"

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.destination = os.path.join(self.temp_dir, "destination")
        os.mkdir(self.destination)
        with patch(f"{PATCH_STEM}.pl.read_parquet") as mock_read_parquet:
            mock_read_parquet.return_value = self.df_test
            preprocess_non_res_pir(self.s3_uri, self.destination, lazy=False)

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

    def test_preprocess_non_res_pir_eliminates_negatives_or_zeros(self):
        df = pl.read_parquet(self.destination)
        self.assertEqual(
            df.filter(pl.col("ascwds_filled_posts_deduplicated_clean") <= 0).shape[0],
            0,
        )
        self.assertEqual(
            df.filter(pl.col("pir_people_directly_employed_deduplicated") <= 0).shape[
                0
            ],
            0,
        )

    def test_preprocess_non_res_pir_eliminates_small_residuals(self):
        df = pl.read_parquet(self.destination).with_columns(
            (
                pl.col("ascwds_filled_posts_deduplicated_clean")
                - pl.col("pir_people_directly_employed_deduplicated")
            )
            .abs()
            .alias("abs_resid")
        )
        self.assertEqual(df.filter(pl.col("abs_resid") > 500).shape[0], 0)

    def test_preprocess_works_with_lazy_frames(self):
        preprocess_non_res_pir(str(SAMPLE_DATA_PATH), self.destination, lazy=True)
        df = pl.read_parquet(self.destination)
        self.assertEqual(df.shape[0], 3)
        ids = set(df["locationId"].to_list())
        self.assertEqual({"1-119187505", "1-2206520209", "1-118618710"}, ids)

    def test_preprocess_non_res_pir_logs_failure(self):
        f = io.StringIO()
        with redirect_stdout(f):
            with self.assertRaises((pl.exceptions.PolarsError, FileNotFoundError)):
                preprocess_non_res_pir(
                    "my/nonexistent/path", self.destination, lazy=False
                )
        output = f.getvalue()
        self.assertIn(
            f"ERROR: Polars was not able to read or process the data in my/nonexistent/path/, or send to {self.destination}",
            output,
        )
