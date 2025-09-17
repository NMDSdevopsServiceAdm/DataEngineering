from projects._03_independent_cqc._04a_preprocessing.fargate.preprocessing import (
    preprocess_non_res_pir,
    logger,
    main_preprocessor,
)
import unittest
import os
import polars as pl
from unittest.mock import patch, MagicMock
import shutil
import tempfile
from pathlib import Path
import logging


PATCH_STEM = "projects._03_independent_cqc._04a_preprocessing.fargate.preprocessing"
DUMMY_SOURCE_BUCKET = "dummy-source-bucket"
DUMMY_DESTINATION_BUCKET = "dummy-destination-bucket"
SAMPLE_DATA_PATH = Path(__file__).parent / "testfile.parquet"


class TestPreprocessing(unittest.TestCase):
    def test_main_preprocessor_calls_processor_with_kwargs(self):
        preprocessor = MagicMock()
        kwargs = {"source": "path/a", "destination": "path/b", "a": 1, "b": 2}
        main_preprocessor(preprocessor, **kwargs)
        preprocessor.assert_called_once_with(**kwargs)

    def test_main_preprocessor_logs_errors(self):
        with self.assertLogs(logger.name, level=logging.INFO) as cm:
            with self.assertRaises(ValueError):
                preprocessor = MagicMock()
                preprocessor.__str__.return_value = "my_preprocessor"
                preprocessor.side_effect = ValueError("foo")
                kwargs = {"source": "path/a", "destination": "path/b", "a": 1, "b": 2}
                main_preprocessor(preprocessor, **kwargs)
            self.assertIn("foo", cm.output[1])
            self.assertIn(
                f"There was an unexpected exception while executing preprocessor my_preprocessor.",
                cm.output[0],
            )

    def test_main_preprocessor_requires_correct_signature(self):
        preprocessor = MagicMock()
        kwargs = {"destination": "path/b", "a": 1}
        with self.assertRaises(TypeError):
            main_preprocessor(preprocessor, **kwargs)

    def test_main_preprocessor_requires_valid_source_and_destination(self):
        preprocessor = MagicMock()
        kwargs = {"source": 5, "destination": 6, "a": 1, "b": 2}
        with self.assertRaises(TypeError):
            main_preprocessor(preprocessor, **kwargs)


class TestPreprocessNonResPir(unittest.TestCase):
    df_test = pl.read_parquet(SAMPLE_DATA_PATH)
    s3_uri = "s3://test_bucket/test_file.parquet"

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.destination = os.path.join(self.temp_dir, "test_dest.parquet")
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
        with self.assertLogs(logger.name, level=logging.INFO) as cm:
            with self.assertRaises((pl.exceptions.PolarsError, FileNotFoundError)):
                preprocess_non_res_pir(
                    "my/nonexistent/path", self.destination, lazy=False
                )
            self.assertIn(
                f"Polars was not able to read or process the data in my/nonexistent/path, or send to {self.destination}",
                cm.output[0],
            )
