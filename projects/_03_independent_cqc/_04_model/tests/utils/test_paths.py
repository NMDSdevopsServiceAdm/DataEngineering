import unittest

from projects._03_independent_cqc._04_model.utils import paths as job

DATASETS_BUCKET = "sfc-test-datasets"
MODEL = "model_a"
VERSION = "1.0.0"


class GenerateModelPathTests(unittest.TestCase):
    def test_returns_expected_path(self):
        returned_path = job.generate_model_path(DATASETS_BUCKET, MODEL, VERSION)
        expected_path = "s3://sfc-test-pipeline-resources/models/model_a/1.0.0/"
        self.assertEqual(returned_path, expected_path)


class GenerateIndCqcPathTests(unittest.TestCase):
    def test_returns_expected_path(self):
        returned_path = job.generate_ind_cqc_path(DATASETS_BUCKET)
        expected_path = "s3://sfc-test-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_03_imputed_ascwds_and_pir/"
        self.assertEqual(returned_path, expected_path)


class GenerateFeaturesPathTests(unittest.TestCase):
    def test_returns_expected_path(self):
        returned_path = job.generate_features_path(DATASETS_BUCKET, MODEL)
        expected_path = "s3://sfc-test-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_04_features_model_a_polars/"
        self.assertEqual(returned_path, expected_path)


class GeneratePredictionsPathTests(unittest.TestCase):
    def test_returns_expected_path(self):
        returned_path = job.generate_predictions_path(DATASETS_BUCKET, MODEL)
        expected_path = "s3://sfc-test-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_04_predictions_model_a/predictions.parquet"
        self.assertEqual(returned_path, expected_path)
