import unittest

from projects._03_independent_cqc._04_model.utils import paths as job

RESOURCES_BUCKET = "sfc-test-pipeline-resources"
DATASETS_BUCKET = "sfc-test-datasets"
MODEL = "model_a"
VERSION = "1.0.0"


class ModelPathTests(unittest.TestCase):
    def test_returns_expected_path(self):
        returned_path = job.model_path(RESOURCES_BUCKET, MODEL, VERSION)
        expected_path = "s3://sfc-test-pipeline-resources/models/model_a/1.0.0/"
        self.assertEqual(returned_path, expected_path)


class FeaturesPathTests(unittest.TestCase):
    def test_returns_expected_path(self):
        returned_path = job.features_path(DATASETS_BUCKET, MODEL)
        expected_path = "s3://sfc-test-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_04_features_model_a/"
        self.assertEqual(returned_path, expected_path)


class PredictionsPathTests(unittest.TestCase):
    def test_returns_expected_path(self):
        returned_path = job.predictions_path(DATASETS_BUCKET, MODEL)
        expected_path = "s3://sfc-test-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_04_predictions_model_a/"
        self.assertEqual(returned_path, expected_path)
