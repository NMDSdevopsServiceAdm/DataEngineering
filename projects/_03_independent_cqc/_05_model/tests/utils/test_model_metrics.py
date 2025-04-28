import unittest
import warnings

from utils import utils
import projects._03_independent_cqc._05_model.utils.model_metrics as job


PATCH_PATH: str = "projects._03_independent_cqc._05_model.utils.model_metrics"


class SaveModelMetricsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.branch_name: str = "test_branch"
        self.model_name: str = "test_model"
        self.model_version: str = "1.0.0"

        warnings.filterwarnings("ignore", category=ResourceWarning)


class GenerateModelMetricsS3PathTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_generate_model_s3_path_returns_expected_path(self):
        returned_path = job.generate_model_metrics_s3_path(
            self.branch_name, self.model_name, self.model_version
        )
        expected_path = "s3://sfc-test_branch-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_model_metrics/model_name=test_model/model_version=1.0.0/"

        self.assertEqual(returned_path, expected_path)
