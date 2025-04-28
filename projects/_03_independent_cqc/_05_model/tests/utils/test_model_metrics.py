import unittest
from unittest.mock import patch, MagicMock, Mock, ANY
import warnings
from pyspark.ml.evaluation import RegressionEvaluator

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
import projects._03_independent_cqc._05_model.utils.model_metrics as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelMetrics as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelMetrics as Schemas,
)

PATCH_PATH: str = "projects._03_independent_cqc._05_model.utils.model_metrics"


class SaveModelMetricsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
