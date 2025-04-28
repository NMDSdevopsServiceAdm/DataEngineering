import unittest
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


class GenerateMetricTests(SaveModelMetricsTests):
    def setUp(self) -> None:
        super().setUp()

        evaluator = RegressionEvaluator(
            predictionCol=IndCqc.prediction, labelCol=IndCqc.imputed_filled_post_model
        )
        generate_metric_df = self.spark.createDataFrame(
            Data.generate_metric_rows, Schemas.generate_metric_schema
        )
        self.r2 = job.generate_metric(evaluator, generate_metric_df, IndCqc.r2)

    def test_generic_metric_returns_float(self):
        self.assertIsInstance(self.r2, float)
