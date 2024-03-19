import unittest
import warnings

from utils.estimate_filled_posts.ml_model_metrics import generate_r2_metric
from tests.test_file_data import GenerateRSquaredMetric as Data
from tests.test_file_schemas import GenerateRSquaredMetric as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


class TestGenerateR2Metric(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_generate_r2_metric(self):
        df = self.spark.createDataFrame(
            Data.cqc_ind_cleaned_rows, Schemas.cqc_ind_cleaned_schema
        )
        r2 = generate_r2_metric(
            df, IndCqc.prediction, IndCqc.ascwds_filled_posts_dedup_clean
        )

        self.assertAlmostEqual(r2, 0.93, places=2)
