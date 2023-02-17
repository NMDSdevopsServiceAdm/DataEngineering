from pyspark.sql import SparkSession

from utils.estimate_job_count.models.r2_metric import generate_r2_metric
import unittest
import warnings


class TestGenerateR2Metric(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_estimate_2021_jobs"
        ).getOrCreate()
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def generate_predictions_df(self):
        # fmt: off
        columns = ["locationid", "primary_service_type", "job_count", "carehome", "ons_region", "number_of_beds", "snapshot_date", "prediction"]

        rows = [
            ("1-000000001", "Care home with nursing", 50, "Y", "South West", 67, "2022-03-29", 56.89),
            ("1-000000004", "non-residential", 10, "N", None, 0, "2022-03-29", 12.34),
        ]
        # fmt: on
        return self.spark.createDataFrame(
            rows,
            schema=columns,
        )

    def test_generate_r2_metric(self):
        df = self.generate_predictions_df()
        r2 = generate_r2_metric(df, "prediction", "job_count")

        self.assertAlmostEqual(r2, 0.93, places=2)
