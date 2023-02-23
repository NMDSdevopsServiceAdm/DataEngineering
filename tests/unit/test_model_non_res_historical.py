import unittest
import warnings

from pyspark.sql import SparkSession

from tests.test_helpers import remove_file_path
from utils.estimate_job_count.models.non_res_historical import (
    model_non_res_historical,
)


class EstimateJobCountTests(unittest.TestCase):
    CAREHOME_MODEL = (
        "tests/test_models/care_home_with_nursing_historical_jobs_prediction/"
    )
    NON_RES_WITH_PIR_MODEL = (
        "tests/test_models/non_residential_with_pir_jobs_prediction/"
    )
    METRICS_DESTINATION = "tests/test_data/tmp/data_engineering/model_metrics/"
    PREPARED_LOCATIONS_DIR = "tests/test_data/tmp/prepared_locations/"
    LOCATIONS_FEATURES_DIR = "tests/test_data/tmp/location_features/"
    DESTINATION = "tests/test_data/tmp/estimated_job_counts/"

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_estimate_2021_jobs"
        ).getOrCreate()
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def tearDown(self):
        remove_file_path(self.PREPARED_LOCATIONS_DIR)
        remove_file_path(self.LOCATIONS_FEATURES_DIR)
        remove_file_path(self.DESTINATION)
        remove_file_path(self.METRICS_DESTINATION)

    def test_model_non_res_historical(self):
        columns = [
            "locationid",
            "primary_service_type",
            "last_known_job_count",
            "estimate_job_count",
            "estimate_job_count_source",
        ]
        rows = [
            ("1-000000001", "non-residential", 10, None, None),
            ("1-000000002", "Care home with nursing", 10, None, None),
            ("1-000000003", "non-residential", 20, None, None),
            ("1-000000004", "non-residential", 10, 10, "already_populated"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = model_non_res_historical(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["estimate_job_count"], 10.3)
        self.assertEqual(
            df[0]["estimate_job_count_source"], "model_non_res_ascwds_projected_forward"
        )
        self.assertEqual(df[0]["model_non_res_historical"], 10.3)
        self.assertEqual(df[1]["estimate_job_count"], None)
        self.assertEqual(df[1]["estimate_job_count_source"], None)
        self.assertEqual(df[1]["model_non_res_historical"], None)
        self.assertEqual(df[2]["estimate_job_count"], 20.6)
        self.assertEqual(df[2]["estimate_job_count_source"], "model_non_res_ascwds_projected_forward")
        self.assertEqual(df[2]["model_non_res_historical"], 20.6)
        self.assertEqual(df[3]["estimate_job_count"], 10)
        self.assertEqual(df[3]["estimate_job_count_source"], "already_populated")
        self.assertEqual(df[3]["model_non_res_historical"], 10.3)
