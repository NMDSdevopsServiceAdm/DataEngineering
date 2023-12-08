import unittest
import warnings
from datetime import datetime, date
import re
import os
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

from tests.test_file_generator import generate_prepared_locations_file_parquet
from tests.test_helpers import remove_file_path
import jobs.create_job_estimates_diagnostics as job


class CreateJobEstimatesDiagnosticsTests(unittest.TestCase):
    ESTIMATED_JOB_COUNTS = "tests/test_data/tmp/estimated_job_counts/"
    CAPACITY_TRACKER_CARE_HOME_DATA= "tests/test_data/tmp/capacity_tracker_care_home_data/"
    CAPACITY_TRACKER_NON_RESIDENTIAL_DATA= "tests/test_data/tmp/capacity_tracker_non_residential_data/"
    PIR_DATA = "tests/test_data/tmp/pir_data/"
    DIAGNOSTICS_DESTINATION = "tests/test_data/tmp/diagnostics/"
    RESIDUALS_DESTINATION = "tests/test_data/tmp/residuals/"

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_create_job_estimates_diagnostics"
        ).getOrCreate()
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def tearDown(self):
        remove_file_path(self.ESTIMATED_JOB_COUNTS)
        remove_file_path(self.CAPACITY_TRACKER_CARE_HOME_DATA)
        remove_file_path(self.CAPACITY_TRACKER_NON_RESIDENTIAL_DATA)
        remove_file_path(self.PIR_DATA)
        remove_file_path(self.DIAGNOSTICS_DESTINATION)
        remove_file_path(self.RESIDUALS_DESTINATION)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
