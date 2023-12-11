import unittest
import warnings
from datetime import datetime, date
import re
import os
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

from tests.test_file_generator import generate_prepared_locations_file_parquet
from tests.test_helpers import remove_file_path
import jobs.create_job_estimates_diagnostics as job


class CreateJobEstimatesDiagnosticsTests(unittest.TestCase):
    ESTIMATED_JOB_COUNTS = "tests/test_data/tmp/estimated_job_counts/"
    CAPACITY_TRACKER_CARE_HOME_DATA = (
        "tests/test_data/tmp/capacity_tracker_care_home_data/"
    )
    CAPACITY_TRACKER_NON_RESIDENTIAL_DATA = (
        "tests/test_data/tmp/capacity_tracker_non_residential_data/"
    )
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


@unittest.skip("not written yet")
def test_create_job_estimates_diagnostics_completes(self):
    pass


@unittest.skip("not written yet")
def test_test_merge_dataframes_does_not_add_additional_rows(self):
    pass


@unittest.skip("not written yet")
def test_add_catagorisation_column_adds_ascwds_known_when_data_is_in_ascwds(self):
    pass


@unittest.skip("not written yet")
def test_add_catagorisation_column_adds_externally_known_when_data_is_in_capacity_tracker_or_pir(
    self,
):
    pass


@unittest.skip("not written yet")
def test_add_catagorisation_column_adds_unknown_when_no_comparison_data_is_available(
    self,
):
    pass


@unittest.skip("not written yet")
def test_calculate_residuals_adds_a_column(self):
    pass


@unittest.skip("not written yet")
def test_calculate_residuals_adds_residual_value_when_known_data_is_available(self):
    pass


@unittest.skip("not written yet")
def test_calculate_residuals_does_not_add_residual_value_when_known_data_is_unkown(
    self,
):
    pass


@unittest.skip("not written yet")
def test_calculate_average_residual_adds_column_with_average_residual(self):
    pass


if __name__ == "__main__":
    unittest.main(warnings="ignore")
