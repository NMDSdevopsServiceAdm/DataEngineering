import unittest
import warnings

from pyspark.sql import SparkSession

import utils.estimate_job_count.models.extrapolation as job
from tests.test_file_generator import (
    generate_data_for_extrapolation_model,
)


class TestModelExtrapolation(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_extrapolation").getOrCreate()
        self.extrapolation_df = generate_data_for_extrapolation_model()

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_model_extrapolation_row_count_unchanged(self):
        df = job.model_extrapolation(self.extrapolation_df)
        self.assertEqual(df.count(), self.extrapolation_df.count())
