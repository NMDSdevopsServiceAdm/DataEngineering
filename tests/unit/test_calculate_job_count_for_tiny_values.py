import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from utils.prepare_locations_utils.job_calculator.calculate_job_count_for_tiny_values import (
    calculate_jobcount_handle_tiny_values,
)


class TestJobCountTinyValues(unittest.TestCase):
    calculate_jobs_schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("total_staff", IntegerType(), True),
            StructField("worker_record_count", IntegerType(), True),
            StructField("number_of_beds", IntegerType(), True),
            StructField("job_count", DoubleType(), True),
            StructField("job_count_source", StringType(), True),
        ]
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_ahndle_tiny_values"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_jobcount_handle_tiny_values(self):
        rows = [
            ("1-000000008", 2, 53, 26, None, None),
            ("1-000000008", 55, 53, 26, None, None),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

        df = calculate_jobcount_handle_tiny_values(df)
        self.assertEqual(df.count(), 2)

        df = df.collect()

        self.assertEqual(df[0]["job_count"], 53)
        self.assertEqual(df[0]["job_count_source"], "Tiny Values")

        self.assertEqual(df[1]["job_count"], None)
        self.assertEqual(df[1]["job_count_source"], None)
