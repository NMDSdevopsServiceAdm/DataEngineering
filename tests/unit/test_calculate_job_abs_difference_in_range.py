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

from utils.prepare_locations_utils.job_calculator.calculate_jobcount_abs_difference_within_range import (
    calculate_jobcount_abs_difference_within_range,
)


class TestJobCountAbsDiffInRange(unittest.TestCase):
    calculate_jobs_schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("total_staff", IntegerType(), True),
            StructField("worker_record_count", IntegerType(), True),
            StructField("number_of_beds", IntegerType(), True),
            StructField("job_count_unfiltered", DoubleType(), True),
        ]
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_calculate_abs_diff_in_range"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_jobcount_abs_difference_within_range(self):
        rows = [
            ("1-000000001", 10, 12, 15, None),
            ("1-000000002", 100, 109, 80, None),
            ("1-000000002", 2, 4, 80, None),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

        df = calculate_jobcount_abs_difference_within_range(
            df, "total_staff", "worker_record_count", "job_count_unfiltered"
        )
        self.assertEqual(df.count(), 3)

        df = df.collect()
        self.assertEqual(df[0]["job_count_unfiltered"], 11)
        self.assertEqual(df[1]["job_count_unfiltered"], 104.5)
        self.assertEqual(df[2]["job_count_unfiltered"], None)
