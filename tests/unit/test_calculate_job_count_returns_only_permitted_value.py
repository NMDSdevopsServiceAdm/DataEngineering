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

from utils.prepare_locations_utils.job_calculator.calculate_job_count_return_only_permitted_value import (
    calculate_jobcount_select_only_value_which_is_at_least_minimum_job_count_permitted,
)


class TestJobCountCoalesceWorkerRecords(unittest.TestCase):
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
            "test_calculate_coalesce"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_job_count_returns_total_staff_when_worker_records_below_permitted(
        self,
    ):
        rows = [
            ("1-000000001", None, 20, 25, None),
            ("1-000000002", 30, None, 25, None),
            ("1-000000003", 1, 20, 25, None),
            ("1-000000004", 30, 1, 25, None),
            ("1-000000005", 35, 40, 25, None),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

        df = calculate_jobcount_select_only_value_which_is_at_least_minimum_job_count_permitted(
            df, "total_staff", "worker_record_count", "job_count_unfiltered"
        )
        self.assertEqual(df.count(), 5)

        df = df.collect()

        self.assertEqual(df[0]["job_count_unfiltered"], None)
        self.assertEqual(df[1]["job_count_unfiltered"], 30)
        self.assertEqual(df[2]["job_count_unfiltered"], None)
        self.assertEqual(df[3]["job_count_unfiltered"], 30)
        self.assertEqual(df[4]["job_count_unfiltered"], None)

    def test_calculate_job_count_returns_worker_records_when_total_staff_below_permitted(
        self,
    ):
        rows = [
            ("1-000000001", None, 20, 25, None),
            ("1-000000002", 30, None, 25, None),
            ("1-000000003", 1, 20, 25, None),
            ("1-000000004", 30, 1, 25, None),
            ("1-000000005", 35, 40, 25, None),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

        df = calculate_jobcount_select_only_value_which_is_at_least_minimum_job_count_permitted(
            df, "worker_record_count", "total_staff", "job_count_unfiltered"
        )
        self.assertEqual(df.count(), 5)

        df = df.collect()
        self.assertEqual(df[0]["job_count_unfiltered"], 20)
        self.assertEqual(df[1]["job_count_unfiltered"], None)
        self.assertEqual(df[2]["job_count_unfiltered"], 20)
        self.assertEqual(df[3]["job_count_unfiltered"], None)
        self.assertEqual(df[4]["job_count_unfiltered"], None)
