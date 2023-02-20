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

from utils.prepare_locations_utils.job_calculator.calculate_job_count_return_worker_record_count_if_equal_to_total_staff import (
    calculate_jobcount_totalstaff_equal_wkrrecs,
)


class TestJobCountTotalStaffEqualWorkerRecords(unittest.TestCase):
    calculate_jobs_schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("total_staff", IntegerType(), True),
            StructField("worker_record_count", IntegerType(), True),
            StructField("number_of_beds", IntegerType(), True),
            StructField("job_count", DoubleType(), True),
        ]
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_calculate_totalstaff_equal_worker_records"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_jobcount_totalstaff_equal_wkrrecs_when_greater_than_zero(self):
        rows = [
            ("1-000000001", 20, 20, 25, None),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

        df = calculate_jobcount_totalstaff_equal_wkrrecs(
            df, "total_staff", "worker_record_count", "job_count"
        )
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["job_count"], 20)

    def test_calculate_jobcount_totalstaff_equal_wkrrecs_when_both_are_zero(self):
        rows = [
            ("1-000000001", 0, 0, 25, None),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

        df = calculate_jobcount_totalstaff_equal_wkrrecs(
            df, "total_staff", "worker_record_count", "job_count"
        )
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["job_count"], 0)

    def test_calculate_jobcount_totalstaff_equal_wkrrecs_when_both_are_below_min_cutoff(
        self,
    ):
        rows = [
            ("1-000000001", 1, 1, 25, None),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

        df = calculate_jobcount_totalstaff_equal_wkrrecs(
            df, "total_staff", "worker_record_count", "job_count"
        )
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["job_count"], 1)

    def test_calculate_jobcount_totalstaff_equal_wkrrecs_returns_none_when_not_equal(
        self,
    ):
        rows = [
            ("1-000000001", None, None, 25, None),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

        df = calculate_jobcount_totalstaff_equal_wkrrecs(
            df, "total_staff", "worker_record_count", "job_count"
        )
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["job_count"], None)
