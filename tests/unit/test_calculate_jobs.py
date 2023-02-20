import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

from utils.prepare_locations_utils.job_calculator.job_calculator import (
    calculate_jobcount,
)


class TestJobCalculator(unittest.TestCase):
    calculate_job_count_schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("total_staff", IntegerType(), True),
            StructField("worker_record_count", IntegerType(), True),
            StructField("number_of_beds", IntegerType(), True),
        ]
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_job_calculator").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_jobcount(self):
        rows = [
            ("1-000000001", 0, None, 0),  # Both 0: Return 0
            # Only know worker_record_count: Return worker_record_count (100)
            ("1-000000003", None, 100, 10),
            # Only know total_staff: Return totalstaf (10)
            ("1-000000004", 10, None, 12),
            # None of the rules apply: Return None
            ("1-000000005", 75, 25, 40),
            # None of the rules apply: Return None
            ("1-000000006", 60, 30, 40),
            # None of the rules apply: Return None
            ("1-000000007", 900, 600, 150),
            # Absolute difference is within 10%: Return Average
            ("1-000000008", 12, 10, None),
            # Either total_staff or worker_record_count < 3: return max
            ("1-000000009", 23, 1, None),
            # Utilise bedcount estimate - Average
            ("1-000000010", 102, 90, 85),
            # Utilise bedcount estimate - Wkrrecs
            ("1-000000011", 102, 90, 95),
            # Utilise bedcount estimate - Totalstaff
            ("1-000000012", 102, 90, 80),
        ]
        df = self.spark.createDataFrame(rows, schema=self.calculate_job_count_schema)

        jobcount_df = calculate_jobcount(
            df, "total_staff", "worker_record_count", "job_count"
        )
        jobcount_df_list = jobcount_df.collect()

        self.assertEqual(jobcount_df_list[0]["job_count"], 0.0)
        self.assertEqual(
            jobcount_df_list[0]["job_count_source"], "coalesce_total_staff_wkrrecs"
        )

        self.assertEqual(jobcount_df_list[1]["job_count"], 500.0)
        self.assertEqual(
            jobcount_df_list[1]["job_count_source"],
            "worker_records_equal_to_total_staff",
        )

        self.assertEqual(jobcount_df_list[2]["job_count"], 100.0)
        self.assertEqual(
            jobcount_df_list[2]["job_count_source"], "coalesce_total_staff_wkrrecs"
        )

        self.assertEqual(jobcount_df_list[3]["job_count"], 10.0)
        self.assertEqual(
            jobcount_df_list[3]["job_count_source"], "coalesce_total_staff_wkrrecs"
        )

        self.assertEqual(jobcount_df_list[4]["job_count"], None)
        self.assertEqual(jobcount_df_list[4]["job_count_source"], None)

        self.assertEqual(jobcount_df_list[5]["job_count"], None)
        self.assertEqual(jobcount_df_list[5]["job_count_source"], None)

        self.assertEqual(jobcount_df_list[6]["job_count"], None)
        self.assertEqual(jobcount_df_list[6]["job_count_source"], None)

        self.assertEqual(jobcount_df_list[7]["job_count"], 11.0)
        self.assertEqual(
            jobcount_df_list[7]["job_count_source"], "abs_difference_within_range"
        )

        self.assertEqual(jobcount_df_list[8]["job_count"], 23.0)
        self.assertEqual(jobcount_df_list[8]["job_count_source"], "handle_tiny_values")

        self.assertEqual(jobcount_df_list[9]["job_count"], 96.0)
        self.assertEqual(jobcount_df_list[9]["job_count_source"], "estimate_from_beds")

        self.assertEqual(jobcount_df_list[10]["job_count"], 102.0)
        self.assertEqual(jobcount_df_list[10]["job_count_source"], "estimate_from_beds")

        self.assertEqual(jobcount_df_list[11]["job_count"], 90.0)
        self.assertEqual(jobcount_df_list[11]["job_count_source"], "estimate_from_beds")

    def test_calculate_jobcount_matching_zeros_return_None(self):
        rows = [
            ("1-000000001", 0, 0, 0),
        ]
        df = self.spark.createDataFrame(rows, schema=self.calculate_job_count_schema)

        jobcount_df = calculate_jobcount(
            df, "total_staff", "worker_record_count", "job_count"
        )
        jobcount_df_list = jobcount_df.collect()

        self.assertEqual(jobcount_df_list[0]["job_count"], None)
        self.assertEqual(jobcount_df_list[0]["job_count_source"], None)

    def test_calculate_jobcount_matching_Nones_return_None(self):
        rows = [
            ("1-000000001", None, None, 0),
        ]
        df = self.spark.createDataFrame(rows, schema=self.calculate_job_count_schema)

        jobcount_df = calculate_jobcount(
            df, "total_staff", "worker_record_count", "job_count"
        )
        jobcount_df_list = jobcount_df.collect()

        self.assertEqual(jobcount_df_list[0]["job_count"], None)
        self.assertEqual(jobcount_df_list[0]["job_count_source"], None)

    def test_calculate_jobcount_matching_job_counts_below_minimum_return_None(self):
        rows = [
            ("1-000000001", 2, 2, 0),
        ]
        df = self.spark.createDataFrame(rows, schema=self.calculate_job_count_schema)

        jobcount_df = calculate_jobcount(
            df, "total_staff", "worker_record_count", "job_count"
        )
        jobcount_df_list = jobcount_df.collect()

        self.assertEqual(jobcount_df_list[0]["job_count"], None)
        self.assertEqual(jobcount_df_list[0]["job_count_source"], None)

    def test_calculate_jobcount_matching_job_counts_above_minimum_return_worker_record_count(
        self,
    ):
        rows = [
            ("1-000000001", 50, 50, 0),
        ]
        df = self.spark.createDataFrame(rows, schema=self.calculate_job_count_schema)

        jobcount_df = calculate_jobcount(
            df, "total_staff", "worker_record_count", "job_count"
        )
        jobcount_df_list = jobcount_df.collect()

        self.assertEqual(jobcount_df_list[0]["job_count"], 50.0)
        self.assertEqual(
            jobcount_df_list[0]["job_count_source"],
            "worker_records_equal_to_total_staff",
        )
