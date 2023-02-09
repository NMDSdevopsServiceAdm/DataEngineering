import unittest
import warnings

from pyspark.sql import SparkSession

from utils.prepare_locations_utils.job_calculator.job_calculator import (
    calculate_jobcount,
)


class TestJobCalculator(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_job_calculator").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_jobcount(self):
        columns = ["locationid", "worker_record_count", "total_staff", "number_of_beds"]
        rows = [
            ("1-000000001", None, 0, 0),  # Both 0: Return 0
            # Both 500: Return 500
            ("1-000000002", 500, 500, 490),
            # Only know worker_record_count: Return worker_record_count (100)
            ("1-000000003", 100, None, 10),
            # Only know total_staff: Return totalstaf (10)
            ("1-000000004", None, 10, 12),
            # None of the rules apply: Return None
            ("1-000000005", 25, 75, 40),
            # None of the rules apply: Return None
            ("1-000000006", 30, 60, 40),
            # None of the rules apply: Return None
            ("1-000000007", 600, 900, 150),
            # Absolute difference is within 10%: Return Average
            ("1-000000008", 10, 12, None),
            # Either total_staff or worker_record_count < 3: return max
            ("1-000000009", 1, 23, None),
            # Utilise bedcount estimate - Average
            ("1-000000010", 90, 102, 85),
            # Utilise bedcount estimate - Wkrrecs
            ("1-000000011", 90, 102, 95),
            # Utilise bedcount estimate - Totalstaff
            ("1-000000012", 90, 102, 80),
        ]
        df = self.spark.createDataFrame(rows, columns)

        jobcount_df = calculate_jobcount(df)
        jobcount_df_list = jobcount_df.collect()

        self.assertEqual(jobcount_df_list[0]["job_count"], 0.0)
        self.assertEqual(
            jobcount_df_list[0]["job_count_source"], "coalesce totalstaff worker records"
        )

        self.assertEqual(jobcount_df_list[1]["job_count"], 500.0)
        self.assertEqual(jobcount_df_list[1]["job_count_source"], None)

        self.assertEqual(jobcount_df_list[2]["job_count"], 100.0)
        self.assertEqual(
            jobcount_df_list[2]["job_count_source"], "coalesce totalstaff worker records"
        )

        self.assertEqual(jobcount_df_list[3]["job_count"], 10.0)
        self.assertEqual(
            jobcount_df_list[3]["job_count_source"], "coalesce totalstaff worker records"
        )

        self.assertEqual(jobcount_df_list[4]["job_count"], None)
        self.assertEqual(jobcount_df_list[4]["job_count_source"], None)

        self.assertEqual(jobcount_df_list[5]["job_count"], None)
        self.assertEqual(jobcount_df_list[5]["job_count_source"], None)

        self.assertEqual(jobcount_df_list[6]["job_count"], None)
        self.assertEqual(jobcount_df_list[6]["job_count_source"], None)

        self.assertEqual(jobcount_df_list[7]["job_count"], 11.0)
        self.assertEqual(jobcount_df_list[7]["job_count_source"], None)

        self.assertEqual(jobcount_df_list[8]["job_count"], 23.0)
        self.assertEqual(jobcount_df_list[8]["job_count_source"], None)

        self.assertEqual(jobcount_df_list[9]["job_count"], 96.0)
        self.assertEqual(jobcount_df_list[9]["job_count_source"], None)

        self.assertEqual(jobcount_df_list[10]["job_count"], 102.0)
        self.assertEqual(jobcount_df_list[10]["job_count_source"], None)

        self.assertEqual(jobcount_df_list[11]["job_count"], 90.0)
        self.assertEqual(jobcount_df_list[11]["job_count_source"], None)
