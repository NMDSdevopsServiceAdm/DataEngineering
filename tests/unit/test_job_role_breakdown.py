import unittest
from pathlib import Path
from pyspark.sql import SparkSession
from jobs import job_role_breakdown
from tests.test_file_generator import (
    generate_estimate_jobs_2021_parquet,
    generate_worker_parquet,
)
import shutil


class JobRoleBreakdownTests(unittest.TestCase):

    TEST_JOB_ESTIMATES_FILE = "tests/test_data/tmp/job_estimates_2021.parquet"

    TEST_WORKER_FILE = "tests/test_data/tmp/worker_file.parquet"

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_job_role_breakdown"
        ).getOrCreate()
        generate_estimate_jobs_2021_parquet(self.TEST_JOB_ESTIMATES_FILE)
        generate_worker_parquet(self.TEST_WORKER_FILE)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_JOB_ESTIMATES_FILE)
            shutil.rmtree(self.TEST_WORKER_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_get_job_estimates_dataset(self):

        breakdown_df = job_role_breakdown.get_job_estimates_dataset(
            self.TEST_JOB_ESTIMATES_FILE
        )

        self.assertEqual(breakdown_df.count(), 5)
        self.assertEqual(
            breakdown_df.columns,
            ["master_locationid", "primary_service_type", "estimate_job_count_2021"],
        )

    def test_get_worker_dataset(self):
        worker_df = job_role_breakdown.get_worker_dataset(self.TEST_WORKER_FILE)

        self.assertEqual(worker_df.count(), 15)
        self.assertEqual(worker_df.columns, ["locationid", "workerid", "mainjrid"])

    def test_get_distinct_list_with_alias(self):
        columns = ["mainjrid", "some_other_field"]
        rows = [
            (1, "random value"),
            (1, "random value"),
            (2, "random value"),
            (22, "random value"),
            (22, "random value"),
            (23, "random value"),
            (23, "random value"),
            (42, "random value"),
            (42, "random value"),
            (42, "random value"),
            (42, "random value"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        output_df = job_role_breakdown.get_distinct_list(
            df, column_name="mainjrid", alias="mylist"
        )

        self.assertEqual(output_df.count(), 5)
        self.assertEqual(output_df.columns, ["mylist"])

        self.assertCountEqual(
            output_df.select("mylist").rdd.flatMap(lambda x: x).collect(),
            [1, 2, 22, 23, 42],
        )

    def test_get_distinct_list_without_alias(self):
        columns = ["mainjrid", "some_other_field"]
        rows = [
            (1, "random value"),
            (1, "random value"),
            (2, "random value"),
            (22, "random value"),
            (22, "random value"),
            (23, "random value"),
            (23, "random value"),
            (42, "random value"),
            (42, "random value"),
            (42, "random value"),
            (42, "random value"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        output_df = job_role_breakdown.get_distinct_list(df, column_name="mainjrid")

        self.assertEqual(output_df.count(), 5)
        self.assertEqual(output_df.columns, ["mainjrid"])

        self.assertCountEqual(
            output_df.select("mainjrid").rdd.flatMap(lambda x: x).collect(),
            [1, 2, 22, 23, 42],
        )

    def test_get_comprehensive_list_of_job_roles_to_locations(self):

        master_columns = [
            "master_locationid",
            "primary_service_type",
            "estimate_job_count_2021",
            "location_worker_records",
        ]
        master_rows = [
            ("1-000000001", "something or other", 15.5, 6),
            ("1-000000002", "something or other", 90.0, 3),
            ("1-000000003", "something or other", 16.5, 3),
        ]

        master_df = self.spark.createDataFrame(master_rows, master_columns)

        worker_df = job_role_breakdown.get_worker_dataset(self.TEST_WORKER_FILE)

        ouput_df = job_role_breakdown.get_comprehensive_list_of_job_roles_to_locations(
            worker_df, master_df
        )

        self.assertEqual(ouput_df.count(), 9)
        self.assertEqual(
            ouput_df.columns,
            [
                "master_locationid",
                "primary_service_type",
                "estimate_job_count_2021",
                "location_worker_records",
                "main_job_role",
            ],
        )

    def test_determine_worker_record_to_jobs_ratio(self):
        columns = ["estimate_job_count_2021", "location_worker_records"]
        rows = [(10, 0), (10, 5), (10, 10), (10, 100)]

        df = self.spark.createDataFrame(rows, columns)

        df = job_role_breakdown.determine_worker_record_to_jobs_ratio(df)

        self.assertEqual(df.count(), 4)
        self.assertEqual(
            df.columns,
            [
                "estimate_job_count_2021",
                "location_worker_records",
                "location_jobs_ratio",
                "location_jobs_to_model",
            ],
        )
        df_list = df.collect()
        self.assertEqual(df_list[0]["location_jobs_ratio"], 1)
        self.assertEqual(df_list[1]["location_jobs_ratio"], 1)
        self.assertEqual(df_list[2]["location_jobs_ratio"], 1)
        self.assertEqual(df_list[3]["location_jobs_ratio"], 0.1)
        self.assertEqual(df_list[0]["location_jobs_to_model"], 10)
        self.assertEqual(df_list[1]["location_jobs_to_model"], 5)
        self.assertEqual(df_list[2]["location_jobs_to_model"], 0)
        self.assertEqual(df_list[3]["location_jobs_to_model"], 0)

    def test_main(self):
        result_df = job_role_breakdown.main(
            self.TEST_JOB_ESTIMATES_FILE, self.TEST_WORKER_FILE
        )

        self.assertEqual(
            result_df.columns,
            [
                "master_locationid",
                "primary_service_type",
                "estimate_job_count_2021",
                "main_job_role",
                "location_jobs_ratio",
                "ascwds_num_of_jobs",
                "estimated_num_of_jobs",
                "estimate_job_role_count_2021",
            ],
        )

        self.assertEqual(result_df.count(), 15)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
