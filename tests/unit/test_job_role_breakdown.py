import unittest
from pathlib import Path
from pyspark.sql import SparkSession
from jobs import job_role_breakdown


class JobRoleBreakdownTests(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_job_role_breakdown").getOrCreate()

    # def test_determine_job_role_breakdown_by_service(self):
    #      columns = [
    #         "locationid",
    #         "primary_service_type",
    #         "pir_service_users",
    #         "number_of_beds",
    #         "estimate_job_count_2021",
    #     ]
    #     rows = [
    #         ("1-000000001", "Care home without nursing", 10, 5, None),
    #         ("1-000000002", "Care home with nursing", 10, 3, None),
    #         ("1-000000003", "Care home without nursing", 20, None, None),
    #         ("1-000000004", "non-residential", 10, None, 10),
    #     ]
    #     df = self.spark.createDataFrame(rows, columns)

    #     df = job.model_care_home_without_nursing_cqc_beds_and_pir(df)
    #     self.assertEqual(df.count(), 4)

    #     df = df.collect()
    #     self.assertEqual(df[0]["estimate_job_count_2021"], 16.467)
    #     self.assertEqual(df[1]["estimate_job_count_2021"], None)
    #     self.assertEqual(df[2]["estimate_job_count_2021"], None)
    #     self.assertEqual(df[3]["estimate_job_count_2021"], 10)

    def test_get_job_estimates_dataset(self):
        test_job_estimates_file = "tests/test_data/domain=data_engineering/dataset=job_estimates_2021/format=parquet/job_estimates_2021.parquet"
        breakdown_df = job_role_breakdown.get_job_estimates_dataset(
            test_job_estimates_file)

        self.assertEqual(breakdown_df.count(), 4)
        self.assertEqual(breakdown_df.columns, [
                         "master_locationid", "primary_service_type", "estimate_job_count_2021"])

    def test_get_worker_dataset(self):
        test_worker_file = "tests/test_data/domain=CQC/dataset=worker/format=parquet/worker_file.parquet"
        worker_df = job_role_breakdown.get_worker_dataset(test_worker_file)

        self.assertEqual(worker_df.count(), 12)
        self.assertEqual(worker_df.columns, [
                         "locationid", "workerid", "mainjrid"])


if __name__ == "__main__":
    unittest.main(warnings="ignore")
