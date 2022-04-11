import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import ethnicity_breakdown
from tests.test_file_generator import generate_ethnicity_parquet


class EthnicityBreakdownTests(unittest.TestCase):

    TEST_ETHNICITY_FILE = "tests/test_data/tmp/ethnicity_file.parquet"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_ethnicity_breakdown").getOrCreate()
        generate_ethnicity_parquet(self.TEST_ETHNICITY_FILE)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ETHNICITY_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_get_ascwds_ethnicity_df(self):
        path = "tests/test_data/domain=ASCWDS/dataset=worker/version=0.0.1/format=parquet"
        import_date = "20220301"
        df = ethnicity_breakdown.get_ascwds_ethnicity_df(path, import_date, "tests/test_data/")

        self.assertEqual(df.count(), 10)

        self.assertEqual(df.columns[0], "locationid")
        self.assertEqual(df.columns[1], "mainjrid")
        self.assertEqual(df.columns[2], "ethnicity")

        df = df.collect()
        self.assertEqual(df[0]["mainjrid"], 1)

    def test_main(self):
        result_df = ethnicity_breakdown.main(self.TEST_ETHNICITY_FILE, "20200101")

        # worker_source, ascwds_import_date

        # self.assertEqual(
        #     result_df.columns,
        #     [
        #         "master_locationid",
        #         "primary_service_type",
        #         "estimate_job_count_2021",
        #         "main_job_role",
        #         "location_jobs_ratio",
        #         "ascwds_num_of_jobs",
        #         "estimated_num_of_jobs",
        #         "estimate_job_role_count_2021",
        #     ],
        # )

        self.assertEqual(result_df.count(), 14)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
