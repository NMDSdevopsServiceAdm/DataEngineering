import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import ethnicity_breakdown
from tests.test_file_generator import generate_ethnicity_parquet


class EthnicityBreakdownTests(unittest.TestCase):

    TEST_ETHNICITY_FILE = "tests/test_data/tmp/ethnicity_file.parquet"
    ASCWDS_IMPORT_DATE = "20200301"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_ethnicity_breakdown").getOrCreate()
        generate_ethnicity_parquet(self.TEST_ETHNICITY_FILE)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ETHNICITY_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_get_ascwds_ethnicity_df(self):
        ethnicity_df = ethnicity_breakdown.get_ascwds_ethnicity_df(self.TEST_ETHNICITY_FILE, self.ASCWDS_IMPORT_DATE)

        self.assertEqual(ethnicity_df.count(), 14)
        self.assertEqual(ethnicity_df.columns, ["locationid", "mainjrid", "ethnicity"])

    def test_main(self):
        result_df = ethnicity_breakdown.main(self.TEST_ETHNICITY_FILE, self.ASCWDS_IMPORT_DATE)

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

        self.assertEqual(result_df.count(), 12)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
