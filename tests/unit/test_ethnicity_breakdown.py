import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import ethnicity_breakdown
from tests.test_file_generator import (
    generate_all_job_roles_parquet,
    generate_cqc_locations_prepared_parquet,
    generate_ethnicity_parquet,
)


class EthnicityBreakdownTests(unittest.TestCase):

    TEST_ALL_JOB_ROLES_FILE = "tests/test_data/tmp/all_job_roles_file.parquet"
    TEST_CQC_LOCATIONS_PREPARED_FILE = "tests/test_data/tmp/cqc_locations_prepared_file.parquet"
    TEST_ETHNICITY_FILE = "tests/test_data/tmp/ethnicity_file.parquet"
    ASCWDS_IMPORT_DATE = "20200301"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_ethnicity_breakdown").getOrCreate()
        generate_all_job_roles_parquet(self.TEST_ALL_JOB_ROLES_FILE)
        generate_cqc_locations_prepared_parquet(self.TEST_CQC_LOCATIONS_PREPARED_FILE)
        generate_ethnicity_parquet(self.TEST_ETHNICITY_FILE)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ALL_JOB_ROLES_FILE)
            shutil.rmtree(self.TEST_CQC_LOCATIONS_PREPARED_FILE)
            shutil.rmtree(self.TEST_ETHNICITY_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_get_all_job_roles_per_location_df(self):
        all_job_roles_df = ethnicity_breakdown.get_all_job_roles_per_location_df(self.TEST_ALL_JOB_ROLES_FILE)

        self.assertEqual(all_job_roles_df.count(), 15)
        self.assertEqual(
            all_job_roles_df.columns, ["master_locationid", "primary_service_type", "main_job_role", "estimated_jobs"]
        )

    def get_cqc_locations_df(self):
        cqc_locations_prepared_df = ethnicity_breakdown.get_cqc_locations_df(self.TEST_CQC_LOCATIONS_PREPARED_FILE)

        self.assertEqual(cqc_locations_prepared_df.count(), 5)
        self.assertEqual(cqc_locations_prepared_df.columns, ["locationid", "providerid", "postal_code"])

    def test_get_ascwds_ethnicity_df(self):
        ethnicity_df = ethnicity_breakdown.get_ascwds_ethnicity_df(self.TEST_ETHNICITY_FILE, self.ASCWDS_IMPORT_DATE)

        self.assertEqual(ethnicity_df.count(), 14)
        self.assertEqual(ethnicity_df.columns, ["locationid", "mainjrid", "ethnicity"])

    def test_main(self):
        result_df = ethnicity_breakdown.main(
            self.TEST_ALL_JOB_ROLES_FILE,
            self.TEST_CQC_LOCATIONS_PREPARED_FILE,
            self.TEST_ETHNICITY_FILE,
            self.ASCWDS_IMPORT_DATE,
        )

        self.assertEqual(result_df.count(), 15)

        self.assertEqual(
            result_df.columns,
            [
                "master_locationid",
                "primary_service_type",
                "main_job_role",
                "estimated_jobs",
                "providerid",
                "postal_code",
            ],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
