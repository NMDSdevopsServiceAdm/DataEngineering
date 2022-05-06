import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import ingest_cqc_csv_dataset
from tests.test_file_generator import generate_prov_loc_csv


class CQC_Care_Directory_Tests(unittest.TestCase):

    TEST_CQC_PROV_LOC_FILE = "tests/test_data/tmp/cqc_care_directory.csv"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_cqc_care_directory_to_parquet").getOrCreate()
        generate_prov_loc_csv(self.TEST_CQC_PROV_LOC_FILE)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_CQC_PROV_LOC_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_get_all_job_roles_per_location_df(self):
        pass

        # all_job_roles_df = ingest_cqc_csv_dataset.unique_providers_with_locations(self.TEST_CQC_CARE_DIRECTORY_FILE)

        # self.assertEqual(all_job_roles_df.count(), 15)
        # self.assertEqual(
        #     all_job_roles_df.columns, ["master_locationid", "primary_service_type", "main_job_role", "estimated_jobs"]
        # )

        # census_df = census_df.collect()
        # self.assertEqual(census_df[0]["lsoa"], "E01000001")

    def test_main(self):
        pass

        # result_df = ingest_cqc_csv_dataset.main(
        #     self.TEST_CQC_PROV_LOC_FILE,
        # )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
