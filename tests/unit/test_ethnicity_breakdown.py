import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import ethnicity_breakdown
from tests.test_file_generator import (
    generate_all_job_roles_parquet,
    generate_cqc_locations_prepared_parquet,
    generate_ons_geography_parquet,
    generate_ethnicity_parquet,
    generate_ethnicity_census_lsoa_csv,
)


class EthnicityBreakdownTests(unittest.TestCase):

    TEST_ALL_JOB_ROLES_FILE = "tests/test_data/tmp/all_job_roles_file.parquet"
    TEST_CQC_LOCATIONS_PREPARED_FILE = "tests/test_data/tmp/cqc_locations_prepared_file.parquet"
    TEST_ONS_FILE = "tests/test_data/tmp/ons_geography_file.parquet"
    TEST_ETHNICITY_FILE = "tests/test_data/tmp/ethnicity_file.parquet"
    TEST_CENSUS_FILE = "tests/test_data/tmp/ethnicity_by_super_output_area.csv"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_ethnicity_breakdown").getOrCreate()
        generate_all_job_roles_parquet(self.TEST_ALL_JOB_ROLES_FILE)
        generate_cqc_locations_prepared_parquet(self.TEST_CQC_LOCATIONS_PREPARED_FILE)
        generate_ons_geography_parquet(self.TEST_ONS_FILE)
        generate_ethnicity_parquet(self.TEST_ETHNICITY_FILE)
        generate_ethnicity_census_lsoa_csv(self.TEST_CENSUS_FILE)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ALL_JOB_ROLES_FILE)
            shutil.rmtree(self.TEST_CQC_LOCATIONS_PREPARED_FILE)
            shutil.rmtree(self.TEST_ONS_FILE)
            shutil.rmtree(self.TEST_ETHNICITY_FILE)
            shutil.rmtree(self.TEST_CENSUS_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_get_all_job_roles_per_location_df(self):
        all_job_roles_df = ethnicity_breakdown.get_all_job_roles_per_location_df(self.TEST_ALL_JOB_ROLES_FILE)

        self.assertEqual(all_job_roles_df.count(), 15)
        self.assertEqual(
            all_job_roles_df.columns, ["master_locationid", "primary_service_type", "main_job_role", "estimated_jobs"]
        )

    def test_get_cqc_locations_df(self):
        ons_df = ethnicity_breakdown.get_cqc_locations_df(self.TEST_CQC_LOCATIONS_PREPARED_FILE)

        self.assertEqual(ons_df.count(), 6)
        self.assertEqual(ons_df.columns, ["locationid", "providerid", "postal_code"])

    def test_get_ons_geography_df(self):
        ons_df = ethnicity_breakdown.get_ons_geography_df(self.TEST_ONS_FILE)

        self.assertEqual(ons_df.count(), 3)
        self.assertEqual(ons_df.columns, ["ons_postcode", "ons_lsoa11", "ons_msoa11", "ons_region"])

    def test_get_ascwds_ethnicity_df(self):
        ethnicity_df = ethnicity_breakdown.get_ascwds_ethnicity_df(self.TEST_ETHNICITY_FILE)

        self.assertEqual(ethnicity_df.count(), 16)
        self.assertEqual(ethnicity_df.columns, ["locationid", "mainjrid", "ethnicity"])

    def test_get_census_ethnicity_df(self):
        census_df = ethnicity_breakdown.get_census_ethnicity_lsoa_df(self.TEST_CENSUS_FILE)

        self.assertEqual(census_df.count(), 4)
        # self.assertEqual(
        #     census_df.columns,
        #     [
        #         "lsoa",
        #         "census_asian_lsoa",
        #         "census_black_lsoa",
        #         "census_mixed_lsoa",
        #         "census_other_lsoa",
        #         "census_white_lsoa",
        #         "census_base_lsoa",
        #     ],
        # )

        census_df = census_df.collect()
        self.assertEqual(census_df[0]["lsoa"], "E01000001")

    def test_main(self):
        result_df = ethnicity_breakdown.main(
            self.TEST_ALL_JOB_ROLES_FILE,
            self.TEST_CQC_LOCATIONS_PREPARED_FILE,
            self.TEST_ONS_FILE,
            self.TEST_ETHNICITY_FILE,
            self.TEST_CENSUS_FILE,
        )

        self.assertEqual(result_df.count(), 30)

        self.assertEqual(
            result_df.columns,
            [
                "primary_service_type",
                "ons_region",
                "main_job_role",
                "ethnicity",
                "estimated_jobs",
            ],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
