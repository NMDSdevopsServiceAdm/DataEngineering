import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import ethnicity_breakdown
from tests.test_file_generator import (
    generate_all_job_roles_parquet,
    generate_cqc_locations_prepared_parquet,
    generate_ons_geography_parquet,
    generate_ethnicity_census_lsoa_csv,
)


class EthnicityBreakdownTests(unittest.TestCase):

    TEST_ALL_JOB_ROLES_FILE = "tests/test_data/tmp/all_job_roles_file.parquet"
    TEST_CQC_LOCATIONS_PREPARED_FILE = "tests/test_data/tmp/cqc_locations_prepared_file.parquet"
    TEST_ONS_FILE = "tests/test_data/tmp/ons_geography_file.parquet"
    TEST_CENSUS_FILE = "tests/test_data/tmp/ethnicity_by_super_output_area.csv"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_ethnicity_breakdown").getOrCreate()
        generate_all_job_roles_parquet(self.TEST_ALL_JOB_ROLES_FILE)
        generate_cqc_locations_prepared_parquet(self.TEST_CQC_LOCATIONS_PREPARED_FILE)
        generate_ons_geography_parquet(self.TEST_ONS_FILE)
        generate_ethnicity_census_lsoa_csv(self.TEST_CENSUS_FILE)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ALL_JOB_ROLES_FILE)
            shutil.rmtree(self.TEST_CQC_LOCATIONS_PREPARED_FILE)
            shutil.rmtree(self.TEST_ONS_FILE)
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

    def test_get_census_ethnicity_df(self):
        census_df = ethnicity_breakdown.get_census_ethnicity_lsoa_df(self.TEST_CENSUS_FILE)

        self.assertEqual(census_df.count(), 4)

        census_df = census_df.collect()
        self.assertEqual(census_df[0]["lsoa"], "E01000001")

    def test_model_ethnicity_white(self):
        columns = ["magic_service", "magic_region", "magic_jobrole", "census_white_msoa_%"]

        rows = [
            (-1.000, -1.000, -1.000, 0.0),
            (0.1, 0.2, -0.7, 1.0),
            (1.000, 1.000, 1.000, 0.5),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = ethnicity_breakdown.model_ethnicity_white(df)
        self.assertEqual(df.count(), 3)

        # TODO - issue with floats and integers
        # df = df.collect()
        # self.assertEqual(round(df[0]["magic_white_prediction"], 0.1), 0.0)
        # self.assertEqual(round(df[1]["magic_white_prediction"], 0.1), 0.5)
        # self.assertEqual(round(df[2]["magic_white_prediction"], 0.1), 1.0)

    def test_unpivot_data_for_tableau(self):
        columns = ["primary_service_type", "ons_region", "main_job_role", "estimated_jobs_white", "estimated_jobs_bame"]

        rows = [
            ("service_1", "region_1", "job_role_1", 1.2, 3.4),
            ("service_1", "region_1", "job_role_1", 5.6, 7.8),
            ("service_2", "region_1", "job_role_2", 9.9, 0.0),
        ]
        input_df = self.spark.createDataFrame(rows, columns)

        output_df = ethnicity_breakdown.unpivot_data_for_tableau(input_df)
        self.assertEqual(input_df.count(), 3)
        self.assertEqual(output_df.count(), 6)

        output_df = output_df.collect()
        self.assertEqual(output_df[0]["ethnicity"], "White")
        self.assertEqual(output_df[0]["estimated_jobs"], 1.2)
        self.assertEqual(output_df[1]["ethnicity"], "BAME")
        self.assertEqual(output_df[1]["estimated_jobs"], 3.4)
        self.assertEqual(output_df[2]["ethnicity"], "White")
        self.assertEqual(output_df[2]["estimated_jobs"], 5.6)

    def test_main(self):
        result_df = ethnicity_breakdown.main(
            self.TEST_ALL_JOB_ROLES_FILE,
            self.TEST_CQC_LOCATIONS_PREPARED_FILE,
            self.TEST_ONS_FILE,
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
