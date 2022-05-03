import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import create_cqc_coverage_csv_file as job

from tests.test_file_generator import (
    generate_cqc_locations_parquet,
    generate_cqc_providers_parquet,
    generate_ascwds_workplace_parquet,
)


class Estimate2021JobsTests(unittest.TestCase):

    TEST_CQC_LOCATIONS_FILE = "tests/test_data/tmp/cqc_locations_prepared_file.parquet"
    TEST_CQC_PROVIDERS_FILE = "tests/test_data/tmp/ons_geography_file.parquet"
    TEST_ASCWDS_WORKPLACE_FILE = "tests/test_data/tmp/ethnicity_by_super_output_area.csv"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_create_cqc_coverage_csv_file").getOrCreate()
        generate_cqc_locations_parquet(self.TEST_CQC_LOCATIONS_FILE)
        generate_cqc_providers_parquet(self.TEST_CQC_PROVIDERS_FILE)
        generate_ascwds_workplace_parquet(self.TEST_ASCWDS_WORKPLACE_FILE)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_CQC_LOCATIONS_FILE)
            shutil.rmtree(self.TEST_CQC_PROVIDERS_FILE)
            shutil.rmtree(self.TEST_ASCWDS_WORKPLACE_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_get_cqc_locations_df(self):
        df = job.get_cqc_locations_df(self.TEST_CQC_LOCATIONS_FILE)

        self.assertEqual(df.count(), 15)
        self.assertEqual(
            df.columns,
            [
                "locationid",
                "providerid",
                "name",
                "postalcode",
                "type",
                "registrationstatus",
                "localauthority",
                "region",
            ],
        )

    def test_get_cqc_providers_df(self):
        df = job.get_cqc_providers_df(self.TEST_CQC_PROVIDERS_FILE)

        self.assertEqual(df.count(), 5)
        self.assertEqual(
            df.columns,
            [
                "providerid",
                "providername",
            ],
        )

    def test_get_ascwds_workplace_df(self):
        df = job.get_ascwds_workplace_df(self.TEST_ASCWDS_WORKPLACE_FILE)

        self.assertEqual(df.count(), 15)
        self.assertEqual(
            df.columns,
            [
                "locationid",
                "locationid_ASCWDS",
                "establishmentid",
                "orgid",
                "isparent",
                "import_date",
                "mupddate",
                "lapermission",
            ],
        )

    def test_relabel_permission_col(self):
        pass

    def test_main(self):
        pass


if __name__ == "__main__":
    unittest.main(warnings="ignore")


class EthnicityBreakdownTests(unittest.TestCase):
    def test_get_all_job_roles_per_location_df(self):
        all_job_roles_df = ethnicity_breakdown.get_all_job_roles_per_location_df(self.TEST_ALL_JOB_ROLES_FILE)

        self.assertEqual(all_job_roles_df.count(), 15)
        self.assertEqual(
            all_job_roles_df.columns, ["master_locationid", "primary_service_type", "main_job_role", "estimated_jobs"]
        )

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
