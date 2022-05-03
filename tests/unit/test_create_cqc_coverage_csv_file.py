import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import create_cqc_coverage_csv_file as job

from tests.test_file_generator import (
    generate_cqc_locations_parquet,
    generate_cqc_providers_parquet,
    generate_ascwds_workplace_parquet,
)


class CreateCQCCoverageTests(unittest.TestCase):

    TEST_CQC_LOCATIONS_FILE = "tests/test_data/tmp/cqc_locations_file.parquet"
    TEST_CQC_PROVIDERS_FILE = "tests/test_data/tmp/cqc_providers_file.parquet"
    TEST_ASCWDS_WORKPLACE_FILE = "tests/test_data/tmp/ascwds_workplace_file.parquet"
    TEST_OUTPUT_FILE = "tests/test_data/tmp/output.csv"

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
                "provider_name",
            ],
        )

    def test_get_ascwds_workplace_df(self):
        df = job.get_ascwds_workplace_df(self.TEST_ASCWDS_WORKPLACE_FILE)

        self.assertEqual(df.count(), 8)
        self.assertEqual(
            df.columns,
            [
                "locationid",
                "establishmentid",
                "orgid",
                "isparent",
                "import_date",
                "mupddate",
                "lapermission",
                "locationid_ASCWDS",
                "ascwds_workplace_import_date",
            ],
        )

        df = df.collect()
        self.assertEqual(df[0]["lapermission"], "Yes")
        self.assertEqual(df[1]["lapermission"], "No")
        self.assertEqual(df[5]["lapermission"], "Not recorded")

    def test_main(self):
        result_df = job.main(
            self.TEST_ASCWDS_WORKPLACE_FILE,
            self.TEST_CQC_LOCATIONS_FILE,
            self.TEST_CQC_PROVIDERS_FILE,
            self.TEST_OUTPUT_FILE,
        )

        self.assertEqual(result_df.count(), 15)

        self.assertEqual(
            result_df.columns,
            [
                "locationid",
                "name",
                "postalcode",
                "providerid",
                "provider_name",
                "region",
                "localauthority",
                "location_in_ASCWDS",
                "lapermission",
            ],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
