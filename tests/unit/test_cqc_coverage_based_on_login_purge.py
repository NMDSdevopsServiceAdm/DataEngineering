import unittest
import shutil
from datetime import date
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from jobs import cqc_coverage_based_on_login_purge
from tests.test_file_generator import (
    generate_ascwds_workplace_file,
    generate_cqc_locations_file,
    generate_cqc_providers_file,
    generate_cqc_covarage_to_summarise_parquet,
)


class PrepareLocationsTests(unittest.TestCase):

    TEST_ASCWDS_WORKPLACE_FILE = "tests/test_data/domain=ascwds/dataset=workplace"
    TEST_CQC_LOCATION_FILE = "tests/test_data/domain=cqc/dataset=location"
    TEST_CQC_PROVIDERS_FILE = "tests/test_data/domain=cqc/dataset=providers"
    DESTINATION = "tests/test_data/domain=data_engineering/dataset=locations_prepared/version=1.0.0"
    TEST_CQC_COVERAGE_FILE = "tests/test_data/domain=cqc/dataset=coverage"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_prepare_locations").getOrCreate()
        generate_ascwds_workplace_file(self.TEST_ASCWDS_WORKPLACE_FILE)
        self.cqc_loc_df = generate_cqc_locations_file(self.TEST_CQC_LOCATION_FILE)
        generate_cqc_providers_file(self.TEST_CQC_PROVIDERS_FILE)
        coverage_df = generate_cqc_covarage_to_summarise_parquet(self.TEST_CQC_COVERAGE_FILE)

        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ASCWDS_WORKPLACE_FILE)
            shutil.rmtree(self.TEST_CQC_LOCATION_FILE)
            shutil.rmtree(self.TEST_CQC_PROVIDERS_FILE)
            shutil.rmtree(self.DESTINATION)
        except OSError:
            pass  # Ignore dir does not exist

    def test_calculate_coverage(self):
        coverage = cqc_coverage_based_on_login_purge.calculate_coverage(self.TEST_CQC_LOCATION_FILE, "region")
        # check column names
        self.assertEqual(coverage.columns[0], "region")
        self.assertEqual(coverage.columns[1], "total_locations")
        self.assertEqual(coverage.columns[2], "total_locations_in_ASC-WDS")
        self.assertEqual(coverage.columns[3], "percentage_coverage_by_region")
        # check counts
        self.assertEqual(coverage[2, 1], 2)
        # check coverage calculations
        self.assertEqual(coverage[2, 3], 0.5)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
