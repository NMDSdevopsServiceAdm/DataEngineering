import unittest
import shutil
from datetime import date
import warnings

from pyspark.sql import SparkSession

from jobs import cqc_coverage_based_on_login_purge
from tests.test_file_generator import (
    generate_ascwds_workplace_file,
    generate_cqc_locations_file,
    generate_cqc_providers_file,
    generate_acswds_workplace_structure_file,
)


class PrepareLocationsTests(unittest.TestCase):

    TEST_ASCWDS_WORKPLACE_FILE = "tests/test_data/domain=ascwds/dataset=workplace"
    TEST_CQC_LOCATION_FILE = "tests/test_data/domain=cqc/dataset=location"
    TEST_CQC_PROVIDERS_FILE = "tests/test_data/domain=cqc/dataset=providers"
    TEST_ASCWDS_WORKPLACE_STRUCTURE = "tests/test_data/domain=ascwds/tmp_workplace_structure"
    DESTINATION = "tests/test_data/domain=data_engineering/dataset=locations_prepared/version=1.0.0"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_prepare_locations").getOrCreate()
        generate_ascwds_workplace_file(self.TEST_ASCWDS_WORKPLACE_FILE)
        generate_cqc_locations_file(self.TEST_CQC_LOCATION_FILE)
        generate_cqc_providers_file(self.TEST_CQC_PROVIDERS_FILE)
        generate_acswds_workplace_structure_file(self.TEST_ASCWDS_WORKPLACE_STRUCTURE)

        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ASCWDS_WORKPLACE_FILE)
            shutil.rmtree(self.TEST_CQC_LOCATION_FILE)
            shutil.rmtree(self.TEST_CQC_PROVIDERS_FILE)
            shutil.rmtree(self.TEST_ASCWDS_WORKPLACE_STRUCTURE)
            shutil.rmtree(self.DESTINATION)
        except OSError:
            pass  # Ignore dir does not exist

    def test_add_ascwds_workplace_structure(self):
        workplace_df = cqc_coverage_based_on_login_purge.add_ascwds_workplace_structure(
            self.TEST_ASCWDS_WORKPLACE_STRUCTURE
        )

        self.assertEqual(workplace_df.count(), 4)

        rows = workplace_df.collect()
        self.assertEqual(rows[0]["ascwds_workplace_structure"], "Parent")
        self.assertEqual(rows[1]["ascwds_workplace_structure"], "Subsidiary")
        self.assertEqual(rows[2]["ascwds_workplace_structure"], "Single")
        self.assertEqual(rows[3]["ascwds_workplace_structure"], "")


if __name__ == "__main__":
    unittest.main(warnings="ignore")
