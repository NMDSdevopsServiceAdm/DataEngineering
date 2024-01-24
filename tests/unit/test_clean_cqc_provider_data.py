import unittest
import shutil
import warnings

from pyspark.sql import SparkSession

from tests.test_file_generator import generate_cqc_providers_file


class CleanCQCProviderDatasetTests(unittest.TestCase):
    TEST_CQC_PROVIDERS_FILE = "tests/test_data/domain=cqc/dataset=providers"

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "sfc_data_engineering_test_clean_cqc_provider_data"
        ).getOrCreate()
        generate_cqc_providers_file(self.TEST_CQC_PROVIDERS_FILE)

        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_CQC_PROVIDERS_FILE)
        except OSError:
            pass  # Ignore dir does not exist

    def test_get_cqc_provider_df(self):
        print("todo")

    def test_clean_cqc_provider_df(self):
        print("todo")

    def test_write_cleaned_provider_df_to_parquet(self):
        print("todo")

    def test_main(self):
        print("todo")


if __name__ == "__main__":
    unittest.main(warnings="ignore")
