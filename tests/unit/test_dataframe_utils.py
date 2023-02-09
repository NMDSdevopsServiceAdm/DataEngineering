import shutil
import unittest
import warnings
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, StructField

from utils.prepare_locations_utils import dataframe_utils
from tests.test_file_generator import (
    generate_ascwds_workplace_file,
    generate_cqc_locations_file,
    generate_cqc_providers_file,
    generate_pir_file,
    generate_ons_denormalised_data,
)


@dataclass
class PathsForTestData:
    TEST_ASCWDS_WORKPLACE_FILE = "tests/test_data/domain=ascwds/dataset=workplace"
    TEST_CQC_LOCATION_FILE = "tests/test_data/domain=cqc/dataset=location"
    TEST_CQC_PROVIDERS_FILE = "tests/test_data/domain=cqc/dataset=providers"
    TEST_PIR_FILE = "tests/test_data/domain=cqc/dataset=pir"
    TEST_ONS_FILE = "tests/test_data/domain=ons/dataset=postcodes"
    DESTINATION = "tests/test_data/domain=data_engineering/dataset=locations_prepared/version=1.0.0"


class HelperForDataFrameTests:
    @staticmethod
    def get_test_df_schema() -> StructType:
        return StructType(
            [
                StructField("locationid", StringType(), False),
                StructField("snapshot_date", StringType(), True),
            ]
        )

    @staticmethod
    def get_row_of_test_data(
        location_id: str,
        snapshot_date: str,
    ) -> list[tuple[str, str]]:
        return [(location_id, snapshot_date)]


class TestGroup(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_dataframe_utils").getOrCreate()
        generate_ascwds_workplace_file(PathsForTestData.TEST_ASCWDS_WORKPLACE_FILE)
        self.cqc_loc_df = generate_cqc_locations_file(PathsForTestData.TEST_CQC_LOCATION_FILE)
        generate_cqc_providers_file(PathsForTestData.TEST_CQC_PROVIDERS_FILE)
        generate_pir_file(PathsForTestData.TEST_PIR_FILE)
        self.ons_df = generate_ons_denormalised_data(PathsForTestData.TEST_ONS_FILE)

        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            shutil.rmtree(PathsForTestData.TEST_ASCWDS_WORKPLACE_FILE)
            shutil.rmtree(PathsForTestData.TEST_CQC_LOCATION_FILE)
            shutil.rmtree(PathsForTestData.TEST_CQC_PROVIDERS_FILE)
            shutil.rmtree(PathsForTestData.TEST_PIR_FILE)
            shutil.rmtree(PathsForTestData.TEST_ONS_FILE)
            shutil.rmtree(PathsForTestData.DESTINATION)
        except OSError:
            pass  # Ignore dir does not exist

    @unittest.skip("finish this test")
    def test_returns_dataframe_with_additional_column(self):
        pass

    @unittest.skip("finish this test")
    def test_returns_dataframe_with_additional_column_of_string_type(self):
        pass

    @unittest.skip("finish this test")
    def test_returns_dataframe_with_additional_column_for_for_snapshot_year_when_year_substring_is_requested(self):
        pass

    @unittest.skip("finish this test")
    def test_returns_dataframe_with_additional_column_for_for_snapshot_month_when_month_substring_is_requested(self):
        pass

    @unittest.skip("finish this test")
    def test_returns_dataframe_with_additional_column_for_for_snapshot_day_when_day_substring_is_requested(self):
        pass


if __name__ == "__main__":
    unittest.main(warnings="ignore")
