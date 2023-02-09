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


class HelperForJobRuleTests:
    @staticmethod
    def get_test_job_schema() -> StructType:
        return StructType(
            [
                StructField("locationid", StringType(), False),
                StructField("total_staff", IntegerType(), True),
                StructField("worker_record_count", IntegerType(), True),
                StructField("number_of_beds", IntegerType(), True),
            ]
        )

    @staticmethod
    def get_row_of_test_data(
        location_id: str,
        worker_record_count: int,
        total_staff: int,
        number_of_beds: int,
    ) -> list[tuple[str, int, int, int]]:
        return [(location_id, worker_record_count, total_staff, number_of_beds)]


class TestGroup(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_prepare_locations").getOrCreate()
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
    def test_returns_0_when_worker_record_count_is_0_and_total_staff_is_0(self):
        pass


if __name__ == "__main__":
    unittest.main(warnings="ignore")
