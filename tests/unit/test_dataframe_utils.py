import shutil
import unittest
import warnings
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, StructField

from utils.prepare_locations_utils.dataframe_utils import (
    add_column_with_snaphot_date_substring,
    START_OF_YEAR_SUBSTRING,
    LENGTH_OF_YEAR_SUBSTRING,
    START_OF_MONTH_SUBSTRING,
    LENGTH_OF_MONTH_SUBSTRING,
    START_OF_DAY_SUBSTRING,
    LENGTH_OF_DAY_SUBSTRING,
    SNAPSHOT_DAY_COLUMN_NAME,
    SNAPSHOT_MONTH_COLUMN_NAME,
    SNAPSHOT_YEAR_COLUMN_NAME,
)
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

    def test_add_column_with_snapshot_date_substring_returns_dataframe_with_additional_column(self):
        dataframe_utils_data_schema = HelperForDataFrameTests.get_test_df_schema()
        row_data = HelperForDataFrameTests.get_row_of_test_data(location_id="1-000000001", snapshot_date="20230121")

        df = self.spark.createDataFrame(row_data, dataframe_utils_data_schema)

        df_with_snapshot_substring_column = add_column_with_snaphot_date_substring(
            df, SNAPSHOT_YEAR_COLUMN_NAME, START_OF_YEAR_SUBSTRING, LENGTH_OF_YEAR_SUBSTRING
        )

        self.assertEqual(df_with_snapshot_substring_column.columns[0], "locationid")
        self.assertEqual(df_with_snapshot_substring_column.columns[1], "snapshot_date")
        self.assertEqual(df_with_snapshot_substring_column.columns[2], SNAPSHOT_YEAR_COLUMN_NAME)

    def test_add_column_with_snapshot_date_substring_returns_dataframe_with_additional_column_of_string_type(self):
        dataframe_utils_data_schema = HelperForDataFrameTests.get_test_df_schema()
        row_data = HelperForDataFrameTests.get_row_of_test_data(location_id="1-000000001", snapshot_date="20230121")

        df = self.spark.createDataFrame(row_data, dataframe_utils_data_schema)

        df_with_snapshot_substring_column = add_column_with_snaphot_date_substring(
            df, SNAPSHOT_YEAR_COLUMN_NAME, START_OF_YEAR_SUBSTRING, LENGTH_OF_YEAR_SUBSTRING
        )
        df_with_snapshot_substring_column_list = df_with_snapshot_substring_column.collect()

        self.assertIsInstance(df_with_snapshot_substring_column_list[0][SNAPSHOT_YEAR_COLUMN_NAME], str)

    def test_add_column_with_snapshot_date_substring_returns_correct_values_for_year_month_and_day(
        self,
    ):
        dataframe_utils_data_schema = HelperForDataFrameTests.get_test_df_schema()
        row_data = HelperForDataFrameTests.get_row_of_test_data(location_id="1-000000001", snapshot_date="20230121")
        snapshot_year = "2023"
        snapshot_month = "01"
        snapshot_day = "21"

        df = self.spark.createDataFrame(row_data, dataframe_utils_data_schema)

        df_with_snapshot_substring_column = add_column_with_snaphot_date_substring(
            df, SNAPSHOT_YEAR_COLUMN_NAME, START_OF_YEAR_SUBSTRING, LENGTH_OF_YEAR_SUBSTRING
        )
        df_with_snapshot_substring_column = add_column_with_snaphot_date_substring(
            df_with_snapshot_substring_column,
            SNAPSHOT_MONTH_COLUMN_NAME,
            START_OF_MONTH_SUBSTRING,
            LENGTH_OF_MONTH_SUBSTRING,
        )
        df_with_snapshot_substring_column = add_column_with_snaphot_date_substring(
            df_with_snapshot_substring_column, SNAPSHOT_DAY_COLUMN_NAME, START_OF_DAY_SUBSTRING, LENGTH_OF_DAY_SUBSTRING
        )

        df_with_snapshot_substring_column_list = df_with_snapshot_substring_column.collect()

        self.assertEqual(df_with_snapshot_substring_column_list[0][SNAPSHOT_YEAR_COLUMN_NAME], snapshot_year)
        self.assertEqual(df_with_snapshot_substring_column_list[0][SNAPSHOT_MONTH_COLUMN_NAME], snapshot_month)
        self.assertEqual(df_with_snapshot_substring_column_list[0][SNAPSHOT_DAY_COLUMN_NAME], snapshot_day)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
