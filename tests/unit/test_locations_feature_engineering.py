import unittest
import shutil

from pyspark.sql import SparkSession
from jobs.locations_feature_engineering import main
from tests.test_file_generator import generate_prepared_locations_file_parquet


class LocationsFeatureEngineeringTests(unittest.TestCase):
    PREPARED_LOCATIONS_TEST_DATA = (
        "tests/test_data/domain=data_engineering/dataset=prepared_locations"
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_locations_feature_engineering"
        ).getOrCreate()
        generate_prepared_locations_file_parquet(self.PREPARED_LOCATIONS_TEST_DATA)
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(self.PREPARED_LOCATIONS_TEST_DATA)
        return super().tearDown()

    def test_main_adds_service_count_column(self):
        df = main(self.PREPARED_LOCATIONS_TEST_DATA)
        self.assertIn("service_count", df.columns)

        rows = df.collect()

        self.assertEqual(rows[0].service_count, 2)
        self.assertEqual(rows[1].service_count, 1)
