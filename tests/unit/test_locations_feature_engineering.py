import unittest
import shutil

from pyspark.sql import SparkSession
from pyspark.ml.linalg import SparseVector

from jobs import locations_feature_engineering
from tests.test_file_generator import generate_prepared_locations_file_parquet


class LocationsFeatureEngineeringTests(unittest.TestCase):
    PREPARED_LOCATIONS_TEST_DATA = (
        "tests/test_data/domain=data_engineering/dataset=prepared_locations"
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_locations_feature_engineering"
        ).getOrCreate()
        self.test_df = generate_prepared_locations_file_parquet(
            self.PREPARED_LOCATIONS_TEST_DATA
        )
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(self.PREPARED_LOCATIONS_TEST_DATA)
        return super().tearDown()

    # MAIN METHOD TESTS

    def test_main_adds_service_count_column(self):
        df = locations_feature_engineering.main(self.PREPARED_LOCATIONS_TEST_DATA)
        self.assertIn("service_count", df.columns)

        rows = df.collect()

        self.assertEqual(rows[0].service_count, 2)
        self.assertEqual(rows[1].service_count, 1)

    def test_main_explodes_service_columns(self):
        df = locations_feature_engineering.main(self.PREPARED_LOCATIONS_TEST_DATA)
        self.assertIn("service_21", df.columns)

        rows = df.collect()

        self.assertEqual(rows[2].service_10, 1)

    # OTHER METHODS TEST

    def test_explode_services_creates_a_column_for_each_service(self):
        df = locations_feature_engineering.explode_services(self.test_df)

        expected_service_columns = [f"service_{i}" for i in range(1, 30)]

        for column in expected_service_columns:
            self.assertIn(column, df.columns)

        rows = df.select(expected_service_columns).collect()

        self.assertEqual(rows[0].service_12, 1)
        self.assertEqual(rows[0].service_23, 1)
        self.assertEqual(rows[0].service_8, 0)
        self.assertEqual(rows[0].service_27, 0)

        self.assertEqual(rows[8].service_8, 1)
        self.assertEqual(rows[8].service_12, 0)
        self.assertEqual(rows[8].service_23, 0)

    def test_vectorize_adds_new_features_column(self):
        df = locations_feature_engineering.explode_services(self.test_df)
        features = ['service_1',
        'service_2','service_3','service_4','service_5','service_6','service_7',
        'service_8','service_9','service_10','service_11','service_12','service_13',
        'service_14','service_15','service_16','service_17','service_18','service_19',
        'service_20','service_21','service_22','service_23','service_24','service_25',
        'service_26','service_27','service_28','service_29']
        df = locations_feature_engineering.vectorize(df, features)

        self.assertIn("features", df.columns)
        self.assertIsInstance(df.first()["features"], SparseVector)
