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

    def test_main_adds_vectorized_column(self):
        df = locations_feature_engineering.main(self.PREPARED_LOCATIONS_TEST_DATA)
        self.assertIn("features", df.columns)

    def test_main_explodes_region_columns(self):
        df = locations_feature_engineering.main(self.PREPARED_LOCATIONS_TEST_DATA)
        self.assertIn("south_east", df.columns)

        rows = df.collect()

        self.assertEqual(rows[6].yorkshire_and_the_humbler, 1)

    def test_main_adds_date_diff_column(self):
        df = locations_feature_engineering.main(self.PREPARED_LOCATIONS_TEST_DATA)
        self.assertIn("date_diff", df.columns)

        rows = df.collect()

        self.assertEqual(rows[7].date_diff, 52)
        self.assertEqual(rows[12].date_diff, 0)

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
        # fmt: off
        features = [
            'service_1','service_2','service_3','service_4','service_5','service_6','service_7','service_8',
            'service_9','service_10','service_11','service_12','service_13','service_14','service_15','service_16',
            'service_17','service_18','service_19','service_20','service_21','service_22','service_23','service_24',
            'service_25','service_26','service_27','service_28','service_29'
            ]
        # fmt: on
        df = locations_feature_engineering.vectorize(df, features)

        self.assertIn("features", df.columns)
        self.assertIsInstance(df.first()["features"], SparseVector)

    def test_explode_regions_returns_codifies_regions(self):
        _, regions = locations_feature_engineering.explode_regions(self.test_df)

        self.assertIn("south_east", regions)
        self.assertIn("south_west", regions)
        self.assertIn("yorkshire_and_the_humbler", regions)
        self.assertIn("pseudo_wales", regions)

        self.assertNotIn("South East", regions)
        self.assertNotIn("South West", regions)
        self.assertNotIn("Yorkshire and The Humbler", regions)
        self.assertNotIn("(pseudo) Wales", regions)

    def test_explode_regions_returns_distinct_regions(self):
        _, regions = locations_feature_engineering.explode_regions(self.test_df)

        self.assertEqual(len(regions), 6)

    def test_explode_regions_creates_a_column_for_each_region(self):
        df, regions = locations_feature_engineering.explode_regions(self.test_df)

        for region in regions:
            self.assertIn(region, df.columns)

        rows = df.select(regions).collect()

        self.assertEqual(rows[0].south_east, 1)
        self.assertEqual(rows[0].south_west, 0)
        self.assertEqual(rows[0].yorkshire_and_the_humbler, 0)
        self.assertEqual(rows[0].merseyside, 0)

        self.assertEqual(rows[3].merseyside, 1)
        self.assertEqual(rows[4].london_senate, 1)
        self.assertEqual(rows[6].yorkshire_and_the_humbler, 1)

    def test_add_date_diff_column_works_out_diff_from_max_snapshop(self):
        df = locations_feature_engineering.days_diff_from_latest_snapshot(self.test_df)

        self.assertIn("date_diff", df.columns)

        rows = df.select("date_diff").collect()

        self.assertEqual(rows[0].date_diff, 80)
        self.assertEqual(rows[2].date_diff, 100)
