import unittest
import shutil
import warnings

from pyspark.sql import SparkSession
from pyspark.ml.linalg import SparseVector

from jobs import locations_feature_engineering
from tests.test_file_generator import (
    generate_prepared_locations_file_parquet,
    generate_location_features_file_parquet,
)
from utils import feature_engineering_dictionaries


class LocationsFeatureEngineeringTests(unittest.TestCase):
    PREPARED_LOCATIONS_TEST_DATA = (
        "tests/test_data/domain=data_engineering/dataset=prepared_locations"
    )
    OUTPUT_DESTINATION = (
        "tests/test_data/domain=data_engineering/dataset=locations_features"
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_locations_feature_engineering"
        ).getOrCreate()
        self.test_df = generate_prepared_locations_file_parquet(
            self.PREPARED_LOCATIONS_TEST_DATA
        )
        warnings.simplefilter("ignore", ResourceWarning)
        return super().setUp()

    def tearDown(self) -> None:
        try:
            shutil.rmtree(self.PREPARED_LOCATIONS_TEST_DATA)
            shutil.rmtree(self.OUTPUT_DESTINATION)
        except OSError:
            pass
        return super().tearDown()

    # MAIN METHOD TESTS

    def test_main_selects_only_needed_columns(self):
        df = locations_feature_engineering.main(self.PREPARED_LOCATIONS_TEST_DATA)

        expected_columns = [
            "locationid",
            "snapshot_date",
            "ons_region",
            "number_of_beds",
            "people_directly_employed",
            "snapshot_year",
            "snapshot_month",
            "snapshot_day",
            "carehome",
            "care_home_features",
            "non_residential_inc_pir_features",
        ]

        for column in expected_columns:
            self.assertIn(column, df.columns)

        self.assertEqual(len(df.columns), len(expected_columns))

    def test_main_adds_care_home_vectorized_column(self):
        df = locations_feature_engineering.main(self.PREPARED_LOCATIONS_TEST_DATA)
        self.assertIn("care_home_features", df.columns)

        rows = df.collect()

        row_1 = next(row for row in rows if row.locationid == "1-1783948")
        row_2 = next(row for row in rows if row.locationid == "1-108950835")
        row_3 = next(row for row in rows if row.locationid == "1-1060912125")

        self.assertIsNotNone(row_1.care_home_features)
        self.assertIsNotNone(row_2.care_home_features)
        self.assertIsNotNone(row_3.care_home_features)

    def test_main_adds_non_res_pir_vectorized_column(self):
        df = locations_feature_engineering.main(self.PREPARED_LOCATIONS_TEST_DATA)
        self.assertIn("non_residential_inc_pir_features", df.columns)

        rows = df.collect()

        row_1 = next(row for row in rows if row.locationid == "1-348374832")
        row_2 = next(row for row in rows if row.locationid == "1-10235302415")
        row_3 = next(row for row in rows if row.locationid == "1-108369587")

        self.assertIsNotNone(row_1.non_residential_inc_pir_features)
        self.assertIsNotNone(row_2.non_residential_inc_pir_features)
        self.assertIsNotNone(row_3.non_residential_inc_pir_features)

    def test_main_feature_columns_are_null_if_non_residential_and_no_pir(self):
        df = locations_feature_engineering.main(self.PREPARED_LOCATIONS_TEST_DATA)

        rows = df.collect()

        row_1 = next(row for row in rows if row.locationid == "1-10894414510")
        row_2 = next(row for row in rows if row.locationid == "1-108387554")

        self.assertIsNone(row_1.non_residential_inc_pir_features)
        self.assertIsNone(row_1.care_home_features)
        self.assertIsNone(row_2.non_residential_inc_pir_features)
        self.assertIsNone(row_2.care_home_features)

    def test_main_processes_only_new_data(self):
        generate_location_features_file_parquet(self.OUTPUT_DESTINATION)
        generate_prepared_locations_file_parquet(
            self.PREPARED_LOCATIONS_TEST_DATA,
            append=True,
            partitions=["2022", "02", "28"],
        )
        generate_prepared_locations_file_parquet(
            self.PREPARED_LOCATIONS_TEST_DATA,
            append=True,
            partitions=["2022", "01", "28"],
        )
        df = locations_feature_engineering.main(
            self.PREPARED_LOCATIONS_TEST_DATA, self.OUTPUT_DESTINATION
        )

        self.assertEqual(df.count(), 14)

    # OTHER METHODS TEST

    def test_filter_records_since_snapshot_date_if_date_is_noene_does_nothing(self):
        result_df = locations_feature_engineering.filter_records_since_snapshot_date(
            self.test_df, None
        )

        self.assertEqual(result_df.count(), self.test_df.count())

    def test_filter_records_since_snapshot_date_removes_data_on_before_date(self):
        df = self.test_df.union(
            generate_prepared_locations_file_parquet(
                append=True,
                partitions=["2022", "01", "28"],
            )
        )
        df = df.union(
            generate_prepared_locations_file_parquet(
                append=True,
                partitions=["2022", "04", "01"],
            )
        )
        result_df = locations_feature_engineering.filter_records_since_snapshot_date(
            df, ("2022", "03", "08")
        )

        self.assertEqual(result_df.count(), 14)

    def test_explode_column_using_dictionary_creates_a_column_for_each_service(self):
        services_dict = feature_engineering_dictionaries.SERVICES_LOOKUP
        df = locations_feature_engineering.explode_array_column_using_dictionary(
            self.test_df, "services_offered", services_dict
        )

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

    def test_explode_column_creates_a_column_for_each_category_in_dictionary(self):
        rural_ind_dict = feature_engineering_dictionaries.RURAL_URBAN_INDICATOR_LOOKUP
        df = locations_feature_engineering.explode_string_column_using_dictionary(
            self.test_df, "rural_urban_indicator.year_2011", rural_ind_dict
        )

        expected_columns = [f"indicator_{i}" for i in range(1, 11)]

        for column in expected_columns:
            self.assertIn(column, df.columns)

        rows = df.select(expected_columns).collect()

        self.assertEqual(rows[0].indicator_1, 1)

        self.assertEqual(rows[8].indicator_4, 1)

    def test_vectorize_adds_new_care_home_features_column(self):
        services_dict = feature_engineering_dictionaries.SERVICES_LOOKUP
        df = locations_feature_engineering.explode_array_column_using_dictionary(
            self.test_df, "services_offered", services_dict
        )
        # fmt: off
        features = [
            'service_1', 'service_2', 'service_3', 'service_4', 'service_5', 'service_6', 'service_7', 'service_8',
            'service_9', 'service_10', 'service_11', 'service_12', 'service_13', 'service_14', 'service_15', 'service_16',
            'service_17', 'service_18', 'service_19', 'service_20', 'service_21', 'service_22', 'service_23', 'service_24',
            'service_25', 'service_26', 'service_27', 'service_28', 'service_29'
        ]
        # fmt: on
        df = locations_feature_engineering.vectorize_features(
            df, features, "care_home_features"
        )

        self.assertIn("care_home_features", df.columns)
        self.assertIsInstance(df.first()["care_home_features"], SparseVector)

    def test_explode_column_returns_codifies_categories(self):
        _, regions = locations_feature_engineering.explode_column(
            self.test_df, "ons_region"
        )

        self.assertIn("south_east", regions)
        self.assertIn("south_west", regions)
        self.assertIn("yorkshire_and_the_humbler", regions)
        self.assertIn("pseudo_wales", regions)

        self.assertNotIn("South East", regions)
        self.assertNotIn("South West", regions)
        self.assertNotIn("Yorkshire and The Humbler", regions)
        self.assertNotIn("(pseudo) Wales", regions)

    def test_explode_column_returns_codifies_categories_for_any_column(self):
        _, local_authorities = locations_feature_engineering.explode_column(
            self.test_df, "local_authority"
        )

        self.assertIn("surrey", local_authorities)
        self.assertIn("gloucestershire", local_authorities)
        self.assertIn("lewisham", local_authorities)

        self.assertNotIn("Surrey", local_authorities)
        self.assertNotIn("Gloucestershire", local_authorities)
        self.assertNotIn("Lewisham", local_authorities)

    def test_explode_column_returns_distinct_categories(self):
        df, ons_regions = locations_feature_engineering.explode_column(
            self.test_df, "ons_region"
        )

        self.assertIn("unspecified", ons_regions)
        self.assertEqual(df.collect()[9].unspecified, 1)

    def test_explode_column_returns_marks_missing_category_as_unspecified(self):
        _, ons_regions = locations_feature_engineering.explode_column(
            self.test_df, "ons_region"
        )

        self.assertEqual(len(ons_regions), 7)

    def test_explode_column_creates_a_column_for_each_category(self):
        df, ons_regions = locations_feature_engineering.explode_column(
            self.test_df, "ons_region"
        )

        for region in ons_regions:
            self.assertIn(region, df.columns)

        rows = df.select(ons_regions).collect()

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
