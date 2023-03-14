import datetime
import shutil
import unittest
import warnings

from pyspark.ml.linalg import SparseVector
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from jobs import locations_non_res_feature_engineering
from jobs.locations_non_res_feature_engineering import (
    explode_column_from_distinct_values,
    format_strings,
    filter_locations_df_for_independent_non_res_care_home_data,
    add_service_count_to_data,
    add_date_diff_into_df,
    add_rui_data_data_frame,
)
from tests.test_file_generator import (
    generate_prepared_locations_file_parquet,
)


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

    def test_add_date_diff_into_df(self):

        df = self.spark.createDataFrame(
            [["01-10-2013"], ["01-10-2023"]], ["test_input"]
        )
        df = df.select(
            F.col("test_input"),
            F.to_date(F.col("test_input"), "MM-dd-yyyy").alias("snapshot_date"),
        )
        result = add_date_diff_into_df(
            df=df, new_col_name="diff", snapshot_date_col="snapshot_date"
        )
        expected_max_date = datetime.date(2023, 1, 10)
        actual_max_date = result.agg(F.max("snapshot_date")).first()[0]

        expected_diff_between_max_date_and_other_date = 3652
        actual_diff = (
            result.filter(F.col("test_input") == "01-10-2013")
            .select(F.col("diff"))
            .collect()
        )

        self.assertEqual(actual_max_date, expected_max_date)
        self.assertEqual(
            actual_diff[0].diff, expected_diff_between_max_date_and_other_date
        )

    def test_main_produces_dataframe_with_features(self):
        result = locations_non_res_feature_engineering.main(
            self.PREPARED_LOCATIONS_TEST_DATA, self.OUTPUT_DESTINATION
        ).orderBy(F.col("locationid"))

        self.assertTrue(result.filter(F.col("features").isNull()).count() == 0)
        expected_features = SparseVector(
            46, [0, 3, 13, 15, 18, 19, 45], [100.0, 1.0, 1.0, 17.0, 1.0, 1.0, 2.0]
        )
        actual_features = result.select(F.col("features")).collect()[0].features
        self.assertEqual(actual_features, expected_features)

    def test_main_is_filtering_out_rows_missing_data_for_features(self):
        input_df_length = self.test_df.count()
        self.assertTrue(input_df_length, 14)

        result = locations_non_res_feature_engineering.main(
            self.PREPARED_LOCATIONS_TEST_DATA, self.OUTPUT_DESTINATION
        )

        self.assertTrue(result.count() == 7)

    def test_add_rui_data_data_frame(self):
        cols = ["location", "rural_urban_indicator"]
        rows = [
            (
                "1-10894414510",
                {
                    "year_2011": "(England/Wales) Rural hamlet and isolated dwellings in a sparse setting"
                },
            )
        ]
        df = self.spark.createDataFrame(rows, cols)
        result = add_rui_data_data_frame(
            df=df,
            new_rui_col_name="rui_2011",
            col_to_check="rural_urban_indicator.year_2011",
            lookup_dict={
                "indicator_1": "(England/Wales) Rural hamlet and isolated dwellings in a sparse setting"
            },
        )
        result_rows = result.collect()
        self.assertEqual(result_rows[0].indicator_1, 1)

    def test_add_service_count_to_data(self):
        cols = ["location", "services"]
        new_col_name = "service_count"
        rows = [("1", ["service_1", "service_2", "service_3"]), ("2", ["service_1"])]
        df = self.spark.createDataFrame(rows, cols)

        result = add_service_count_to_data(
            df=df, new_col_name=new_col_name, col_to_check="services"
        )
        rows = result.collect()

        self.assertTrue(
            rows[0].asDict()
            == {
                "location": "1",
                "services": ["service_1", "service_2", "service_3"],
                "service_count": 3,
            }
        )
        self.assertTrue(
            rows[1].asDict()
            == {"location": "2", "services": ["service_1"], "service_count": 1}
        )

    def test_filter_locations_df_for_non_res_care_home_data(self):
        cols = ["carehome", "cqc_sector"]
        rows = [
            ("Y", "Independent"),
            ("N", "Independent"),
            ("Y", "local authority"),
            ("Y", ""),
            ("Y", None),
        ]

        df = self.spark.createDataFrame(rows, cols)

        result = filter_locations_df_for_independent_non_res_care_home_data(
            df=df, carehome_col_name="carehome", cqc_col_name="cqc_sector"
        )
        result_row = result.collect()
        self.assertTrue(result.count() == 1)
        self.assertEqual(
            result_row[0].asDict(), {"carehome": "N", "cqc_sector": "Independent"}
        )

    def test_format_strings(self):
        string_1 = "lots of spaces"
        string_2 = "CAPITALS"
        string_3 = "Punctuation?!()"

        self.assertEqual("lots_of_spaces", format_strings(string_1))
        self.assertEqual("capitals", format_strings(string_2))
        self.assertEqual("punctuation", format_strings(string_3))

    def test_explode_column_from_distinct_values_returns_df_with_new_cols_and_prefix(
        self,
    ):
        val_1 = "Glasgow"
        val_2 = "London"
        val_3 = "Leeds"

        col_list = {val_1, val_2, val_3}

        cols = ["locationid", "region"]
        rows = [
            ("1", val_1),
            ("2", val_1),
            ("3", val_2),
            ("4", val_3),
            ("5", val_3),
        ]
        df = self.spark.createDataFrame(rows, cols)

        result = explode_column_from_distinct_values(
            df=df, column_name="region", col_prefix="ons_", col_list_set=col_list
        )
        df_rows = result[0].collect()
        list_created_of_new_cols = result[1]
        list_created_of_new_cols.sort()

        expected_returned_rows = [
            {
                "locationid": "1",
                "region": "Glasgow",
                "ons_leeds": 0,
                "ons_london": 0,
                "ons_glasgow": 1,
            },
            {
                "locationid": "2",
                "region": "Glasgow",
                "ons_leeds": 0,
                "ons_london": 0,
                "ons_glasgow": 1,
            },
            {
                "locationid": "3",
                "region": "London",
                "ons_leeds": 0,
                "ons_london": 1,
                "ons_glasgow": 0,
            },
            {
                "locationid": "4",
                "region": "Leeds",
                "ons_leeds": 1,
                "ons_london": 0,
                "ons_glasgow": 0,
            },
            {
                "locationid": "5",
                "region": "Leeds",
                "ons_leeds": 1,
                "ons_london": 0,
                "ons_glasgow": 0,
            },
        ]

        actual_returned_rows = [row.asDict() for row in df_rows]

        self.assertTrue(len(result[0].columns) == 5)
        self.assertEqual(len(df_rows), len(rows))
        self.assertListEqual(actual_returned_rows, expected_returned_rows)
        self.assertEqual(
            list_created_of_new_cols, ["ons_glasgow", "ons_leeds", "ons_london"]
        )
