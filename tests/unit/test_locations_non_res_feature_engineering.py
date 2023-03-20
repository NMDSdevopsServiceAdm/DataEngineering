import datetime
import shutil
import unittest
import warnings

from pyspark.ml.linalg import SparseVector
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from jobs import locations_non_res_feature_engineering
from jobs.locations_non_res_feature_engineering import filter_locations_df_for_independent_non_res_care_home_data
from tests.test_file_generator import (
    generate_prepared_locations_file_parquet,
)
from utils.features.helper import add_date_diff_into_df


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
