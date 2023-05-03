import shutil
import unittest
import warnings

from pyspark.ml.linalg import SparseVector
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import jobs.locations_care_home_feature_engineering as job

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

    def test_main_produces_dataframe_with_features(self):
        result = job.main(self.PREPARED_LOCATIONS_TEST_DATA, self.OUTPUT_DESTINATION)

        expected_features = SparseVector(
            43, [8, 11, 12, 13, 42], [1.0, 10.0, 1.0, 1.0, 1.0]
        )
        actual_features = result.select(F.col("features")).collect()[0].features
        self.assertEqual(actual_features, expected_features)

    def test_main_is_filtering_out_rows_missing_data_for_features(self):
        input_df_length = self.test_df.count()
        self.assertTrue(input_df_length, 14)

        result = job.main(self.PREPARED_LOCATIONS_TEST_DATA, self.OUTPUT_DESTINATION)

        self.assertTrue(result.count() == 1)

    def test_filter_locations_df_for_independent_care_home_data(self):
        cols = ["carehome", "cqc_sector"]
        rows = [
            ("Y", "Independent"),
            ("N", "Independent"),
            ("Y", "local authority"),
            ("Y", ""),
            ("Y", None),
        ]

        df = self.spark.createDataFrame(rows, cols)

        result = job.filter_locations_df_for_independent_care_home_data(
            df=df, carehome_col_name="carehome", cqc_col_name="cqc_sector"
        )
        result_row = result.collect()
        self.assertTrue(result.count() == 1)
        self.assertEqual(
            result_row[0].asDict(), {"carehome": "Y", "cqc_sector": "Independent"}
        )
