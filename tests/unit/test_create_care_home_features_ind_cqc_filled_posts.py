import shutil
import unittest
import warnings
from unittest.mock import ANY, Mock, patch

from pyspark.sql import functions as F
from pyspark.ml.linalg import SparseVector

import jobs.create_care_home_features_ind_cqc_filled_posts as job
from utils import utils
from utils.feature_engineering_dictionaries import (
    SERVICES_LOOKUP,
    RURAL_URBAN_INDICATOR_LOOKUP,
)
from tests.test_file_data import CareHomeFeaturesData as Data
from tests.test_file_schemas import CareHomeFeaturesSchema as Schemas


class CareHomeFeaturesIndCqcFilledPosts(unittest.TestCase):
    IND_FILLED_POSTS_CLEANED_DIR = "source_dir"
    CARE_HOME_FEATURES_DIR = "destination_dir"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(Data.clean_merged_data_rows, Schemas.clean_merged_data_schema)


    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.IND_FILLED_POSTS_CLEANED_DIR,
            self.CARE_HOME_FEATURES_DIR,
        )

        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.CARE_HOME_FEATURES_DIR,
            mode=ANY,
            partitionKeys=["year", "month", "day", "import_date"],
        )


    def test_create_care_home_features_produces_dataframe_with_features(self):
        result = job.create_care_home_features(self.test_df, SERVICES_LOOKUP, RURAL_URBAN_INDICATOR_LOOKUP)

        expected_features = SparseVector(
            43, [8, 11, 12, 13, 42], [1.0, 10.0, 1.0, 1.0, 1.0]
        )
        actual_features = result.select(F.col("features")).collect()[0].features
        self.assertEqual(actual_features, expected_features)

    @unittest.skip("needs_refactoring")
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


if __name__ == "__main__":
    unittest.main(warnings="ignore")
