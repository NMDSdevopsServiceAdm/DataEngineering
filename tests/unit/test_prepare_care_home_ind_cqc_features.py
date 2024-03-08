import unittest
from unittest.mock import ANY, Mock, patch

from pyspark.sql import functions as F
from pyspark.ml.linalg import SparseVector

import jobs.prepare_care_home_ind_cqc_features as job
from utils import utils

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

from tests.test_file_data import CareHomeFeaturesData as Data
from tests.test_file_schemas import CareHomeFeaturesSchema as Schemas


class CareHomeFeaturesIndCqcFilledPosts(unittest.TestCase):
    IND_FILLED_POSTS_CLEANED_DIR = "source_dir"
    CARE_HOME_FEATURES_DIR = "destination_dir"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.clean_merged_data_rows, Schemas.clean_merged_data_schema
        )
        self.filter_to_ind_care_home_df = self.spark.createDataFrame(
            Data.filter_to_ind_care_home_rows, Schemas.filter_to_ind_care_home_schema
        )

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

    def test_filter_locations_df_for_independent_care_home_data(self):
        returned_df = job.filter_locations_df_for_independent_care_home_data(
            self.filter_to_ind_care_home_df, IndCQC.care_home, IndCQC.cqc_sector
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_filtered_to_ind_care_home_rows,
            Schemas.filter_to_ind_care_home_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_produces_dataframe_with_features(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(self.IND_FILLED_POSTS_CLEANED_DIR, self.CARE_HOME_FEATURES_DIR)

        result = write_to_parquet_mock.call_args[0][0].orderBy(
            F.col(IndCQC.location_id)
        )

        self.assertTrue(result.filter(F.col("features").isNull()).count() == 0)
        expected_features = SparseVector(
            43, [8, 11, 12, 13, 42], [1.0, 10.0, 1.0, 1.0, 1.0]
        )
        actual_features = result.select(F.col("features")).collect()[0].features
        self.assertEqual(actual_features, expected_features)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_is_filtering_out_rows_missing_data_for_features(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        read_from_parquet_mock.return_value = self.test_df

        input_df_length = self.test_df.count()
        self.assertEqual(input_df_length, 14)

        job.main(self.IND_FILLED_POSTS_CLEANED_DIR, self.CARE_HOME_FEATURES_DIR)

        result = write_to_parquet_mock.call_args[0][0]

        self.assertEqual(result.count(), 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
