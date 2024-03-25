import unittest
import warnings
from unittest.mock import ANY, Mock, patch

from pyspark.sql import functions as F
from pyspark.ml.linalg import SparseVector

import jobs.prepare_care_home_ind_cqc_features as job
from utils import utils

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
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

        warnings.simplefilter("ignore", ResourceWarning)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
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
            partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_produces_dataframe_with_expected_features(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        read_from_parquet_mock.return_value = self.test_df
        expected_feature_list = [
            "date_diff",
            "indicator_1",
            "indicator_10",
            "indicator_2",
            "indicator_3",
            "indicator_4",
            "indicator_5",
            "indicator_6",
            "indicator_7",
            "indicator_8",
            "indicator_9",
            "numberofbeds",
            "ons_east_midlands",
            "ons_eastern",
            "ons_london",
            "ons_north_east",
            "ons_north_west",
            "ons_south_east",
            "ons_south_west",
            "ons_west_midlands",
            "ons_yorkshire_and_the_humber",
            "service_1",
            "service_10",
            "service_11",
            "service_12",
            "service_13",
            "service_14",
            "service_15",
            "service_16",
            "service_17",
            "service_18",
            "service_19",
            "service_2",
            "service_20",
            "service_21",
            "service_22",
            "service_23",
            "service_24",
            "service_25",
            "service_26",
            "service_27",
            "service_28",
            "service_29",
            "service_3",
            "service_4",
            "service_5",
            "service_6",
            "service_7",
            "service_8",
            "service_9",
            "service_count",
        ]

        returned_feature_list = job.main(
            self.IND_FILLED_POSTS_CLEANED_DIR, self.CARE_HOME_FEATURES_DIR
        )

        self.assertEqual(returned_feature_list, expected_feature_list)

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
        self.assertEqual(input_df_length, 8)

        job.main(self.IND_FILLED_POSTS_CLEANED_DIR, self.CARE_HOME_FEATURES_DIR)

        result = write_to_parquet_mock.call_args[0][0]

        self.assertEqual(result.count(), 1)

    def test_filter_df_to_care_home_only(self):
        filter_to_ind_care_home_df = self.spark.createDataFrame(
            Data.filter_to_care_home_rows, Schemas.filter_to_care_home_schema
        )
        returned_df = job.filter_df_to_care_home_only(filter_to_ind_care_home_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_filtered_to_care_home_rows,
            Schemas.filter_to_care_home_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
