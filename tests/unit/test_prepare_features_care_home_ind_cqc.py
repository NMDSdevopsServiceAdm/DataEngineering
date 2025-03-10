import unittest
import warnings
from unittest.mock import ANY, Mock, patch

from pyspark.sql import DataFrame, functions as F
from pyspark.ml.linalg import SparseVector

import jobs.prepare_features_care_home_ind_cqc as job
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
    @patch("jobs.prepare_features_care_home_ind_cqc.vectorise_dataframe")
    @patch(
        "jobs.prepare_features_care_home_ind_cqc.convert_categorical_variable_to_binary_variables_based_on_a_dictionary"
    )
    @patch("jobs.prepare_features_care_home_ind_cqc.column_expansion_with_dict")
    @patch("jobs.prepare_features_care_home_ind_cqc.add_array_column_count")
    @patch("utils.utils.select_rows_with_value")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        select_rows_with_value_mock: Mock,
        add_array_column_count_mock: Mock,
        column_expansion_with_dict_mock: Mock,
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary_mock: Mock,
        vectorise_dataframe_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.IND_FILLED_POSTS_CLEANED_DIR,
            self.CARE_HOME_FEATURES_DIR,
        )

        self.assertEqual(select_rows_with_value_mock.call_count, 1)
        self.assertEqual(add_array_column_count_mock.call_count, 1)
        self.assertEqual(column_expansion_with_dict_mock.call_count, 1)
        self.assertEqual(
            convert_categorical_variable_to_binary_variables_based_on_a_dictionary_mock.call_count,
            2,
        )
        self.assertEqual(vectorise_dataframe_mock.call_count, 1)

        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.CARE_HOME_FEATURES_DIR,
            mode="overwrite",
            partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_produces_dataframe_with_features(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(self.IND_FILLED_POSTS_CLEANED_DIR, self.CARE_HOME_FEATURES_DIR)

        result: DataFrame = write_to_parquet_mock.call_args[0][0].orderBy(
            F.col(IndCQC.location_id)
        )

        self.assertTrue(result.filter(F.col(IndCQC.features).isNull()).count() == 0)
        expected_features = SparseVector(
            51, [0, 9, 10, 17, 24, 31], [10.0, 1.0, 2.5, 1.0, 1.0, 1.0]
        )
        actual_features = result.select(F.col(IndCQC.features)).collect()[0].features
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

        result: DataFrame = write_to_parquet_mock.call_args[0][0]

        self.assertEqual(result.count(), 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
