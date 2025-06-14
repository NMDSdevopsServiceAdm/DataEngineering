import unittest
import warnings
from unittest.mock import ANY, Mock, patch

from pyspark.sql import DataFrame, functions as F
from pyspark.ml.linalg import SparseVector

import projects._03_independent_cqc._04_feature_engineering.jobs.prepare_features_care_home_ind_cqc as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    CareHomeFeaturesData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    CareHomeFeaturesSchema as Schemas,
)

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)

PATCH_PATH: str = "projects._03_independent_cqc._04_feature_engineering.jobs.prepare_features_care_home_ind_cqc"


class CareHomeFeaturesIndCqcFilledPosts(unittest.TestCase):
    IND_FILLED_POSTS_CLEANED_DIR = "source_dir"
    CARE_HOME_FEATURES_DIR = "destination_dir"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.clean_merged_data_rows, Schemas.clean_merged_data_schema
        )

        warnings.simplefilter("ignore", ResourceWarning)

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.vectorise_dataframe")
    @patch(f"{PATCH_PATH}.expand_encode_and_extract_features")
    @patch(f"{PATCH_PATH}.cap_integer_at_max_value")
    @patch(f"{PATCH_PATH}.add_date_index_column")
    @patch(f"{PATCH_PATH}.add_array_column_count")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_non_null_value")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_value")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        select_rows_with_value_mock: Mock,
        select_rows_with_non_null_value_mock: Mock,
        add_array_column_count_mock: Mock,
        add_date_index_column_mock: Mock,
        cap_integer_at_max_value_mock: Mock,
        expand_encode_and_extract_features_mock: Mock,
        vectorise_dataframe_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df
        expand_encode_and_extract_features_mock.return_value = (
            self.test_df,
            ["some_feature"],
        )

        job.main(
            self.IND_FILLED_POSTS_CLEANED_DIR,
            self.CARE_HOME_FEATURES_DIR,
        )

        self.assertEqual(select_rows_with_value_mock.call_count, 1)
        self.assertEqual(select_rows_with_non_null_value_mock.call_count, 1)
        self.assertEqual(add_array_column_count_mock.call_count, 2)
        self.assertEqual(add_date_index_column_mock.call_count, 1)
        self.assertEqual(cap_integer_at_max_value_mock.call_count, 2)
        self.assertEqual(expand_encode_and_extract_features_mock.call_count, 4)
        self.assertEqual(vectorise_dataframe_mock.call_count, 1)

        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.CARE_HOME_FEATURES_DIR,
            mode="overwrite",
            partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
        )

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
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
            39,
            [0, 1, 2, 3, 12, 19, 23, 25, 34],
            [1.0, 1.8, 1.0, 10.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        )
        actual_features = result.select(F.col(IndCQC.features)).collect()[0].features
        self.assertEqual(actual_features, expected_features)

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_is_filtering_out_rows_missing_data_for_features(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        read_from_parquet_mock.return_value = self.test_df

        input_df_length = self.test_df.count()
        self.assertEqual(input_df_length, 3)

        job.main(self.IND_FILLED_POSTS_CLEANED_DIR, self.CARE_HOME_FEATURES_DIR)

        result: DataFrame = write_to_parquet_mock.call_args[0][0]

        self.assertEqual(result.count(), 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
