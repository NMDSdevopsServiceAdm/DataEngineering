import unittest
import warnings
from unittest.mock import ANY, Mock, patch, call

from pyspark.sql import DataFrame

import jobs.prepare_features_non_res_ascwds_ind_cqc as job
from tests.test_file_data import NonResAscwdsFeaturesData as Data
from tests.test_file_schemas import NonResAscwdsFeaturesSchema as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

PATCH_PATH: str = "jobs.prepare_features_non_res_ascwds_ind_cqc"


class NonResLocationsFeatureEngineeringTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/source"
    WITH_DORMANCY_DESTINATION = "with_dormancy/destination"
    WITHOUT_DORMANCY_DESTINATION = "without_dormancy/destination"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(Data.rows, Schemas.basic_schema)

        warnings.simplefilter("ignore", ResourceWarning)

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_non_null_value")
    @patch(f"{PATCH_PATH}.vectorise_dataframe")
    @patch(f"{PATCH_PATH}.add_date_index_column")
    @patch(f"{PATCH_PATH}.filter_without_dormancy_features_to_pre_2025")
    @patch(f"{PATCH_PATH}.group_rural_urban_sparse_categories")
    @patch(f"{PATCH_PATH}.expand_encode_and_extract_features")
    @patch(f"{PATCH_PATH}.cap_integer_at_max_value")
    @patch(f"{PATCH_PATH}.add_array_column_count")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_value")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        select_rows_with_value_mock: Mock,
        add_array_column_count_mock: Mock,
        cap_integer_at_max_value_mock: Mock,
        expand_encode_and_extract_features_mock: Mock,
        group_rural_urban_sparse_categories_mock: Mock,
        filter_without_dormancy_features_to_pre_2025_mock: Mock,
        add_date_index_column_mock: Mock,
        vectorise_dataframe_mock: Mock,
        select_rows_with_non_null_value_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df
        expand_encode_and_extract_features_mock.return_value = (
            self.test_df,
            ["some_feature"],
        )

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.WITH_DORMANCY_DESTINATION,
            self.WITHOUT_DORMANCY_DESTINATION,
        )

        write_to_parquet_calls = [
            call(
                ANY,
                self.WITHOUT_DORMANCY_DESTINATION,
                mode="overwrite",
                partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
            ),
            call(
                ANY,
                self.WITH_DORMANCY_DESTINATION,
                mode="overwrite",
                partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
            ),
        ]

        select_rows_with_value_mock.assert_called_once()
        self.assertEqual(add_array_column_count_mock.call_count, 3)
        self.assertEqual(cap_integer_at_max_value_mock.call_count, 4)
        self.assertEqual(expand_encode_and_extract_features_mock.call_count, 6)
        group_rural_urban_sparse_categories_mock.assert_called_once()
        filter_without_dormancy_features_to_pre_2025_mock.assert_called_once()
        self.assertEqual(add_date_index_column_mock.call_count, 2)
        self.assertEqual(vectorise_dataframe_mock.call_count, 2)
        select_rows_with_non_null_value_mock.assert_called_once()
        write_to_parquet_mock.assert_has_calls(write_to_parquet_calls)

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_is_filtering_out_rows_missing_data_for_features(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.WITH_DORMANCY_DESTINATION,
            self.WITHOUT_DORMANCY_DESTINATION,
        )

        result_with_dormancy: DataFrame = write_to_parquet_mock.call_args_list[1][0][0]
        result_without_dormancy: DataFrame = write_to_parquet_mock.call_args_list[0][0][
            0
        ]

        self.assertEqual(self.test_df.count(), 10)
        self.assertEqual(result_with_dormancy.count(), 6)
        self.assertEqual(result_without_dormancy.count(), 7)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
