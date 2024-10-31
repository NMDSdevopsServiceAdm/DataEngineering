import unittest
import warnings
from unittest.mock import ANY, Mock, patch, call

from pyspark.sql import DataFrame

import jobs.prepare_features_non_res_ascwds_ind_cqc as job
from utils import utils

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

from tests.test_file_data import NonResAscwdsWithDormancyFeaturesData as Data
from tests.test_file_schemas import NonResAscwdsWithDormancyFeaturesSchema as Schemas


class NonResLocationsFeatureEngineeringTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/source"
    WITH_DORMANCY_DESTINATION = "some/destination"
    WITHOUT_DORMANCY_DESTINATION = "other/destination"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(Data.rows, Schemas.basic_schema)

        warnings.simplefilter("ignore", ResourceWarning)

    @patch("utils.utils.write_to_parquet")
    @patch("jobs.prepare_features_non_res_ascwds_ind_cqc.vectorise_dataframe")
    @patch("utils.utils.select_rows_with_non_null_value")
    @patch("jobs.prepare_features_non_res_ascwds_ind_cqc.add_date_diff_into_df")
    @patch(
        "jobs.prepare_features_non_res_ascwds_ind_cqc.convert_categorical_variable_to_binary_variables_based_on_a_dictionary"
    )
    @patch("jobs.prepare_features_non_res_ascwds_ind_cqc.column_expansion_with_dict")
    @patch(
        "jobs.prepare_features_non_res_ascwds_ind_cqc.add_array_column_count_to_data"
    )
    @patch("utils.utils.select_rows_with_value")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        select_rows_with_value_mock: Mock,
        add_array_column_count_to_data_mock: Mock,
        column_expansion_with_dict_mock: Mock,
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary_mock: Mock,
        add_date_diff_into_df_mock: Mock,
        select_rows_with_non_null_value_mock: Mock,
        vectorise_dataframe_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.WITH_DORMANCY_DESTINATION,
            self.WITHOUT_DORMANCY_DESTINATION,
        )

        self.assertEqual(select_rows_with_value_mock.call_count, 1)
        self.assertEqual(add_array_column_count_to_data_mock.call_count, 3)
        self.assertEqual(column_expansion_with_dict_mock.call_count, 1)
        self.assertEqual(
            convert_categorical_variable_to_binary_variables_based_on_a_dictionary_mock.call_count,
            3,
        )
        self.assertEqual(add_date_diff_into_df_mock.call_count, 1)
        self.assertEqual(select_rows_with_non_null_value_mock.call_count, 1)
        self.assertEqual(vectorise_dataframe_mock.call_count, 2)

        write_to_parquet_calls = [
            call(
                ANY,
                self.WITH_DORMANCY_DESTINATION,
                mode="overwrite",
                partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
            ),
            call(
                ANY,
                self.WITHOUT_DORMANCY_DESTINATION,
                mode="overwrite",
                partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
            ),
        ]

        write_to_parquet_mock.assert_has_calls(write_to_parquet_calls)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_is_filtering_out_rows_missing_data_for_features(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.WITH_DORMANCY_DESTINATION,
            self.WITHOUT_DORMANCY_DESTINATION,
        )

        result_with_dormancy: DataFrame = write_to_parquet_mock.call_args_list[0][0][0]
        result_without_dormancy: DataFrame = write_to_parquet_mock.call_args_list[1][0][
            0
        ]

        self.assertEqual(self.test_df.count(), 10)
        self.assertEqual(result_with_dormancy.count(), 6)
        self.assertEqual(result_without_dormancy.count(), 7)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
