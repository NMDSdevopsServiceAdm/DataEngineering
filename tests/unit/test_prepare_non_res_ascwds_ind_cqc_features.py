import unittest
import warnings
from unittest.mock import ANY, Mock, patch, call

import jobs.prepare_non_res_ascwds_ind_cqc_features as job
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
    @patch("jobs.prepare_non_res_ascwds_ind_cqc_features.vectorise_dataframe")
    @patch("jobs.prepare_non_res_ascwds_ind_cqc_features.add_date_diff_into_df")
    @patch(
        "jobs.prepare_non_res_ascwds_ind_cqc_features.convert_categorical_variable_to_binary_variables_based_on_a_dictionary"
    )
    @patch("jobs.prepare_non_res_ascwds_ind_cqc_features.column_expansion_with_dict")
    @patch(
        "jobs.prepare_non_res_ascwds_ind_cqc_features.add_array_column_count_to_data"
    )
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        add_array_column_count_to_data_mock: Mock,
        column_expansion_with_dict_mock: Mock,
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary_mock: Mock,
        add_date_diff_into_df_mock: Mock,
        vectorise_dataframe_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.WITH_DORMANCY_DESTINATION,
            self.WITHOUT_DORMANCY_DESTINATION,
        )

        self.assertEqual(add_array_column_count_to_data_mock.call_count, 3)
        self.assertEqual(column_expansion_with_dict_mock.call_count, 1)
        self.assertEqual(
            convert_categorical_variable_to_binary_variables_based_on_a_dictionary_mock.call_count,
            3,
        )
        self.assertEqual(add_date_diff_into_df_mock.call_count, 1)
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

        result_with_dormancy = write_to_parquet_mock.call_args_list[0][0][0]
        result_without_dormancy = write_to_parquet_mock.call_args_list[1][0][0]

        self.assertEqual(self.test_df.count(), 10)
        self.assertEqual(result_with_dormancy.count(), 6)
        self.assertEqual(result_without_dormancy.count(), 7)

    def test_filter_df_for_non_res_care_home_data(self):
        ind_cqc_df = self.spark.createDataFrame(
            Data.filter_to_non_care_home_rows, Schemas.filter_to_non_care_home_schema
        )
        returned_df = job.filter_df_to_non_res_only(ind_cqc_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_filtered_to_non_care_home_rows,
            Schemas.filter_to_non_care_home_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_filter_df_non_null_dormancy_data(self):
        ind_cqc_df = self.spark.createDataFrame(
            Data.filter_to_dormancy_rows, Schemas.filter_to_dormancy_schema
        )
        returned_df = job.filter_df_to_non_null_dormancy(ind_cqc_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_filtered_to_dormancy_rows,
            Schemas.filter_to_dormancy_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
