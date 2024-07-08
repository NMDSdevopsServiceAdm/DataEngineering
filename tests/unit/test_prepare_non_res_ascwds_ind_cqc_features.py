import unittest
import warnings
from unittest.mock import ANY, Mock, patch

import jobs.prepare_non_res_ascwds_ind_cqc_features as job
from utils import utils

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

from tests.test_file_data import NonResAscwdsWithDormancyFeaturesData as Data
from tests.test_file_schemas import NonResAscwdsWithDormancyFeaturesSchema as Schemas


class NonResLocationsFeatureEngineeringTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/source"
    OUTPUT_DESTINATION = "some/destination"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(Data.rows, Schemas.basic_schema)

        warnings.simplefilter("ignore", ResourceWarning)

    @patch("utils.utils.write_to_parquet")
    @patch(
        "jobs.prepare_non_res_ascwds_inc_dormancy_ind_cqc_features.vectorise_dataframe"
    )
    @patch(
        "jobs.prepare_non_res_ascwds_inc_dormancy_ind_cqc_features.add_date_diff_into_df"
    )
    @patch(
        "jobs.prepare_non_res_ascwds_inc_dormancy_ind_cqc_features.convert_categorical_variable_to_binary_variables_based_on_a_dictionary"
    )
    @patch(
        "jobs.prepare_non_res_ascwds_inc_dormancy_ind_cqc_features.column_expansion_with_dict"
    )
    @patch(
        "jobs.prepare_non_res_ascwds_inc_dormancy_ind_cqc_features.add_service_count_to_data"
    )
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        add_service_count_to_data_mock: Mock,
        column_expansion_with_dict_mock: Mock,
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary_mock: Mock,
        add_date_diff_into_df_mock: Mock,
        vectorise_dataframe_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.CLEANED_IND_CQC_TEST_DATA,
            self.OUTPUT_DESTINATION,
        )

        self.assertEqual(add_service_count_to_data_mock.call_count, 1)
        self.assertEqual(column_expansion_with_dict_mock.call_count, 1)
        self.assertEqual(
            convert_categorical_variable_to_binary_variables_based_on_a_dictionary_mock.call_count,
            3,
        )
        self.assertEqual(add_date_diff_into_df_mock.call_count, 1)
        self.assertEqual(vectorise_dataframe_mock.call_count, 1)

        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.OUTPUT_DESTINATION,
            mode=ANY,
            partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_is_filtering_out_rows_missing_data_for_features(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(self.CLEANED_IND_CQC_TEST_DATA, self.OUTPUT_DESTINATION)

        result = write_to_parquet_mock.call_args[0][0]

        self.assertEqual(self.test_df.count(), 10)
        self.assertEqual(result.count(), 6)

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
