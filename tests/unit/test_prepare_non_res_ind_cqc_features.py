import unittest
import warnings
from unittest.mock import ANY, Mock, patch

from pyspark.sql import functions as F
from pyspark.ml.linalg import SparseVector

import jobs.prepare_non_res_ind_cqc_features as job
from utils import utils

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)

from tests.test_file_data import NonResFeaturesData as Data
from tests.test_file_schemas import NonResFeaturesSchema as Schemas


class NonResLocationsFeatureEngineeringTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/source"
    OUTPUT_DESTINATION = "some/destination"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(Data.rows, Schemas.basic_schema)

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
            self.CLEANED_IND_CQC_TEST_DATA,
            self.OUTPUT_DESTINATION,
        )

        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.OUTPUT_DESTINATION,
            mode=ANY,
            partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_produces_dataframe_with_features(
        self,
        read_from_parquet_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(self.CLEANED_IND_CQC_TEST_DATA, self.OUTPUT_DESTINATION)

        result = write_to_parquet_mock.call_args[0][0].orderBy(
            F.col(IndCQC.location_id)
        )

        self.assertTrue(result.filter(F.col("features").isNull()).count() == 0)
        expected_features = SparseVector(
            46, [0, 3, 13, 15, 18, 19, 45], [100.0, 1.0, 1.0, 17.0, 1.0, 1.0, 2.0]
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
        self.assertEqual(input_df_length, 10)

        job.main(self.CLEANED_IND_CQC_TEST_DATA, self.OUTPUT_DESTINATION)

        result = write_to_parquet_mock.call_args[0][0]

        self.assertEqual(result.count(), 7)

    def test_filter_df_for_non_res_care_home_data(self):
        filter_to_ind_care_home_df = self.spark.createDataFrame(
            Data.filter_to_non_care_home_rows, Schemas.filter_to_non_care_home_schema
        )
        returned_df = job.filter_df_to_non_res_only(filter_to_ind_care_home_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_filtered_to_non_care_home_rows,
            Schemas.filter_to_non_care_home_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
