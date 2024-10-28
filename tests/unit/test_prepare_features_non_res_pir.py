import unittest
import warnings
from unittest.mock import ANY, Mock, patch

from pyspark.sql import DataFrame

import jobs.prepare_features_non_res_pir as job
from tests.test_file_data import NonResPirFeaturesData as Data
from tests.test_file_schemas import NonResPirFeaturesSchema as Schemas
from utils import utils


class NonResLocationsFeatureEngineeringTests(unittest.TestCase):
    CLEANED_IMPORT_DATA = "some/source"
    NON_RES_PIR_FEATURE_DESTINATION = "some/destination"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.feature_rows, Schemas.features_schema
        )

        warnings.simplefilter("ignore", ResourceWarning)

    @patch("utils.utils.write_to_parquet")
    @patch("jobs.prepare_features_non_res_pir.vectorise_dataframe")
    @patch("utils.utils.select_rows_with_non_null_value")
    @patch("utils.utils.select_rows_with_value")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        select_rows_with_value_mock: Mock,
        select_rows_with_non_null_value_mock: Mock,
        vectorise_dataframe_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.CLEANED_IMPORT_DATA,
            self.NON_RES_PIR_FEATURE_DESTINATION,
        )

        self.assertEqual(read_from_parquet_mock.call_count, 1)
        self.assertEqual(select_rows_with_value_mock.call_count, 1)
        self.assertEqual(select_rows_with_non_null_value_mock.call_count, 1)
        self.assertEqual(vectorise_dataframe_mock.call_count, 1)

        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.NON_RES_PIR_FEATURE_DESTINATION,
            mode="overwrite",
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_is_filtering_out_rows_missing_data_for_features(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.CLEANED_IMPORT_DATA,
            self.NON_RES_PIR_FEATURE_DESTINATION,
        )

        result: DataFrame = write_to_parquet_mock.call_args[0][0]

        self.assertEqual(self.test_df.count(), 4)
        self.assertEqual(result.count(), 2)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
