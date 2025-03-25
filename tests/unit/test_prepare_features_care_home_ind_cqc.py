import unittest
import warnings
from unittest.mock import ANY, Mock, patch

import jobs.prepare_features_care_home_ind_cqc as job
from tests.test_file_data import CareHomeFeaturesData as Data
from tests.test_file_schemas import CareHomeFeaturesSchema as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


class CareHomeFeaturesIndCqcFilledPosts(unittest.TestCase):
    IND_FILLED_POSTS_CLEANED_DIR = "source_dir"
    CARE_HOME_FEATURES_DIR = "destination_dir"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.clean_merged_data_rows, Schemas.clean_merged_data_schema
        )

        warnings.simplefilter("ignore", ResourceWarning)

    @patch("jobs.prepare_features_care_home_ind_cqc.vectorise_dataframe")
    @patch("jobs.prepare_features_care_home_ind_cqc.cap_integer_at_max_value")
    @patch("jobs.prepare_features_care_home_ind_cqc.add_date_index_column")
    @patch("jobs.prepare_features_care_home_ind_cqc.add_array_column_count")
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.select_rows_with_non_null_value")
    @patch("utils.utils.select_rows_with_value")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        select_rows_with_value_mock: Mock,
        select_rows_with_non_null_value_mock: Mock,
        write_to_parquet_mock: Mock,
        add_array_column_count_mock: Mock,
        add_date_index_column_mock: Mock,
        cap_integer_at_max_value_mock: Mock,
        vectorise_dataframe_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.IND_FILLED_POSTS_CLEANED_DIR,
            self.CARE_HOME_FEATURES_DIR,
        )

        read_from_parquet_mock.assert_called_once_with(
            self.IND_FILLED_POSTS_CLEANED_DIR
        )
        self.assertEqual(select_rows_with_value_mock.call_count, 1)
        self.assertEqual(select_rows_with_non_null_value_mock.call_count, 1)
        self.assertEqual(add_array_column_count_mock.call_count, 2)
        self.assertEqual(add_date_index_column_mock.call_count, 1)
        self.assertEqual(cap_integer_at_max_value_mock.call_count, 2)
        self.assertEqual(vectorise_dataframe_mock.call_count, 1)
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.CARE_HOME_FEATURES_DIR,
            mode="overwrite",
            partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
        )
