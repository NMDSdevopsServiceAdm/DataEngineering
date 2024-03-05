import datetime
import unittest
import warnings
from unittest.mock import Mock, patch

from pyspark.ml.linalg import SparseVector
from pyspark.sql import functions as F

import jobs.prepare_non_res_ind_cqc_features as job
from tests.test_file_data import PrepareNonResData as Data
from tests.test_file_schemas import PrepareNonResSchemas as Schemas
from utils import utils
from utils.features.helper import add_date_diff_into_df


class LocationsFeatureEngineeringTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/source"
    OUTPUT_DESTINATION = "some/destination"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(Data.rows, Schemas.basic_schema)
        warnings.simplefilter("ignore", ResourceWarning)

    def test_add_date_diff_into_df(self):
        df = self.spark.createDataFrame(
            [["01-10-2013"], ["01-10-2023"]], ["test_input"]
        )
        df = df.select(
            F.col("test_input"),
            F.to_date(F.col("test_input"), "MM-dd-yyyy").alias("import_date"),
        )
        result = add_date_diff_into_df(
            df=df, new_col_name="diff", snapshot_date_col="import_date"
        )
        expected_max_date = datetime.date(2023, 1, 10)
        actual_max_date = result.agg(F.max("import_date")).first()[0]

        expected_diff_between_max_date_and_other_date = 3652
        actual_diff = (
            result.filter(F.col("test_input") == "01-10-2013")
            .select(F.col("diff"))
            .collect()
        )

        self.assertEqual(actual_max_date, expected_max_date)
        self.assertEqual(
            actual_diff[0].diff, expected_diff_between_max_date_and_other_date
        )

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_produces_dataframe_with_features(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(self.CLEANED_IND_CQC_TEST_DATA, self.OUTPUT_DESTINATION)

        result = write_to_parquet_mock.call_args[0][0].orderBy(F.col("locationid"))

        self.assertTrue(result.filter(F.col("features").isNull()).count() == 0)
        expected_features = SparseVector(
            46, [0, 3, 13, 15, 18, 19, 45], [100.0, 1.0, 1.0, 17.0, 1.0, 1.0, 2.0]
        )
        actual_features = result.select(F.col("features")).collect()[0].features
        print(actual_features)
        print(expected_features)
        self.assertEqual(actual_features, expected_features)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_is_filtering_out_rows_missing_data_for_features(
        self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock
    ):
        read_from_parquet_mock.return_value = self.test_df

        input_df_length = self.test_df.count()
        self.assertEqual(input_df_length, 14)

        job.main(self.CLEANED_IND_CQC_TEST_DATA, self.OUTPUT_DESTINATION)

        result = write_to_parquet_mock.call_args[0][0]

        self.assertEqual(result.count(), 7)

    def test_filter_locations_df_for_non_res_care_home_data(self):
        cols = ["carehome", "cqc_sector"]
        rows = [
            ("Y", "Independent"),
            ("N", "Independent"),
            ("Y", "local authority"),
            ("Y", ""),
            ("Y", None),
        ]

        df = self.spark.createDataFrame(rows, cols)

        result = job.filter_locations_df_for_independent_non_res_care_home_data(
            df=df, carehome_col_name="carehome", cqc_col_name="cqc_sector"
        )
        result_row = result.collect()
        self.assertTrue(result.count() == 1)
        self.assertEqual(
            result_row[0].asDict(), {"carehome": "N", "cqc_sector": "Independent"}
        )
