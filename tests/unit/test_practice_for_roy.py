import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

import jobs.practice_for_roy as job

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

class TestModelInterpolation(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_interpolation").getOrCreate()
        
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_select_la_area_year_imd_score_has_correct_columns(self):
              
        rows = [
            ("Leeds", 2019, 2.5, 100.0, 50.0),
            ("Leeds", 2020, 2.5, 20.0, 13.0),
            ("York", 2019, 2.5, 30.0, 14.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.IMD_SCORE, FloatType(), True),
                StructField(DP.DPRS_ADASS, FloatType(), True),
                StructField(DP.DPRS_EMPLOYING_STAFF_ADASS, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.select_la_area_year_imd_score(df)

        output_df_columns = output_df.columns()

        self.assertEqual(
            output_df_columns, [DP.LA_AREA, DP.YEAR, DP.IMD_SCORE]
        )

    def test_filter_to_leeds_local_authority_has_two_rows(self):
              
        rows = [
            ("Leeds", 2019, 2.5, 100.0, 50.0),
            ("Leeds", 2020, 2.5, 20.0, 13.0),
            ("York", 2019, 2.5, 30.0, 14.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.IMD_SCORE, FloatType(), True),
                StructField(DP.DPRS_ADASS, FloatType(), True),
                StructField(DP.DPRS_EMPLOYING_STAFF_ADASS, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.filter_to_leeds_local_authority(df)

        output_df_rows = output_df.count()

        self.assertEqual(
            output_df_rows, 2
        )


    def test_calculate_proportion_employing_staff(self):
              
        rows = [
            ("Leeds", 2019, 2.5, 100.0, 50.0),
            ("Leeds", 2020, 2.5, 20.0, 5.0),
            ("York", 2019, 2.5, 28.0, 14.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.IMD_SCORE, FloatType(), True),
                StructField(DP.DPRS_ADASS, FloatType(), True),
                StructField(DP.DPRS_EMPLOYING_STAFF_ADASS, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_proportion_employing_staff(df)

        output_df_list = output_df.collect()

        self.assertEqual(
            output_df_list[0][DP.EMPLOYING_STAFF],
            0.5,
        )
        self.assertEqual(
            output_df_list[1][DP.EMPLOYING_STAFF],
            0.25,
        )
        self.assertEqual(
            output_df_list[2][DP.EMPLOYING_STAFF],
            0.5,
        )

