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
            output_df_columns = [DP.LA_AREA, DP.YEAR, DP.IMD_SCORE]
        )
        

