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

import utils.direct_payments_utils.estimate_direct_payments.models.mean_imputation as job
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestMeanImputation(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_mean_imputation").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_model_when_no_historical_data_completes(self):
        rows = [
            ("area_1", 2021, 300.0, 0.3),
            ("area_2", 2021, 300.0, 0.4),
            ("area_3", 2020, 300.0, 0.45),
            ("area_4", 2020, 300.0, 0.35),
            ("area_5", 2019, 300.0, 0.375),
            ("area_6", 2019, 300.0, 0.2),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.model_using_mean(df)
        self.assertEqual(df.count(), output_df.count())

    def test_model_using_mean_returns_mean(
        self,
    ):
        rows = [
            ("area_1", 2021, 300.0, 0.3),
            ("area_2", 2021, 300.0, 0.4),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.model_using_mean(df)
        output_df_list = output_df.collect()
        self.assertAlmostEqual(output_df_list[0][DP.ESTIMATE_USING_MEAN], 0.35, places=5)
        self.assertAlmostEqual(output_df_list[1][DP.ESTIMATE_USING_MEAN], 0.35, places=5)
