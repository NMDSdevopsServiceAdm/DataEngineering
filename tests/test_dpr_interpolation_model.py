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

import utils.direct_payments_utils.estimate_direct_payments.models.interpolation as job
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestDPRInterpolation(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_dpr_interpolation").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    @unittest.skip("to do")
    def test_model_interpolation_completes(self):
        rows = [
            ("area_1", 2021, 300.0, 0.3, 320.0),
            ("area_2", 2021, 300.0, 0.4, 320.0),
            ("area_1", 2020, 300.0, 0.45, 300.0),
            ("area_2", 2020, 300.0, 0.35, 300.0),
            ("area_1", 2019, 300.0, 0.375, 300.0),
            ("area_2", 2019, 300.0, 0.2, 300.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.ESTIMATE_USING_MEAN, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.model_interpolation(df)
        self.assertEqual(df.count(), output_df.count())
