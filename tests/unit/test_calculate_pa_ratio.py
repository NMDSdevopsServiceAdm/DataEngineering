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

import utils.direct_payments_utils.estimate_direct_payments.calculate_pa_ratio as job
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestCalculatePARatio(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_calculate_pa_ratio"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_pa_ratio_completes(self):
        rows = [
            (2021, 1.0),
            (2021, 2.0),
            (2021, 2.0),
            (2021, 1.0),
            (2021, 1.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.TOTAL_STAFF_RECODED,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_pa_ratio(df)
        output_rows = output_df.count()

        self.assertGreaterEqual(
            output_rows, 0
        )
        
    def test_exclude_outliers(self):
        rows = [
            (2021, 10.0),
            (2021, 20.0),
            (2021, 0.0),
            (2021, 9.0),
            (2021, 1.0),
            (2021, -1.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.TOTAL_STAFF_RECODED,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.exclude_outliers(df)
        output_rows = output_df.count()
        expected_rows = 2

        self.assertEqual(
            output_rows, expected_rows
        )
    
    def test_calculate_average_ratios(self):
        rows = [
            (2021, 1.0),
            (2021, 2.0),
            (2020, 1.0),
            (2020, 1.0),
            (2019, 2.0),
            (2019, 2.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.TOTAL_STAFF_RECODED,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_average_ratios(df)
        output_rows = output_df.sort(DP.YEAR_AS_INTEGER).collect()
        expected_rows = [2.0, 1.0, 1.5]

        self.assertEqual(
            output_rows[0][DP.AVERAGE_STAFF], expected_rows[0]
        )
        self.assertEqual(
            output_rows[1][DP.AVERAGE_STAFF], expected_rows[1]
        )
        self.assertEqual(
            output_rows[2][DP.AVERAGE_STAFF], expected_rows[2]
        )
    
