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
        output_df = job.calculate_pa_ratio(df, self.spark)
        output_rows = output_df.count()

        self.assertGreaterEqual(output_rows, 0)

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

        self.assertEqual(output_rows, expected_rows)

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

        self.assertEqual(output_rows[0][DP.AVERAGE_STAFF], expected_rows[0])
        self.assertEqual(output_rows[1][DP.AVERAGE_STAFF], expected_rows[1])
        self.assertEqual(output_rows[2][DP.AVERAGE_STAFF], expected_rows[2])
        self.assertEqual(len(output_rows), len(expected_rows))

    def test_estimate_ratios(self):
        rows = [
            (2021, 1.8),
            (2022, 1.8),
            (2020, 1.8),
            (2012, 1.5),
            (2019, 1.8),
            (2013, 1.5),
            (2023, 1.85)
        ]
        test_schema = StructType(
            [
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.AVERAGE_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.estimate_ratios(df, self.spark)
        output_rows = output_df.sort(DP.YEAR_AS_INTEGER).collect()
        expected_rows = [1.5, 1.5, 1.5, 1.55, 1.6, 1.65, 1.7, 1.75, 1.8, 1.8, 1.8, 1.8, 1.85]

        self.assertAlmostEqual(output_rows[0][DP.AVERAGE_STAFF], expected_rows[0])
        self.assertAlmostEqual(output_rows[1][DP.AVERAGE_STAFF], expected_rows[1])
        self.assertAlmostEqual(output_rows[2][DP.AVERAGE_STAFF], expected_rows[2])
        self.assertAlmostEqual(output_rows[3][DP.AVERAGE_STAFF], expected_rows[3])
        self.assertAlmostEqual(output_rows[4][DP.AVERAGE_STAFF], expected_rows[4])
        self.assertAlmostEqual(output_rows[5][DP.AVERAGE_STAFF], expected_rows[5])
        self.assertAlmostEqual(output_rows[6][DP.AVERAGE_STAFF], expected_rows[6])
        self.assertAlmostEqual(output_rows[7][DP.AVERAGE_STAFF], expected_rows[7])
        self.assertAlmostEqual(output_rows[8][DP.AVERAGE_STAFF], expected_rows[8])
        self.assertAlmostEqual(output_rows[9][DP.AVERAGE_STAFF], expected_rows[9])
        self.assertAlmostEqual(output_rows[10][DP.AVERAGE_STAFF], expected_rows[10])
        self.assertAlmostEqual(output_rows[11][DP.AVERAGE_STAFF], expected_rows[11])
        self.assertAlmostEqual(output_rows[12][DP.AVERAGE_STAFF], expected_rows[12])
        self.assertEqual(len(output_rows), len(expected_rows))

    def test_create_year_range(self):
        min_year = 2019
        max_year = 2022
        step_size_in_years = 1
        output = job.create_year_range(min_year, max_year, step_size_in_years)
        expected = [2019, 2020, 2021, 2022]

        self.assertEqual(output[0], expected[0])
        self.assertEqual(output[1], expected[1])
        self.assertEqual(output[2], expected[2])
        self.assertEqual(output[3], expected[3])
        self.assertEqual(len(output), len(expected))

    def test_get_first_known_ratio(self):
        rows = [
            (2021, 1.8),
            (2022, 1.7),
            (2020, 1.9),
            (2019, 1.6),
        ]
        test_schema = StructType(
            [
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.AVERAGE_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output = job.get_first_known_ratio(df, first_known_year=2019)
        expected = 1.6

        self.assertAlmostEqual(output, expected)

    def test_apply_rolling_average(self):
        rows = [
            (2019, 1.0),
            (2020, 1.0),
            (2021, 1.0),
            (2022, 1.6),
            (2023, 2.2),
        ]
        test_schema = StructType(
            [
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.AVERAGE_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.apply_rolling_average(df)
        output_rows = output_df.sort(DP.YEAR_AS_INTEGER).collect()
        expected_rows = [1.0, 1.0, 1.0, 1.2, 1.6]

        self.assertAlmostEqual(output_rows[0][DP.RATIO_ROLLING_AVERAGE], expected_rows[0])
        self.assertAlmostEqual(output_rows[1][DP.RATIO_ROLLING_AVERAGE], expected_rows[1])
        self.assertAlmostEqual(output_rows[2][DP.RATIO_ROLLING_AVERAGE], expected_rows[2])
        self.assertAlmostEqual(output_rows[3][DP.RATIO_ROLLING_AVERAGE], expected_rows[3])
        self.assertAlmostEqual(output_rows[4][DP.RATIO_ROLLING_AVERAGE], expected_rows[4])
        
        self.assertEqual(len(output_rows), len(expected_rows))