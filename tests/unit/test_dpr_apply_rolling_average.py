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

import utils.direct_payments_utils.estimate_direct_payments.apply_rolling_average as job
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)
from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


class TestApplyRollingAverage(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_apply_rolling_average").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_apply_rolling_average_completes(self):
        rows = [
            ("area_1", 2021, 0.25),
            ("area_1", 2020, 0.45),
            ("area_1", 2019, 0.46),
            ("area_1", 2018, 0.51),
            ("area_1", 2017, 0.37),
            ("area_1", 2016, 0.42),
            ("area_2", 2021, 0.62),
            ("area_2", 2020, 0.80),
            ("area_2", 2019, 0.54),
            ("area_2", 2018, 0.74),
            ("area_2", 2017, 0.65),
            ("area_2", 2016, 0.62),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.apply_rolling_average(df)
        self.assertEqual(df.count(), output_df.count())

    def test_calculate_aggregates_per_year_returns_aggregated_df(self):
        rows = [
            ("area_1", 2021, 0.25),
            ("area_1", 2020, 0.45),
            ("area_1", 2019, 0.46),
            ("area_1", 2018, 0.51),
            ("area_1", 2017, 0.37),
            ("area_1", 2016, 0.42),
            ("area_2", 2021, 0.62),
            ("area_2", 2020, 0.80),
            ("area_2", 2019, 0.54),
            ("area_2", 2018, 0.74),
            ("area_2", 2017, 0.65),
            ("area_2", 2016, 0.62),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_aggregates_per_year(df)
        self.assertEqual(df.count(), output_df.count())

    def test_create_rolling_average_column_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 0.25, 1.0, 0.25),
            ("area_1", 2020, 0.45, 1.0, 0.45),
            ("area_1", 2019, 0.46, 1.0, 0.46),
            ("area_1", 2018, 0.51, 1.0, 0.51),
            ("area_1", 2017, 0.37, 1.0, 0.37),
            ("area_1", 2016, 0.42, 1.0, 0.42),
            ("area_2", 2021, 0.62, 1.0, 0.62),
            ("area_2", 2020, 0.80, 1.0, 0.80),
            ("area_2", 2019, 0.54, 1.0, 0.54),
            ("area_2", 2018, 0.74, 1.0, 0.74),
            ("area_2", 2017, 0.65, 1.0, 0.65),
            ("area_2", 2016, 0.62, 1.0, 0.62),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.create_rolling_average_column(df)
        self.assertEqual(df.count(), output_df.count())

    def test_join_rolling_average_into_df_returns_correct_columns(self):
        rolling_avg_rows = [
            ("area_1", 2021, 0.25, 0.39),
            ("area_1", 2020, 0.45, 0.47),
            ("area_1", 2019, 0.46, 0.45),
            ("area_1", 2018, 0.51, 0.43),
            ("area_1", 2017, 0.37, 0.40),
            ("area_1", 2016, 0.42, 0.42),
            ("area_2", 2021, 0.62, 0.65),
            ("area_2", 2020, 0.80, 0.69),
            ("area_2", 2019, 0.54, 0.64),
            ("area_2", 2018, 0.74, 0.67),
            ("area_2", 2017, 0.65, 0.64),
            ("area_2", 2016, 0.62, 0.62),
        ]
        rolling_avg_test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
                StructField(
                    DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True
                ),
            ]
        )
        df_rows = [
            ("area_1", 2021, 0.25, 6.0, 2.46),
            ("area_1", 2020, 0.45, 6.0, 2.46),
            ("area_1", 2019, 0.46, 6.0, 2.46),
            ("area_1", 2018, 0.51, 6.0, 2.46),
            ("area_1", 2017, 0.37, 6.0, 2.46),
            ("area_1", 2016, 0.42, 6.0, 2.46),
            ("area_2", 2021, 0.62, 6.0, 3.97),
            ("area_2", 2020, 0.80, 6.0, 3.97),
            ("area_2", 2019, 0.54, 6.0, 3.97),
            ("area_2", 2018, 0.74, 6.0, 3.97),
            ("area_2", 2017, 0.65, 6.0, 3.97),
            ("area_2", 2016, 0.62, 6.0, 3.97),
        ]
        df_test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        rolling_avg_df = self.spark.createDataFrame(rolling_avg_rows, schema=rolling_avg_test_schema)
        df = self.spark.createDataFrame(df_rows, schema=df_test_schema)
        output_df = job.join_rolling_average_into_df(df, rolling_avg_df)
        self.assertEqual(df.count(), output_df.count())

    def test_apply_rolling_average_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 0.25),
            ("area_1", 2020, 0.45),
            ("area_1", 2019, 0.46),
            ("area_1", 2018, 0.51),
            ("area_1", 2017, 0.37),
            ("area_1", 2016, 0.42),
            ("area_2", 2021, 0.62),
            ("area_2", 2020, 0.80),
            ("area_2", 2019, 0.54),
            ("area_2", 2018, 0.74),
            ("area_2", 2017, 0.65),
            ("area_2", 2016, 0.62),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.apply_rolling_average(df)
        self.assertEqual(df.count(), output_df.count())
