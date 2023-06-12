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

import utils.direct_payments_utils.estimate_direct_payments.models.backwards_extrapolation_ratio as job
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)
from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


class TestDetermineAreasIncludingCarers(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_backwards_extrapolation_ratio").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_model_extrapolation_backwards_completes(self):
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
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.model_extrapolation_backwards(df)
        self.assertEqual(df.count(), output_df.count())

    def test_add_column_with_year_as_integer_adds_same_value_as_integer(self):
        rows = [
            ("area_1", "2020"),
            ("area_2", "2021"),
            ("area_3", "2020"),
            ("area_4", "2021"),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.add_column_with_year_as_integer(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.YEAR_AS_INTEGER], 2021)
        self.assertEqual(output_df_list[1][DP.YEAR_AS_INTEGER], 2021)
        self.assertEqual(output_df_list[2][DP.YEAR_AS_INTEGER], 2021)
        self.assertEqual(output_df_list[2][DP.YEAR_AS_INTEGER], 2021)
        self.assertEqual(output_df.count(), 4)

    def test_add_column_with_first_year_of_data_returns_first_year_as_integer(
        self,
    ):
        rows = [
            ("area_1", 2021, 300.0, 0.3),
            ("area_2", 2021, 300.0, 0.4),
            ("area_1", 2020, 300.0, 0.3),
            ("area_2", 2020, 300.0, None),
            ("area_1", 2019, 300.0, 0.3),
            ("area_2", 2019, 300.0, None),
            ("area_1", 2018, 300.0, None),
            ("area_2", 2018, 300.0, None),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.add_column_with_first_year_of_data(df)
        output_df_list = output_df.sort(DP.LA_AREA).collect()
        self.assertEqual(output_df_list[0][DP.FIRST_YEAR_WITH_DATA], 2019)
        self.assertEqual(output_df_list[1][DP.FIRST_YEAR_WITH_DATA], 2019)
        self.assertEqual(output_df_list[2][DP.FIRST_YEAR_WITH_DATA], 2019)
        self.assertEqual(output_df_list[3][DP.FIRST_YEAR_WITH_DATA], 2019)
        self.assertEqual(output_df_list[4][DP.FIRST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[5][DP.FIRST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[6][DP.FIRST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[7][DP.FIRST_YEAR_WITH_DATA], 2021)

    def test_add_column_with_percentage_service_users_employing_staff_in_first_year_of_data_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", 2021, 300.0, 0.3, 2019),
            ("area_2", 2021, 300.0, 0.4, 2020),
            ("area_1", 2020, 300.0, 0.3, 2019),
            ("area_2", 2020, 300.0, None, 2020),
            ("area_1", 2019, 300.0, 0.3, 2019),
            ("area_2", 2019, 300.0, None, 2020),
            ("area_1", 2018, 300.0, None, 2019),
            ("area_2", 2018, 300.0, None, 2020),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.FIRST_YEAR_WITH_DATA, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.add_column_with_percentage_service_users_employing_staff_in_first_year_of_data(df)
        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(
            output_df_list[0][DP.FIRST_DATA_POINT],
            0.3,
        )
        self.assertEqual(
            output_df_list[1][DP.FIRST_DATA_POINT],
            0.3,
        )
        self.assertEqual(
            output_df_list[2][DP.FIRST_DATA_POINT],
            0.3,
        )
        self.assertEqual(
            output_df_list[3][DP.FIRST_DATA_POINT],
            0.3,
        )
        self.assertEqual(
            output_df_list[4][DP.FIRST_DATA_POINT],
            0.4,
        )
        self.assertEqual(
            output_df_list[5][DP.FIRST_DATA_POINT],
            0.4,
        )
        self.assertEqual(
            output_df_list[6][DP.FIRST_DATA_POINT],
            0.4,
        )
        self.assertEqual(
            output_df_list[7][DP.FIRST_DATA_POINT],
            0.4,
        )

def test_calculate_rolling_average_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", 2021, 300.0, 0.3, 2019, 0.3),
            ("area_2", 2021, 300.0, 0.4, 2020, 0.4),
            ("area_1", 2020, 300.0, 0.3, 2019, 0.3),
            ("area_2", 2020, 300.0, None, 2020, 0.4),
            ("area_1", 2019, 300.0, 0.3, 2019, 0.3),
            ("area_2", 2019, 300.0, None, 2020, 0.4),
            ("area_1", 2018, 300.0, None, 2019, 0.3),
            ("area_2", 2018, 300.0, None, 2020, 0.4),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.FIRST_YEAR_WITH_DATA, FloatType(), True),
                StructField(DP.FIRST_DATA_POINT, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_rolling_average(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR).collect()

        self.assertEqual(
            output_df_list[0][DP.EXTRAPOLATION_RATIO],
            ,
        )
        

def test_calculate_extrapolation_ratio_for_earlier_years_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", 2021, 300.0, 0.3, 2019, 0.3),
            ("area_2", 2021, 300.0, 0.4, 2020, 0.4),
            ("area_1", 2020, 300.0, 0.3, 2019, 0.3),
            ("area_2", 2020, 300.0, None, 2020, 0.4),
            ("area_1", 2019, 300.0, 0.3, 2019, 0.3),
            ("area_2", 2019, 300.0, None, 2020, 0.4),
            ("area_1", 2018, 300.0, None, 2019, 0.3),
            ("area_2", 2018, 300.0, None, 2020, 0.4),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.FIRST_YEAR_WITH_DATA, FloatType(), True),
                StructField(DP.FIRST_DATA_POINT, FloatType(), True),
                #rolling avg?
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_extrapolation_ratio_for_earlier_years(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR).collect()

        self.assertEqual(
            output_df_list[0][DP.EXTRAPOLATION_RATIO],
            ,
        )
        
def test_calculate_ratio_estimates_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", 2021, 300.0, 0.3, 2019, 0.3),
            ("area_2", 2021, 300.0, 0.4, 2020, 0.4),
            ("area_1", 2020, 300.0, 0.3, 2019, 0.3),
            ("area_2", 2020, 300.0, None, 2020, 0.4),
            ("area_1", 2019, 300.0, 0.3, 2019, 0.3),
            ("area_2", 2019, 300.0, None, 2020, 0.4),
            ("area_1", 2018, 300.0, None, 2019, 0.3),
            ("area_2", 2018, 300.0, None, 2020, 0.4),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.FIRST_YEAR_WITH_DATA, FloatType(), True),
                StructField(DP.FIRST_DATA_POINT, FloatType(), True),
                # ratio?
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_ratio_estimates(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR).collect()

        self.assertEqual(
            output_df_list[0][DP.EXTRAPOLATION_RATIO],
            ,
        )
        