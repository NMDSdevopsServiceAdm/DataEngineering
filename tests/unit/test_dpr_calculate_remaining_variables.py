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

import utils.direct_payments_utils.estimate_direct_payments.calculate_remaining_variables as job
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestCalculateRemainingVariables(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_calculate_remaining_variables").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_remaining_variables_completes(self):
        rows = [
            ("area_1", 2021, 0.49, 400.0, 30.0, 800.0, 770.0),
            ("area_2", 2021, 0.34, 390.0, 32.0, 850.0, 818.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.TOTAL_DPRS_DURING_YEAR, FloatType(), True),
                StructField(
                    DP.SERVICE_USER_DPRS_DURING_YEAR,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_remaining_variables(df)

        self.assertEqual(output_df.count(), df.count())

    def test_calculate_service_users_with_self_employed_staff_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 0.49, 400.0, 30.0, 800.0, 770.0),
            ("area_2", 2021, 0.34, 390.0, 32.0, 850.0, 818.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.TOTAL_DPRS_DURING_YEAR, FloatType(), True),
                StructField(
                    DP.SERVICE_USER_DPRS_DURING_YEAR,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_service_users_with_self_employed_staff(df)
        output_df_list = output_df.sort(DP.LA_AREA).collect()
        self.assertAlmostEqual(
            output_df_list[0][DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF], 13.82054836444813, places=5
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF], 14.68208904171243, places=5
        )

    def test_calculate_carers_employing_staff_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 0.49, 400.0, 30.0, 800.0, 770.0),
            ("area_2", 2021, 0.34, 390.0, 32.0, 850.0, 818.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.TOTAL_DPRS_DURING_YEAR, FloatType(), True),
                StructField(
                    DP.SERVICE_USER_DPRS_DURING_YEAR,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_carers_employing_staff(df)
        output_df_list = output_df.sort(DP.LA_AREA).collect()
        self.assertAlmostEqual(output_df_list[0][DP.ESTIMATED_CARERS_EMPLOYING_STAFF], 0.191616868609776, places=5)
        self.assertAlmostEqual(output_df_list[1][DP.ESTIMATED_CARERS_EMPLOYING_STAFF], 0.2043913265170944, places=5)

    def test_calculate_total_dpr_employing_staff_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 0.49, 400.0, 30.0, 800.0),
            ("area_2", 2021, 0.34, 390.0, 32.0, 850.0),
            ("area_1", 2020, 0.45, 380.0, 34.0, 1100.0),
            ("area_2", 2020, 0.79, 370.0, 36.0, 670.0),
            ("area_1", 2019, 0.27, 360.0, 38.0, 530.0),
            ("area_2", 2019, 0.45, 350.0, 40.0, 640.0),
            ("area_1", 2018, 0.37, 340.0, 42.0, 1000.0),
            ("area_2", 2018, 0.63, 330.0, 44.0, 800.0),
            ("area_1", 2017, 0.33, 320.0, 46.0, 900.0),
            ("area_2", 2017, 0.34, 310.0, 50.0, 1000.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.TOTAL_DPRS_DURING_YEAR, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_total_dpr_employing_staff(df)

        self.assertEqual(output_df.count(), df.count())

    def test_calculate_total_personal_assistant_filled_posts_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 0.49, 400.0, 30.0, 800.0),
            ("area_2", 2021, 0.34, 390.0, 32.0, 850.0),
            ("area_1", 2020, 0.45, 380.0, 34.0, 1100.0),
            ("area_2", 2020, 0.79, 370.0, 36.0, 670.0),
            ("area_1", 2019, 0.27, 360.0, 38.0, 530.0),
            ("area_2", 2019, 0.45, 350.0, 40.0, 640.0),
            ("area_1", 2018, 0.37, 340.0, 42.0, 1000.0),
            ("area_2", 2018, 0.63, 330.0, 44.0, 800.0),
            ("area_1", 2017, 0.33, 320.0, 46.0, 900.0),
            ("area_2", 2017, 0.34, 310.0, 50.0, 1000.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.TOTAL_DPRS_DURING_YEAR, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_total_personal_assistant_filled_posts(df)

        self.assertEqual(output_df.count(), df.count())

    def test_calculate_proportion_of_dpr_employing_staff_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 0.49, 400.0, 30.0, 800.0),
            ("area_2", 2021, 0.34, 390.0, 32.0, 850.0),
            ("area_1", 2020, 0.45, 380.0, 34.0, 1100.0),
            ("area_2", 2020, 0.79, 370.0, 36.0, 670.0),
            ("area_1", 2019, 0.27, 360.0, 38.0, 530.0),
            ("area_2", 2019, 0.45, 350.0, 40.0, 640.0),
            ("area_1", 2018, 0.37, 340.0, 42.0, 1000.0),
            ("area_2", 2018, 0.63, 330.0, 44.0, 800.0),
            ("area_1", 2017, 0.33, 320.0, 46.0, 900.0),
            ("area_2", 2017, 0.34, 310.0, 50.0, 1000.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.TOTAL_DPRS_DURING_YEAR, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_proportion_of_dpr_employing_staff(df)

        self.assertEqual(output_df.count(), df.count())
