import unittest
import warnings

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

from utils import utils
import projects._04_direct_payment_recipients.utils._03_estimate_direct_payment_utils.calculate_remaining_variables as job
from projects._04_direct_payment_recipients.tests.utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestCalculateRemainingVariables(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_remaining_variables_completes(self):
        rows = [
            ("area_1", 2021, 0.49, 400.0, 30.0, 800.0, 770.0, 1.5),
            ("area_2", 2021, 0.34, 390.0, 32.0, 850.0, 818.0, 1.75),
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
                StructField(
                    DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.TOTAL_DPRS_DURING_YEAR, FloatType(), True),
                StructField(
                    DP.SERVICE_USER_DPRS_DURING_YEAR,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.FILLED_POSTS_PER_EMPLOYER,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_remaining_variables(df)

        self.assertEqual(output_df.count(), df.count())

    def test_calculate_service_users_with_self_employed_staff_returns_correct_values(
        self,
    ):
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
                StructField(
                    DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
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
            output_df_list[0][DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF],
            13.82054836444813,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF],
            14.68208904171243,
            places=5,
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
                StructField(
                    DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
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
        self.assertAlmostEqual(
            output_df_list[0][DP.ESTIMATED_CARERS_EMPLOYING_STAFF],
            0.191616868609776,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.ESTIMATED_CARERS_EMPLOYING_STAFF],
            0.2043913265170944,
            places=5,
        )

    def test_calculate_total_dpr_employing_staff_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 0.49, 400.0, 13.0, 0.1900000),
            ("area_2", 2021, 0.34, 390.0, 14.0, 0.2000000),
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
                StructField(
                    DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.ESTIMATED_CARERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_total_dpr_employing_staff(df)
        output_df_list = output_df.sort(DP.LA_AREA).collect()
        self.assertAlmostEqual(
            output_df_list[0][DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF], 413.19, places=5
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF], 404.20, places=4
        )

    def test_calculate_total_personal_assistant_filled_posts_returns_correct_values(
        self,
    ):
        rows = [
            ("area_1", 2021, 400.0, 1.5),
            ("area_2", 2021, 390.0, 1.75),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.FILLED_POSTS_PER_EMPLOYER, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_total_personal_assistant_filled_posts(df)
        output_df_list = output_df.sort(DP.LA_AREA).collect()
        self.assertAlmostEqual(
            output_df_list[0][DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS],
            600.0,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS],
            682.5,
            places=5,
        )

    def test_calculate_proportion_of_dpr_employing_staff_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 400.0, 800.0),
            ("area_2", 2021, 390.0, 850.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.TOTAL_DPRS_DURING_YEAR, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_proportion_of_dpr_employing_staff(df)
        output_df_list = output_df.sort(DP.LA_AREA).collect()
        self.assertAlmostEqual(
            output_df_list[0][DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF],
            0.5,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF],
            0.4588235294117647,
            places=5,
        )

    def test_calculate_proportion_of_dpr_who_are_service_users(self):
        rows = [
            ("area_1", 2021, 400.0, 800.0),
            ("area_2", 2021, 390.0, 850.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.SERVICE_USER_DPRS_DURING_YEAR,
                    FloatType(),
                    True,
                ),
                StructField(DP.TOTAL_DPRS_DURING_YEAR, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_proportion_of_dpr_who_are_service_users(df)
        output_df_list = output_df.sort(DP.LA_AREA).collect()
        self.assertAlmostEqual(
            output_df_list[0][DP.ESTIMATED_PROPORTION_OF_DPR_WHO_ARE_SERVICE_USERS],
            0.5,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.ESTIMATED_PROPORTION_OF_DPR_WHO_ARE_SERVICE_USERS],
            0.4588235294117647,
            places=5,
        )
