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

import utils.direct_payments_utils.estimate_direct_payments.create_summary_table as job
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestCreateSummaryTable(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_create_summary_table"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_create_summary_table_completes(self):
        rows = [
            (
                "area_1",
                "2021",
                0.49,
                400.0,
                0.5,
                800.0,
                770.0,
                10.5,
                3.0,
                1000.0,
                800.0,
                0.4,
            ),
            (
                "area_2",
                "2021",
                0.34,
                390.0,
                0.6,
                850.0,
                818.0,
                21.75,
                5.0,
                1000.0,
                900.0,
                0.5,
            ),
            (
                "area_3",
                "2021",
                0.34,
                390.0,
                0.7,
                850.0,
                818.0,
                11.75,
                6.0,
                1000.0,
                900.0,
                0.6,
            ),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, StringType(), True),
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
                    DP.ESTIMATED_PROPORTION_OF_DPR_WHO_ARE_SERVICE_USERS,
                    FloatType(),
                    True,
                ),
                StructField(DP.TOTAL_DPRS_DURING_YEAR, FloatType(), True),
                StructField(
                    DP.SERVICE_USER_DPRS_DURING_YEAR,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ESTIMATED_CARERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.create_summary_table(df)
        self.assertEqual(output_df.count(), 1)

    def test_create_summary_table_produces_correct_values(self):
        rows = [
            (
                "area_1",
                "2021",
                0.49,
                400.0,
                0.5,
                800.0,
                770.0,
                10.5,
                3.0,
                1000.0,
                800.0,
                0.4,
            ),
            (
                "area_2",
                "2021",
                0.34,
                390.0,
                0.6,
                850.0,
                818.0,
                21.75,
                5.0,
                1000.0,
                900.0,
                0.5,
            ),
            (
                "area_3",
                "2021",
                0.34,
                390.0,
                0.7,
                850.0,
                818.0,
                11.75,
                6.0,
                1000.0,
                900.0,
                0.6,
            ),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, StringType(), True),
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
                    DP.ESTIMATED_PROPORTION_OF_DPR_WHO_ARE_SERVICE_USERS,
                    FloatType(),
                    True,
                ),
                StructField(DP.TOTAL_DPRS_DURING_YEAR, FloatType(), True),
                StructField(
                    DP.SERVICE_USER_DPRS_DURING_YEAR,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ESTIMATED_CARERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.create_summary_table(df)
        output_df.show()
        output_df_list = output_df.collect()
        self.assertAlmostEqual(output_df_list[0]["total_dprs"], 2500.0, places=5)
        self.assertAlmostEqual(
            output_df_list[0]["proportion_of_service_user_dprs"], 0.6, places=5
        )
        self.assertAlmostEqual(output_df_list[0]["service_user_dprs"], 2406, places=5)
        self.assertAlmostEqual(
            output_df_list[0]["estimated_proportion_of_service_users_employing_staff"],
            0.39,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[0]["estimated_service_users_employing_staff"],
            1180.0,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[0]["estimated_service_users_with_self_employed_staff"],
            44.0,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[0]["estimated_carers_employing_staff"], 14.0, places=5
        )
        self.assertAlmostEqual(
            output_df_list[0]["estimated_total_dprs_employing_staff"], 3000.0, places=5
        )
        self.assertAlmostEqual(
            output_df_list[0]["estimated_total_personal_assistant_filled_posts"],
            2600,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[0]["estimated_proportion_of_total_dprs_employing_staff"],
            0.5,
            places=5,
        )
