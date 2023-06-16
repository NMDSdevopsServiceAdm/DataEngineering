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

    def test_filter_to_locations_with_a_known_service_users_employing_staff_filters_correctly(self):
        rows = [
            ("area_1", 2021, 300.0, 0.3),
            ("area_2", 2021, 300.0, 0.4),
            ("area_1", 2020, None, 0.45),
            ("area_2", 2020, 300.0, 0.35),
            ("area_1", 2019, None, 0.375),
            ("area_2", 2019, 300.0, 0.2),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.filter_to_locations_with_known_service_users_employing_staff(df)

        self.assertEqual(output_df.count(), 4)
        self.assertEqual(
            output_df.columns,
            [DP.LA_AREA, DP.YEAR_AS_INTEGER, DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF],
        )

    def test_calculate_first_and_last_submission_date_per_location(self):
        rows = [
            ("area_1", 2021, 300.0),
            ("area_2", 2021, 300.0),
            ("area_2", 2020, 300.0),
            ("area_2", 2019, 300.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_first_and_last_submission_year_per_la_area(df)

        self.assertEqual(output_df.count(), 2)
        self.assertEqual(
            output_df.columns,
            [DP.LA_AREA, DP.FIRST_SUBMISSION_YEAR, DP.LAST_SUBMISSION_YEAR],
        )

        output_df = output_df.sort(DP.LA_AREA).collect()
        self.assertEqual(output_df[0][DP.FIRST_SUBMISSION_YEAR], 2021)
        self.assertEqual(output_df[0][DP.LAST_SUBMISSION_YEAR], 2021)
        self.assertEqual(output_df[1][DP.FIRST_SUBMISSION_YEAR], 2019)
        self.assertEqual(output_df[1][DP.LAST_SUBMISSION_YEAR], 2021)
