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
import projects._04_direct_payment_recipients.utils._03_estimate_direct_payment_utils.estimate_service_users_employing_staff as job
from projects._04_direct_payment_recipients.tests.utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestEstimateServiceUsersEmployingStaff(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_apply_models_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 0.49, None, 0.4),
            ("area_2", 2021, None, None, 0.4),
            ("area_1", 2020, 0.45, 0.45, 0.5),
            ("area_2", 2020, None, None, 0.5),
            ("area_1", 2019, None, 0.41, 0.6),
            ("area_2", 2019, None, None, 0.6),
            ("area_1", 2018, 0.37, 0.37, 0.6),
            ("area_2", 2018, None, None, 0.6),
            ("area_1", 2017, 0.33, None, 0.6),
            ("area_2", 2017, None, None, 0.6),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATE_USING_EXTRAPOLATION_RATIO,
                    FloatType(),
                    True,
                ),
                StructField(DP.ESTIMATE_USING_INTERPOLATION, FloatType(), True),
                StructField(DP.ESTIMATE_USING_MEAN, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.apply_models(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertAlmostEqual(
            output_df_list[0][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.33,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.37,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[2][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.41,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[3][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.45,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[4][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.49,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[5][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.6,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[6][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.6,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[7][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.6,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[8][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[9][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.4,
            places=5,
        )
        self.assertEqual(output_df.count(), df.count())

    def test_merge_in_historical_estimates_with_estimate_using_mean_selects_correct_values(
        self,
    ):
        rows = [
            ("area_1", "2020", 100.0, None),
            ("area_2", "2021", None, 90.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, StringType(), True),
                StructField(DP.ESTIMATE_USING_MEAN, FloatType(), True),
                StructField(
                    DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.merge_in_historical_estimates_with_estimate_using_mean(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.ESTIMATE_USING_MEAN], 100.0)
        self.assertEqual(output_df_list[1][DP.ESTIMATE_USING_MEAN], 90.0)
        self.assertEqual(output_df.count(), 2)

    def test_calculate_estimated_number_of_service_users_employing_staff_returns_correct_values(
        self,
    ):
        rows = [
            ("area_1", "2020", 100.0, 0.5),
            ("area_2", "2021", 150.0, 0.6),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, StringType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(
                    DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_estimated_number_of_service_users_employing_staff(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(
            output_df_list[0][
                DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF
            ],
            50.0,
        )
        self.assertEqual(
            output_df_list[1][
                DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF
            ],
            90.0,
        )
        self.assertEqual(output_df.count(), 2)
