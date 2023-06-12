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

import utils.direct_payments_utils.estimate_direct_payments.models.mean_imputation as job
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)
from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


class TestDetermineAreasIncludingCarers(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_mean_imputation").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_model_when_no_historical_data_completes(self):
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
        output_df = job.model_when_no_historical_data(df)
        self.assertEqual(df.count(), output_df.count())

    def test_filter_to_most_recent_year_removes_all_years_except_most_recent(self):
        rows = [
            ("area_1", 2020),
            ("area_2", 2021),
            ("area_3", 2020),
            ("area_4", 2021),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        filtered_df = job.filter_to_most_recent_year(df)

        filtered_df_list = filtered_df.sort(DP.LA_AREA).collect()

        self.assertEqual(filtered_df_list[0][DP.YEAR], 2021)
        self.assertEqual(filtered_df_list[1][DP.YEAR], 2021)
        self.assertEqual(filtered_df.count(), 2)

    def test_calculate_mean_proportion_of_service_users_employing_staff_returns_mean(
        self,
    ):
        rows = [
            ("area_1", 2021, 300.0, 0.3),
            ("area_2", 2021, 300.0, 0.4),
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
        output = job.calculate_mean_proportion_of_service_users_employing_staff(df)
        self.assertAlmostEqual(output, 0.35, places=5)

    def test_calculate_estimated_service_user_dprs_during_year_employing_staff_using_mean(
        self,
    ):
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
        mean = 0.35
        output_df = job.calculate_estimated_service_user_dprs_during_year_employing_staff_using_mean(df, mean)
        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(
            output_df_list[0][DP.ESTIMATE_USING_MEAN],
            105.0,
        )
        self.assertEqual(
            output_df_list[1][DP.ESTIMATE_USING_MEAN],
            105.0,
        )
        self.assertEqual(
            output_df_list[2][DP.ESTIMATE_USING_MEAN],
            105.0,
        )
        self.assertEqual(
            output_df_list[3][DP.ESTIMATE_USING_MEAN],
            105.0,
        )
        self.assertEqual(
            output_df_list[4][DP.ESTIMATE_USING_MEAN],
            105.0,
        )
        self.assertEqual(
            output_df_list[5][DP.ESTIMATE_USING_MEAN],
            105.0,
        )
        self.assertEqual(output_df.count(), 6)
        self.assertEqual(df.count(), output_df.count())
