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

from utils.prepare_direct_payments_utils.determine_areas_including_carers_on_adass import (
    DIFFERENCE_IN_BASES_THRESHOLD,
    PROPORTION_EMPLOYING_STAFF_THRESHOLD,
    determine_areas_including_carers_on_adass,
    filter_to_most_recent_year,
    calculate_total_dprs_during_year,
    calculate_service_users_employing_staff,
    calculate_carers_employing_staff,
    calculate_service_users_and_carers_employing_staff,
    calculate_difference_between_survey_base_and_total_dpr_during_year,
)
from utils.prepare_direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestDetermineAreasIncludingCarers(unittest.TestCase):
    determine_areas_including_carers_schema = StructType(
        [
            StructField(DP.LA_AREA, StringType(), False),
            StructField(DP.YEAR, IntegerType(), True),
            StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
            StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
        ]
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_areas_including_carers").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

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
        filtered_df = filter_to_most_recent_year(df)

        filtered_df_list = filtered_df.sort(DP.LA_AREA).collect()

        self.assertEqual(filtered_df_list[0][DP.YEAR], 2021)
        self.assertEqual(filtered_df_list[1][DP.YEAR], 2021)
        self.assertEqual(filtered_df.count(), 2)

    def test_calculate_total_dprs_during_year_sums_su_and_carers_during_year_returns_correct_sum(
        self,
    ):
        rows = [
            ("area_1", 200.0, 5.0),
            ("area_2", 100.0, 10.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        total_dprs_df = calculate_total_dprs_during_year(df)

        total_dprs_df_list = total_dprs_df.sort(DP.LA_AREA).collect()

        self.assertEqual(total_dprs_df_list[0][DP.TOTAL_DPRS_DURING_YEAR], 205.0)
        self.assertEqual(total_dprs_df_list[1][DP.TOTAL_DPRS_DURING_YEAR], 110.0)

    def test_calculate_service_users_employing_staff_returns_correct_product(self):
        rows = [
            ("area_1", 200.0, 0.5),
            ("area_2", 100.0, 0.25),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = calculate_service_users_employing_staff(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.SERVICE_USERS_EMPLOYING_STAFF], 100.0)
        self.assertEqual(output_df_list[1][DP.SERVICE_USERS_EMPLOYING_STAFF], 25.0)

    def test_calculate_carers_employing_staff_returns_correct_product(self):
        rows = [
            ("area_1", 5.0, 0.5),
            ("area_2", 10.0, 0.25),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = calculate_carers_employing_staff(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.CARERS_EMPLOYING_STAFF], 2.5)
        self.assertEqual(output_df_list[1][DP.CARERS_EMPLOYING_STAFF], 2.5)

    def test_calculate_service_users_and_carers_employing_staff_returns_correct_sum(
        self,
    ):
        rows = [
            ("area_1", 100.0, 2.5),
            ("area_2", 25.0, 2.5),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.CARERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = calculate_service_users_and_carers_employing_staff(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF], 102.5)
        self.assertEqual(output_df_list[1][DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF], 27.5)

    def test_difference_between_survey_base_and_total_dpr_during_year_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", 120.0, 102.5),
            ("area_2", 25.0, 27.5),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.DPRS_EMPLOYING_STAFF_ADASS, FloatType(), True),
                StructField(DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = calculate_difference_between_survey_base_and_total_dpr_during_year(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.DIFFERENCE_IN_BASES], 17.5)
        self.assertEqual(output_df_list[1][DP.DIFFERENCE_IN_BASES], 2.5)

    def test_difference_in_bases_threshold_is_correct_value(
        self,
    ):

        self.assertEqual(DIFFERENCE_IN_BASES_THRESHOLD, 10.0)

    def test_proportion_emplying_staff_threshold_is_correct_value(
        self,
    ):

        self.assertEqual(PROPORTION_EMPLOYING_STAFF_THRESHOLD, 0.3)
