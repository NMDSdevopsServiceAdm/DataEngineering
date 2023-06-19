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

import utils.direct_payments_utils.prepare_direct_payments.determine_areas_including_carers_on_adass as job
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)
from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


class TestDetermineAreasIncludingCarers(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_areas_including_carers").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_determine_areas_including_carers_on_adass_completes(self):
        rows = [
            ("area_1", 2021, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0, 0.5),
            ("area_2", 2021, 300.0, 50.0, 100.0, 10.0, None, 100.0, 25.0, None),
            ("area_3", 2020, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0, None),
            ("area_4", 2020, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0, None),
            ("area_5", 2019, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0, None),
            ("area_6", 2019, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0, None),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.CARER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.IMD_SCORE, FloatType(), True),
                StructField(DP.DPRS_ADASS, FloatType(), True),
                StructField(DP.DPRS_EMPLOYING_STAFF_ADASS, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.determine_areas_including_carers_on_adass(df)
        self.assertEqual(df.count(), output_df.count())

    def test_calculate_proportion_of_dprs_employing_staff_returns_correct_sum(
        self,
    ):
        rows = [
            ("area_1", 100.0, 50.0),
            ("area_2", 100.0, 25.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.DPRS_ADASS, FloatType(), True),
                StructField(DP.DPRS_EMPLOYING_STAFF_ADASS, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        total_dprs_df = job.calculate_proportion_of_dprs_employing_staff(df)

        total_dprs_df_list = total_dprs_df.sort(DP.LA_AREA).collect()

        self.assertEqual(total_dprs_df_list[0][DP.PROPORTION_OF_DPR_EMPLOYING_STAFF], 0.5)
        self.assertEqual(total_dprs_df_list[1][DP.PROPORTION_OF_DPR_EMPLOYING_STAFF], 0.25)

    def test_calculate_total_dprs_at_year_end_sums_su_and_carers_at_year_end_returns_correct_sum(
        self,
    ):
        rows = [
            ("area_1", 200.0, 5.0),
            ("area_2", 100.0, 10.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.CARER_DPRS_AT_YEAR_END, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        total_dprs_df = job.calculate_total_dprs_at_year_end(df)

        total_dprs_df_list = total_dprs_df.sort(DP.LA_AREA).collect()

        self.assertEqual(total_dprs_df_list[0][DP.TOTAL_DPRS_AT_YEAR_END], 205.0)
        self.assertEqual(total_dprs_df_list[1][DP.TOTAL_DPRS_AT_YEAR_END], 110.0)

    def test_determine_if_adass_base_is_closer_to_total_dpr_or_su_only_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", 100.0, 50.0, 60.0),
            ("area_2", 100.0, 50.0, 120.0),
            ("area_3", 100.0, 50.0, 75.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.TOTAL_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.DPRS_ADASS, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.determine_if_adass_base_is_closer_to_total_dpr_or_su_only(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.CLOSER_BASE], Values.SU_ONLY_DPRS)
        self.assertEqual(output_df_list[1][DP.CLOSER_BASE], Values.TOTAL_DPRS)
        self.assertEqual(output_df_list[2][DP.CLOSER_BASE], Values.TOTAL_DPRS)

    def test_calculate_value_if_adass_base_is_closer_to_total_dpr_returns_correct_value(self):
        rows = [
            ("area_1", 200.0, 0.5, 250.0),
            ("area_2", 100.0, 0.25, 102.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.TOTAL_DPRS_AT_YEAR_END, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_value_if_adass_base_is_closer_to_total_dpr(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertAlmostEqual(output_df_list[0][DP.PROPORTION_IF_TOTAL_DPR_CLOSER], 0.625, places=5)
        self.assertAlmostEqual(output_df_list[1][DP.PROPORTION_IF_TOTAL_DPR_CLOSER], 0.255, places=5)

    def test_calculate_value_if_adass_base_is_closer_to_su_only_returns_correct_value(self):
        rows = [
            ("area_1", 200.0, 0.5, 250.0, 12.0),
            ("area_2", 100.0, 0.25, 102.0, 4.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF, FloatType(), True),
                StructField(DP.TOTAL_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.CARER_DPRS_AT_YEAR_END, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_value_if_adass_base_is_closer_to_su_only(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertAlmostEqual(output_df_list[0][DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER], 0.500383233, places=5)
        self.assertAlmostEqual(output_df_list[1][DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER], 0.250255489, places=5)

    def test_calculate_service_users_employing_staff_returns_correct_product(self):
        rows = [
            ("area_1", 200.0, 0.5),
            ("area_2", 100.0, 0.25),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_service_users_employing_staff(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.SERVICE_USERS_EMPLOYING_STAFF_AT_YEAR_END], 100.0)
        self.assertEqual(output_df_list[1][DP.SERVICE_USERS_EMPLOYING_STAFF_AT_YEAR_END], 25.0)

    def test_calculate_carers_employing_staff_returns_correct_product(self):
        rows = [
            ("area_1", 2.0),
            ("area_2", 10.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.CARER_DPRS_AT_YEAR_END, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_carers_employing_staff(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertAlmostEqual(
            output_df_list[0][DP.CARERS_EMPLOYING_STAFF_AT_YEAR_END],
            0.0127744579073184,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.CARERS_EMPLOYING_STAFF_AT_YEAR_END],
            0.063872289536592,
            places=5,
        )

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
                StructField(DP.SERVICE_USERS_EMPLOYING_STAFF_AT_YEAR_END, FloatType(), True),
                StructField(DP.CARERS_EMPLOYING_STAFF_AT_YEAR_END, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_service_users_and_carers_employing_staff(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(
            output_df_list[0][DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF_AT_YEAR_END],
            102.5,
        )
        self.assertEqual(
            output_df_list[1][DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF_AT_YEAR_END],
            27.5,
        )

    def test_difference_between_survey_base_and_total_dpr_at_year_end_returns_correct_value(
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
                StructField(
                    DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF_AT_YEAR_END,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_difference_between_survey_base_and_total_dpr_at_year_end(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.DIFFERENCE_IN_BASES], 17.5)
        self.assertEqual(output_df_list[1][DP.DIFFERENCE_IN_BASES], 2.5)

    def test_proportion_Service_users_employing_staff_threshold_is_correct_value(
        self,
    ):

        self.assertEqual(Config.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_THRESHOLD, 1.0)

    def test_proportion_emplying_staff_threshold_is_correct_value(
        self,
    ):

        self.assertEqual(Config.PROPORTION_EMPLOYING_STAFF_THRESHOLD, 0.1)

    def test_allocate_proportions_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", Values.TOTAL_DPRS, 0.3, 0.4, None),
            ("area_2", Values.SU_ONLY_DPRS, 0.3, 0.4, None),
            ("area_3", Values.TOTAL_DPRS, 0.4, 0.3, None),
            ("area_4", Values.TOTAL_DPRS, 1.2, 0.9, None),
            ("area_5", Values.TOTAL_DPRS, 0.3, 0.4, 0.6),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.CLOSER_BASE, StringType(), False),
                StructField(DP.PROPORTION_IF_TOTAL_DPR_CLOSER, FloatType(), True),
                StructField(DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.allocate_proportions(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertAlmostEqual(output_df_list[0][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], 0.3, places=5)
        self.assertAlmostEqual(output_df_list[1][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], 0.4, places=5)
        self.assertAlmostEqual(output_df_list[2][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], 0.4, places=5)
        self.assertAlmostEqual(output_df_list[3][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], 0.9, places=5)
        self.assertAlmostEqual(output_df_list[4][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], 0.6, places=5)

    def test_calculate_proportion_of_service_users_only_employing_staff_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", Values.ADASS_INCLUDES_CARERS, 102.5, 100.0, 200.0, None),
            ("area_2", Values.ADASS_DOES_NOT_INCLUDE_CARERS, 72.5, 25.0, 100.0, None),
            ("area_3", Values.ADASS_DOES_NOT_INCLUDE_CARERS, 72.5, 25.0, 100.0, 0.5),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.METHOD, StringType(), True),
                StructField(
                    DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF_AT_YEAR_END,
                    FloatType(),
                    True,
                ),
                StructField(DP.SERVICE_USERS_EMPLOYING_STAFF_AT_YEAR_END, FloatType(), True),
                StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_proportion_of_service_users_only_employing_staff(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(
            output_df_list[0][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_TEMP],
            0.5125,
        )
        self.assertEqual(output_df_list[1][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_TEMP], 0.25)
        self.assertEqual(output_df_list[2][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_TEMP], 0.5)

    def test_determine_areas_including_carers_on_adass_returns_correct_columns(
        self,
    ):
        rows = [
            ("area_1", 2021, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0, 0.5),
            ("area_2", 2021, 300.0, 50.0, 100.0, 10.0, None, 100.0, 25.0, None),
            ("area_3", 2020, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0, None),
            ("area_4", 2020, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0, None),
            ("area_5", 2019, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0, 0.5),
            ("area_6", 2019, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0, None),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.CARER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.IMD_SCORE, FloatType(), True),
                StructField(DP.DPRS_ADASS, FloatType(), True),
                StructField(DP.DPRS_EMPLOYING_STAFF_ADASS, FloatType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.determine_areas_including_carers_on_adass(df)
        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertAlmostEqual(
            output_df_list[0][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5,
            places=5,
        )

        self.assertEqual(
            output_df_list[1][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.25063872289536593,
        )

        self.assertEqual(output_df_list[2][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], None)
        self.assertEqual(output_df_list[3][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], None)
        self.assertEqual(output_df_list[4][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], 0.5)
        self.assertEqual(output_df_list[5][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], None)
        self.assertEqual(len(output_df.columns), 18)

    def test_remove_outliers(
        self,
    ):
        rows = [
            ("area_1", 2021, 0.1),
            ("area_1", 2020, 0.75),
            ("area_1", 2019, 0.67),
            ("area_2", 2021, 0.3),
            ("area_2", 2020, 0.4),
            ("area_2", 2019, 0.9),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.remove_outliers(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR).collect()

        self.assertAlmostEqual(
            output_df_list[0][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.67,
            places=5,
        )

        self.assertAlmostEqual(
            output_df_list[1][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.75,
            places=5,
        )

        self.assertEqual(output_df_list[2][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], None)
        self.assertEqual(output_df_list[3][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], None)
        self.assertAlmostEqual(
            output_df_list[4][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.4,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[5][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.3,
            places=5,
        )
