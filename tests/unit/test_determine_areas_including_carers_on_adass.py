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
)


class TestDetermineAreasIncludingCarers(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_areas_including_carers").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_determine_areas_including_carers_on_adass_completes(self):
        rows = [
            ("area_1", 2021, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_2", 2021, 300.0, 50.0, 100.0, 10.0, None, 100.0, 25.0),
            ("area_3", 2020, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_4", 2020, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_5", 2019, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_6", 2019, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
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
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.determine_areas_including_carers_on_adass(df)
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
        total_dprs_df = job.calculate_propoartion_of_dprs_employing_staff(df)

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

    def test_difference_in_bases_threshold_is_correct_value(
        self,
    ):

        self.assertEqual(job.DIFFERENCE_IN_BASES_THRESHOLD, 100.0)

    def test_proportion_emplying_staff_threshold_is_correct_value(
        self,
    ):

        self.assertEqual(job.PROPORTION_EMPLOYING_STAFF_THRESHOLD, 0.1)

    def test_allocate_method_for_calculating_service_users_employing_staff_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", 100.0, 0.1),
            ("area_2", 100.0, 0.09),
            ("area_3", 99.0, 0.1),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.DIFFERENCE_IN_BASES, FloatType(), True),
                StructField(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.allocate_method_for_calculating_service_users_employing_staff(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.METHOD], job.ADASS_DOES_NOT_INCLUDE_CARERS)
        self.assertEqual(output_df_list[1][DP.METHOD], job.ADASS_INCLUDES_CARERS)
        self.assertEqual(output_df_list[2][DP.METHOD], job.ADASS_INCLUDES_CARERS)

    def test_calculate_proportion_of_service_users_only_employing_staff_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", job.ADASS_INCLUDES_CARERS, 102.5, 100.0, 200.0),
            ("area_2", job.ADASS_DOES_NOT_INCLUDE_CARERS, 72.5, 25.0, 100.0),
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
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_proportion_of_service_users_only_employing_staff(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], 0.5125)
        self.assertEqual(output_df_list[1][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], 0.25)

    def test_determine_areas_including_carers_on_adass_returns_correct_columns(
        self,
    ):
        rows = [
            ("area_1", 2021, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_2", 2021, 300.0, 50.0, 100.0, 10.0, None, 100.0, 25.0),
            ("area_3", 2020, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_4", 2020, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_5", 2019, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_6", 2019, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
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
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.determine_areas_including_carers_on_adass(df)
        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertAlmostEqual(
            output_df_list[0][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5001596807238414,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.25063872289536593,
            places=5,
        )
        self.assertEqual(output_df_list[2][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], None)
        self.assertEqual(output_df_list[3][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], None)
        self.assertEqual(output_df_list[4][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], None)
        self.assertEqual(output_df_list[5][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], None)
        self.assertEqual(len(output_df.columns), 17)
