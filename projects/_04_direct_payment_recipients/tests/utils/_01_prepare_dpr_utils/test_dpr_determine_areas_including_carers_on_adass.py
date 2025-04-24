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
import projects._04_direct_payment_recipients.utils._01_prepare_dpr_utils.determine_areas_including_carers_on_adass as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)
from projects._04_direct_payment_recipients.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


class TestDetermineAreasIncludingCarers(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_determine_areas_including_carers_on_adass_completes(self):
        rows = [
            ("area_1", 2021, 300.0, 50.0, 200.0, 5.0, 100.0, 50.0, 0.5),
            ("area_2", 2021, 300.0, 50.0, 100.0, 10.0, 100.0, 25.0, None),
            ("area_3", 2020, 300.0, 50.0, 200.0, 5.0, 100.0, 50.0, None),
            ("area_4", 2020, 300.0, 50.0, 200.0, 5.0, 100.0, 50.0, None),
            ("area_5", 2019, 300.0, 50.0, 200.0, 5.0, 100.0, 50.0, None),
            ("area_6", 2019, 300.0, 50.0, 200.0, 5.0, 100.0, 50.0, None),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.CARER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.DPRS_ADASS, FloatType(), True),
                StructField(DP.DPRS_EMPLOYING_STAFF_ADASS, FloatType(), True),
                StructField(DP.PROPORTION_IMPORTED, FloatType(), True),
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

        self.assertEqual(
            total_dprs_df_list[0][DP.PROPORTION_OF_DPR_EMPLOYING_STAFF], 0.5
        )
        self.assertEqual(
            total_dprs_df_list[1][DP.PROPORTION_OF_DPR_EMPLOYING_STAFF], 0.25
        )

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

    def test_calculate_value_if_adass_base_is_closer_to_total_dpr_returns_correct_value(
        self,
    ):
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

        self.assertAlmostEqual(
            output_df_list[0][DP.PROPORTION_IF_TOTAL_DPR_CLOSER], 0.625, places=5
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.PROPORTION_IF_TOTAL_DPR_CLOSER], 0.255, places=5
        )

    def test_calculate_value_if_adass_base_is_closer_to_su_only_returns_correct_value(
        self,
    ):
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

        self.assertAlmostEqual(
            output_df_list[0][DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER],
            0.500383233,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER],
            0.250255489,
            places=5,
        )

    def test_proportion_service_users_employing_staff_threshold_is_correct_value(
        self,
    ):
        self.assertEqual(
            Config.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_THRESHOLD, 1.0
        )

    def test_carers_employing_percentage_is_correct_value(
        self,
    ):
        self.assertEqual(Config.CARERS_EMPLOYING_PERCENTAGE, 0.0063872289536592)

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
                StructField(
                    DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER, FloatType(), True
                ),
                StructField(DP.PROPORTION_IMPORTED, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.allocate_proportions(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertAlmostEqual(
            output_df_list[0][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.3,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.4,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[2][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.4,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[3][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.9,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[4][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.6,
            places=5,
        )

    def test_determine_areas_including_carers_on_adass_returns_correct_columns(
        self,
    ):
        rows = [
            ("area_1", 2021, 300.0, 50.0, 200.0, 5.0, 100.0, 50.0, 0.5),
            ("area_2", 2021, 300.0, 50.0, 100.0, 10.0, 100.0, 25.0, None),
            ("area_3", 2020, 300.0, 50.0, 200.0, 5.0, 100.0, 50.0, None),
            ("area_4", 2020, 300.0, 50.0, 200.0, 5.0, 100.0, 50.0, None),
            ("area_5", 2019, 300.0, 50.0, 200.0, 5.0, 100.0, 50.0, 0.5),
            ("area_6", 2019, 300.0, 50.0, 200.0, 5.0, 100.0, 50.0, None),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.CARER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.DPRS_ADASS, FloatType(), True),
                StructField(DP.DPRS_EMPLOYING_STAFF_ADASS, FloatType(), True),
                StructField(DP.PROPORTION_IMPORTED, FloatType(), True),
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

        self.assertEqual(
            output_df_list[2][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5001596807238414,
        )
        self.assertEqual(
            output_df_list[3][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5001596807238414,
        )
        self.assertEqual(
            output_df_list[4][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], 0.5
        )
        self.assertEqual(
            output_df_list[5][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5001596807238414,
        )
        self.assertEqual(len(output_df.columns), 19)

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
                StructField(DP.YEAR, StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.add_column_with_year_as_integer(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(output_df_list[0][DP.YEAR_AS_INTEGER], 2020)
        self.assertEqual(output_df_list[1][DP.YEAR_AS_INTEGER], 2021)
        self.assertEqual(output_df_list[2][DP.YEAR_AS_INTEGER], 2020)
        self.assertEqual(output_df_list[3][DP.YEAR_AS_INTEGER], 2021)
        self.assertEqual(output_df.count(), 4)
