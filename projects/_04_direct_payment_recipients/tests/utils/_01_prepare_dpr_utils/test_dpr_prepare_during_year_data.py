import unittest
import warnings

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)

from utils import utils
import projects._04_direct_payment_recipients.utils._01_prepare_dpr_utils.prepare_during_year_data as job
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestPrepareDuringYearData(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_total_dprs_during_year_returns_correct_sum(
        self,
    ):
        rows = [
            ("area_1", 100.0, 2.5),
            ("area_2", 25.0, 2.5),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_total_dprs_during_year(df)

        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertEqual(
            output_df_list[0][DP.TOTAL_DPRS_DURING_YEAR],
            102.5,
        )
        self.assertEqual(
            output_df_list[1][DP.TOTAL_DPRS_DURING_YEAR],
            27.5,
        )

    def test_estimate_missing_salt_data_for_hackney_works_on_historic_data(
        self,
    ):
        rows = [
            ("area_1", 2021, 100.0, 21.0),
            ("area_1", 2022, 25.0, 2.0),
            ("Hackney", 2021, 100.0, 4.5),
            ("Hackney", 2022, None, None),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), False),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.estimate_missing_salt_data_for_hackney(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertEqual(
            output_df_list[0][DP.SERVICE_USER_DPRS_DURING_YEAR],
            100.0,
        )
        self.assertEqual(
            output_df_list[1][DP.SERVICE_USER_DPRS_DURING_YEAR],
            580.5,
        )
        self.assertEqual(
            output_df_list[2][DP.SERVICE_USER_DPRS_DURING_YEAR],
            100.0,
        )
        self.assertEqual(
            output_df_list[3][DP.SERVICE_USER_DPRS_DURING_YEAR],
            25.0,
        )
        self.assertEqual(
            output_df_list[0][DP.CARER_DPRS_DURING_YEAR],
            4.5,
        )
        self.assertEqual(
            output_df_list[1][DP.CARER_DPRS_DURING_YEAR],
            140.85,
        )
        self.assertEqual(
            output_df_list[2][DP.CARER_DPRS_DURING_YEAR],
            21.0,
        )
        self.assertEqual(
            output_df_list[3][DP.CARER_DPRS_DURING_YEAR],
            2.0,
        )
