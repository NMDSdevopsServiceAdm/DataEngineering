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
    determine_areas_including_carers_on_adass,
    filter_to_most_recent_year,
    calculate_total_dprs_during_year,
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
            ("area_1", 2020, 0.5, 200.0, 5.0),
            ("area_2", 2021, 0.5, 200.0, 5.0),
            ("area_3", 2020, 0.5, 200.0, 5.0),
            ("area_4", 2021, 0.5, 200.0, 5.0),
        ]
        df = self.spark.createDataFrame(rows, schema=self.determine_areas_including_carers_schema)
        filtered_df = filter_to_most_recent_year(df)

        filtered_df_list = filtered_df.sort(DP.LA_AREA).collect()

        self.assertEqual(filtered_df_list[0][DP.YEAR], 2021)
        self.assertEqual(filtered_df_list[1][DP.YEAR], 2021)
        self.assertEqual(filtered_df.count(), 2)

    def test_calculate_total_dprs_during_year_sums_su_and_carers_during_year(self):
        rows = [
            ("area_1", 2021, 0.5, 200.0, 5.0),
            ("area_2", 2021, 0.5, 100.0, 10.0),
        ]
        df = self.spark.createDataFrame(rows, schema=self.determine_areas_including_carers_schema)
        total_dprs_df = calculate_total_dprs_during_year(df)

        total_dprs_df_list = total_dprs_df.sort(DP.LA_AREA).collect()

        self.assertEqual(total_dprs_df_list[0][DP.TOTAL_DPRS_DURING_YEAR], 205.0)
        self.assertEqual(total_dprs_df_list[1][DP.TOTAL_DPRS_DURING_YEAR], 110.0)
