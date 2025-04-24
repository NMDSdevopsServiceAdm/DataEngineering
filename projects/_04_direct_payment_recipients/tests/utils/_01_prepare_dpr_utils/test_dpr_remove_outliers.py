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
import projects._04_direct_payment_recipients.utils._01_prepare_dpr_utils.remove_outliers as job
from projects._04_direct_payment_recipients.tests.utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)


class TestRemoveOutliers(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_remove_outliers_completes(
        self,
    ):
        rows = [
            ("area_1", 2018, 0.6),
            ("area_1", 2019, 0.5),
            ("area_1", 2020, 1.2),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.remove_outliers(df)
        self.assertEqual(df.count(), output_df.count())

    def test_identify_outliers_using_threshold_value(
        self,
    ):
        rows = [
            ("area_1", 2019, 0.1, Values.RETAIN, 0.50667),
            ("area_1", 2020, 0.75, Values.RETAIN, 0.50667),
            ("area_1", 2021, 0.67, Values.RETAIN, 0.50667),
            ("area_2", 2019, 0.3, Values.REMOVE, 0.53333),
            ("area_2", 2020, 0.4, Values.RETAIN, 0.53333),
            ("area_2", 2021, 0.9, Values.REMOVE, 0.53333),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True
                ),
                StructField(DP.OUTLIERS_FOR_REMOVAL, StringType(), True),
                StructField(
                    DP.MEAN_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.identify_outliers_using_threshold_value(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertEqual(output_df_list[0][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[1][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[2][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[3][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[4][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[5][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)

    def test_identify_values_below_zero_or_above_one(
        self,
    ):
        rows = [
            ("area_1", 2019, -0.1, Values.RETAIN),
            ("area_1", 2020, 0.75, Values.RETAIN),
            ("area_1", 2021, 0.67, Values.RETAIN),
            ("area_2", 2019, 1.3, Values.REMOVE),
            ("area_2", 2020, 1.4, Values.RETAIN),
            ("area_2", 2021, 0.9, Values.REMOVE),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True
                ),
                StructField(DP.OUTLIERS_FOR_REMOVAL, StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.identify_values_below_zero_or_above_one(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertEqual(output_df_list[0][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[1][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[2][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[3][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[4][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[5][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)

    def test_identify_extreme_values_when_only_value_in_la_area(
        self,
    ):
        rows = [
            ("area_1", 2019, 0.01, Values.RETAIN),
            ("area_1", 2020, None, Values.RETAIN),
            ("area_2", 2019, 0.95, Values.RETAIN),
            ("area_2", 2020, None, Values.RETAIN),
            ("area_3", 2019, 0.5, Values.REMOVE),
            ("area_4", 2021, 0.4, Values.RETAIN),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True
                ),
                StructField(DP.OUTLIERS_FOR_REMOVAL, StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.identify_extreme_values_when_only_value_in_la_area(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertEqual(output_df_list[0][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[1][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[2][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[3][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[4][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[5][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)

    def test_identify_extreme_values_not_following_a_trend_in_most_recent_year(
        self,
    ):
        rows = [
            ("area_1", 2021, 0.5, Values.RETAIN),
            ("area_1", 2022, 0.01, Values.RETAIN),
            ("area_2", 2021, 0.5, Values.RETAIN),
            ("area_2", 2022, 0.99, Values.RETAIN),
            ("area_3", 2021, None, Values.RETAIN),
            ("area_3", 2022, 0.4, Values.RETAIN),
            ("area_4", 2021, 0.4, Values.RETAIN),
            ("area_4", 2022, None, Values.RETAIN),
            ("area_5", 2021, 0.99, Values.RETAIN),
            ("area_5", 2022, None, Values.RETAIN),
            ("area_6", 2021, 0.5, Values.REMOVE),
            ("area_6", 2022, 0.3, Values.RETAIN),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True
                ),
                StructField(DP.OUTLIERS_FOR_REMOVAL, StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = (
            job.identify_extreme_values_not_following_a_trend_in_most_recent_year(df)
        )
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertEqual(output_df_list[0][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[1][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[2][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[3][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[4][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[5][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[6][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[7][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[8][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[9][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[10][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[11][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)

    def test_retain_cases_where_latest_number_we_know_is_not_outlier(
        self,
    ):
        rows = [
            ("area_1", 2019, 0.5, Values.RETAIN),
            ("area_1", 2020, 0.3, Values.REMOVE),
            ("area_2", 2019, 0.5, Values.RETAIN),
            ("area_2", 2020, 0.99, Values.REMOVE),
            ("area_3", 2019, None, Values.RETAIN),
            ("area_3", 2020, 0.4, Values.RETAIN),
            ("area_4", 2019, 0.4, Values.REMOVE),
            ("area_4", 2020, None, Values.RETAIN),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True
                ),
                StructField(DP.OUTLIERS_FOR_REMOVAL, StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.retain_cases_where_latest_number_we_know_is_not_outlier(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertEqual(output_df_list[0][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[1][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[2][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[3][DP.OUTLIERS_FOR_REMOVAL], Values.REMOVE)
        self.assertEqual(output_df_list[4][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[5][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[6][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)
        self.assertEqual(output_df_list[7][DP.OUTLIERS_FOR_REMOVAL], Values.RETAIN)

    def test_remove_identified_outliers(
        self,
    ):
        rows = [
            ("area_1", 2019, 0.5, Values.RETAIN),
            ("area_1", 2020, 0.9, Values.REMOVE),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True
                ),
                StructField(DP.OUTLIERS_FOR_REMOVAL, StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.remove_identified_outliers(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertEqual(
            output_df_list[0][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], 0.5
        )
        self.assertEqual(
            output_df_list[1][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], None
        )

    def test_remove_outliers(
        self,
    ):
        rows = [
            ("area_1", 2018, 0.6),
            ("area_1", 2019, 0.5),
            ("area_1", 2020, 1.2),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.remove_outliers(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertAlmostEqual(
            output_df_list[0][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.6,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5,
            places=5,
        )
        self.assertEqual(
            output_df_list[2][DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF], None
        )
