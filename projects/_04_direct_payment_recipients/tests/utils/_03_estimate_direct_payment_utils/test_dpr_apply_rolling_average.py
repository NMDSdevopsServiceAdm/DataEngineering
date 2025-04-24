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
import projects._04_direct_payment_recipients.utils._03_estimate_direct_payment_utils.apply_rolling_average as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestApplyRollingAverage(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_apply_rolling_average_completes(self):
        rows = [
            ("area_1", 2021, 0.25),
            ("area_1", 2020, 0.45),
            ("area_1", 2019, 0.46),
            ("area_1", 2018, 0.51),
            ("area_1", 2017, 0.37),
            ("area_1", 2016, 0.42),
            ("area_2", 2021, 0.62),
            ("area_2", 2020, 0.80),
            ("area_2", 2019, 0.54),
            ("area_2", 2018, 0.74),
            ("area_2", 2017, 0.65),
            ("area_2", 2016, 0.62),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.apply_rolling_average(df)
        self.assertEqual(df.count(), output_df.count())

    def test_calculate_aggregates_per_year_returns_aggregated_df(self):
        rows = [
            ("area_1", 2021, 0.25),
            ("area_1", 2020, 0.45),
            ("area_1", 2019, 0.46),
            ("area_1", 2018, 0.51),
            ("area_1", 2017, 0.37),
            ("area_1", 2016, 0.42),
            ("area_2", 2021, 0.62),
            ("area_2", 2020, 0.80),
            ("area_2", 2019, 0.54),
            ("area_2", 2018, 0.74),
            ("area_2", 2017, 0.65),
            ("area_2", 2016, 0.62),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_aggregates_per_year(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertEqual(
            output_df_list[0][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertEqual(
            output_df_list[1][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertEqual(
            output_df_list[2][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertEqual(
            output_df_list[3][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertEqual(
            output_df_list[4][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertEqual(
            output_df_list[5][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertEqual(
            output_df_list[6][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertEqual(
            output_df_list[7][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertEqual(
            output_df_list[8][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertEqual(
            output_df_list[9][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertEqual(
            output_df_list[10][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertEqual(
            output_df_list[11][
                DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            1.0,
        )
        self.assertAlmostEqual(
            output_df_list[0][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.42,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[1][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.37,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[2][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.51,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[3][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.46,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[4][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.45,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[5][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.25,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[6][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.62,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[7][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.65,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[8][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.74,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[9][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.54,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[10][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.80,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[11][
                DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.62,
            places=2,
        )
        self.assertEqual(df.count(), output_df.count())

    def test_create_rolling_average_column_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 0.25, 1.0, 0.25),
            ("area_1", 2020, 0.45, 1.0, 0.45),
            ("area_1", 2019, 0.46, 1.0, 0.46),
            ("area_1", 2018, 0.51, 1.0, 0.51),
            ("area_1", 2017, 0.37, 1.0, 0.37),
            ("area_1", 2016, 0.42, 1.0, 0.42),
            ("area_2", 2021, 0.62, 1.0, 0.62),
            ("area_2", 2020, 0.80, 1.0, 0.80),
            ("area_2", 2019, 0.54, 1.0, 0.54),
            ("area_2", 2018, 0.74, 1.0, 0.74),
            ("area_2", 2017, 0.65, 1.0, 0.65),
            ("area_2", 2016, 0.62, 1.0, 0.62),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.create_rolling_average_column(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertAlmostEqual(
            output_df_list[0][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.42,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[1][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.39,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[2][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.43,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[3][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.45,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[4][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.47,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[5][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.39,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[6][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.62,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[7][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.63,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[8][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.67,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[9][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.64,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[10][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.69,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[11][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.65,
            places=2,
        )
        self.assertEqual(df.count(), output_df.count())

    def test_join_rolling_average_into_df_returns_correct_columns(self):
        rolling_avg_rows = [
            ("area_1", 2021, 0.25, 0.39),
            ("area_1", 2020, 0.45, 0.47),
            ("area_1", 2019, 0.46, 0.45),
            ("area_1", 2018, 0.51, 0.43),
            ("area_1", 2017, 0.37, 0.40),
            ("area_1", 2016, 0.42, 0.42),
            ("area_2", 2021, 0.62, 0.65),
            ("area_2", 2020, 0.80, 0.69),
            ("area_2", 2019, 0.54, 0.64),
            ("area_2", 2018, 0.74, 0.67),
            ("area_2", 2017, 0.65, 0.64),
            ("area_2", 2016, 0.62, 0.62),
        ]
        rolling_avg_test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df_rows = [
            ("area_1", 2021, 0.25, 6.0, 2.46),
            ("area_1", 2020, 0.45, 6.0, 2.46),
            ("area_1", 2019, 0.46, 6.0, 2.46),
            ("area_1", 2018, 0.51, 6.0, 2.46),
            ("area_1", 2017, 0.37, 6.0, 2.46),
            ("area_1", 2016, 0.42, 6.0, 2.46),
            ("area_2", 2021, 0.62, 6.0, 3.97),
            ("area_2", 2020, 0.80, 6.0, 3.97),
            ("area_2", 2019, 0.54, 6.0, 3.97),
            ("area_2", 2018, 0.74, 6.0, 3.97),
            ("area_2", 2017, 0.65, 6.0, 3.97),
            ("area_2", 2016, 0.62, 6.0, 3.97),
        ]
        df_test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        rolling_avg_df = self.spark.createDataFrame(
            rolling_avg_rows, schema=rolling_avg_test_schema
        )
        df = self.spark.createDataFrame(df_rows, schema=df_test_schema)
        output_df = job.join_rolling_average_into_df(df, rolling_avg_df)
        self.assertEqual(len(output_df.columns), 6)
        self.assertEqual(df.count(), output_df.count())

    def test_apply_rolling_average_returns_correct_values(self):
        rows = [
            ("area_1", 2021, 0.25),
            ("area_1", 2020, 0.45),
            ("area_1", 2019, 0.46),
            ("area_1", 2018, 0.51),
            ("area_1", 2017, 0.37),
            ("area_1", 2016, 0.42),
            ("area_2", 2021, 0.62),
            ("area_2", 2020, 0.80),
            ("area_2", 2019, 0.54),
            ("area_2", 2018, 0.74),
            ("area_2", 2017, 0.65),
            ("area_2", 2016, 0.62),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.apply_rolling_average(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertAlmostEqual(
            output_df_list[0][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.42,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[1][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.39,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[2][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.43,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[3][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.45,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[4][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.47,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[5][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.39,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[6][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.62,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[7][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.63,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[8][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.67,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[9][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.64,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[10][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.69,
            places=2,
        )
        self.assertAlmostEqual(
            output_df_list[11][
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            ],
            0.65,
            places=2,
        )
        self.assertEqual(df.count(), output_df.count())
