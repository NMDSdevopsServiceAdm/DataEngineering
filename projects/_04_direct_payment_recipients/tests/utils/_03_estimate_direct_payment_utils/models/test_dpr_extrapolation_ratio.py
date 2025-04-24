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
import projects._04_direct_payment_recipients.utils._03_estimate_direct_payment_utils.models.extrapolation_ratio as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestExtrapolationRatio(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_model_extrapolation_completes(self):
        rows = [
            ("area_1", 2021, 300.0, 0.3, 320.0),
            ("area_2", 2021, 300.0, 0.4, 320.0),
            ("area_1", 2020, 300.0, 0.45, 300.0),
            ("area_2", 2020, 300.0, 0.35, 300.0),
            ("area_1", 2019, 300.0, 0.375, 300.0),
            ("area_2", 2019, 300.0, 0.2, 300.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(
                    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True
                ),
                StructField(DP.ESTIMATE_USING_MEAN, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.model_extrapolation(df)
        self.assertEqual(df.count(), output_df.count())

    def test_add_columns_with_first_and_last_years_of_data_returns_years_as_integers(
        self,
    ):
        rows = [
            ("area_1", 2021, 0.5),
            ("area_2", 2021, 0.5),
            ("area_1", 2020, 0.5),
            ("area_2", 2020, None),
            ("area_1", 2019, 0.5),
            ("area_2", 2019, None),
            ("area_1", 2018, None),
            ("area_2", 2018, None),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(
                    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.add_columns_with_first_and_last_years_of_data(df)
        output_df_list = output_df.sort(DP.LA_AREA).collect()
        self.assertEqual(output_df_list[0][DP.FIRST_YEAR_WITH_DATA], 2019)
        self.assertEqual(output_df_list[1][DP.FIRST_YEAR_WITH_DATA], 2019)
        self.assertEqual(output_df_list[2][DP.FIRST_YEAR_WITH_DATA], 2019)
        self.assertEqual(output_df_list[3][DP.FIRST_YEAR_WITH_DATA], 2019)
        self.assertEqual(output_df_list[4][DP.FIRST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[5][DP.FIRST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[6][DP.FIRST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[7][DP.FIRST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[0][DP.LAST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[1][DP.LAST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[2][DP.LAST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[3][DP.LAST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[4][DP.LAST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[5][DP.LAST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[6][DP.LAST_YEAR_WITH_DATA], 2021)
        self.assertEqual(output_df_list[7][DP.LAST_YEAR_WITH_DATA], 2021)

    def test_add_data_point_from_first_year_of_data_returns_correct_values(
        self,
    ):
        rows = [
            ("area_1", 2021, 2019, 0.2),
            ("area_2", 2021, 2021, 0.4),
            ("area_1", 2020, 2019, 0.3),
            ("area_2", 2020, 2021, None),
            ("area_1", 2019, 2019, 0.4),
            ("area_2", 2019, 2021, None),
            ("area_1", 2018, 2019, None),
            ("area_2", 2018, 2021, None),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.FIRST_YEAR_WITH_DATA, IntegerType(), True),
                StructField(DP.ESTIMATE_USING_MEAN, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.add_data_point_from_given_year_of_data(
            df,
            DP.FIRST_YEAR_WITH_DATA,
            DP.ESTIMATE_USING_MEAN,
            DP.FIRST_YEAR_MEAN_ESTIMATE,
        )
        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertAlmostEqual(
            output_df_list[0][DP.FIRST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.FIRST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )
        self.assertAlmostEqual(
            output_df_list[2][DP.FIRST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )
        self.assertAlmostEqual(
            output_df_list[3][DP.FIRST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )
        self.assertAlmostEqual(
            output_df_list[4][DP.FIRST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )
        self.assertAlmostEqual(
            output_df_list[5][DP.FIRST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )
        self.assertAlmostEqual(
            output_df_list[6][DP.FIRST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )
        self.assertAlmostEqual(
            output_df_list[7][DP.FIRST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )

    def test_add_data_point_from_last_year_of_data_returns_correct_values(
        self,
    ):
        rows = [
            ("area_1", 2021, 2019, 0.3),
            ("area_2", 2021, 2021, 0.4),
            ("area_1", 2020, 2019, 0.3),
            ("area_2", 2020, 2021, None),
            ("area_1", 2019, 2019, 0.3),
            ("area_2", 2019, 2021, None),
            ("area_1", 2018, 2019, None),
            ("area_2", 2018, 2021, None),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.LAST_YEAR_WITH_DATA, IntegerType(), True),
                StructField(DP.ESTIMATE_USING_MEAN, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.add_data_point_from_given_year_of_data(
            df,
            DP.LAST_YEAR_WITH_DATA,
            DP.ESTIMATE_USING_MEAN,
            DP.LAST_YEAR_MEAN_ESTIMATE,
        )
        output_df_list = output_df.sort(DP.LA_AREA).collect()

        self.assertAlmostEqual(
            output_df_list[0][DP.LAST_YEAR_MEAN_ESTIMATE], 0.3, places=5
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.LAST_YEAR_MEAN_ESTIMATE], 0.3, places=5
        )
        self.assertAlmostEqual(
            output_df_list[2][DP.LAST_YEAR_MEAN_ESTIMATE], 0.3, places=5
        )
        self.assertAlmostEqual(
            output_df_list[3][DP.LAST_YEAR_MEAN_ESTIMATE], 0.3, places=5
        )
        self.assertAlmostEqual(
            output_df_list[4][DP.LAST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )
        self.assertAlmostEqual(
            output_df_list[5][DP.LAST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )
        self.assertAlmostEqual(
            output_df_list[6][DP.LAST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )
        self.assertAlmostEqual(
            output_df_list[7][DP.LAST_YEAR_MEAN_ESTIMATE], 0.4, places=5
        )

    def test_calculate_extrapolation_ratio_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", 2021, 320.0, 300.0, 2019, 300.0, 2020),
            ("area_2", 2021, 320.0, 300.0, 2020, 300.0, 2021),
            ("area_1", 2020, 300.0, 300.0, 2019, 300.0, 2020),
            ("area_2", 2020, 300.0, 300.0, 2020, 300.0, 2021),
            ("area_1", 2019, 300.0, 300.0, 2019, 300.0, 2020),
            ("area_2", 2019, 300.0, 300.0, 2020, 300.0, 2021),
            ("area_1", 2018, 300.0, 300.0, 2019, 300.0, 2020),
            ("area_2", 2018, 300.0, 300.0, 2020, 300.0, 2021),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.ESTIMATE_USING_MEAN, FloatType(), True),
                StructField(DP.FIRST_YEAR_MEAN_ESTIMATE, FloatType(), True),
                StructField(DP.FIRST_YEAR_WITH_DATA, IntegerType(), True),
                StructField(DP.LAST_YEAR_MEAN_ESTIMATE, FloatType(), True),
                StructField(DP.LAST_YEAR_WITH_DATA, IntegerType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_extrapolation_ratios(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()
        self.assertAlmostEqual(output_df_list[0][DP.EXTRAPOLATION_RATIO], 1.0, places=5)
        self.assertAlmostEqual(
            output_df_list[1][DP.EXTRAPOLATION_RATIO], 1.0666666, places=5
        )
        self.assertAlmostEqual(output_df_list[2][DP.EXTRAPOLATION_RATIO], 1.0, places=5)
        self.assertAlmostEqual(output_df_list[3][DP.EXTRAPOLATION_RATIO], 1.0, places=5)
        self.assertEqual(output_df.count(), 4)

    def test_calculate_extrapolation_estimates_returns_correct_value(
        self,
    ):
        rows = [
            ("area_1", 2021, 1.1, 0.3, 0.4, 2019, 0.3, 0.4, 2020),
            ("area_2", 2021, 1.0, 0.3, 0.4, 2019, 0.3, 0.4, 2020),
            ("area_1", 2020, 0.9, 0.3, 0.4, 2019, 0.3, 0.4, 2020),
            ("area_2", 2020, 1.0, 0.3, 0.4, 2019, 0.3, 0.4, 2020),
            ("area_1", 2019, 1.0, 0.4, 0.4, 2019, 0.4, 0.4, 2020),
            ("area_2", 2019, 1.0, 0.4, 0.4, 2019, 0.4, 0.4, 2020),
            ("area_1", 2018, 0.8, 0.4, 0.4, 2019, 0.4, 0.4, 2020),
            ("area_2", 2018, 1.0, 0.4, 0.4, 2019, 0.4, 0.4, 2020),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.EXTRAPOLATION_RATIO, FloatType(), True),
                StructField(DP.FIRST_YEAR_MEAN_ESTIMATE, FloatType(), True),
                StructField(DP.FIRST_DATA_POINT, FloatType(), True),
                StructField(DP.FIRST_YEAR_WITH_DATA, IntegerType(), True),
                StructField(DP.LAST_YEAR_MEAN_ESTIMATE, FloatType(), True),
                StructField(DP.LAST_DATA_POINT, FloatType(), True),
                StructField(DP.LAST_YEAR_WITH_DATA, IntegerType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.calculate_extrapolation_estimates(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()
        self.assertEqual(df.count(), output_df.count())
        self.assertAlmostEqual(
            output_df_list[0][DP.ESTIMATE_USING_EXTRAPOLATION_RATIO],
            0.32,
            places=5,
        )
        self.assertEqual(
            output_df_list[1][DP.ESTIMATE_USING_EXTRAPOLATION_RATIO],
            None,
        )
        self.assertEqual(
            output_df_list[2][DP.ESTIMATE_USING_EXTRAPOLATION_RATIO],
            None,
        )
        self.assertAlmostEqual(
            output_df_list[3][DP.ESTIMATE_USING_EXTRAPOLATION_RATIO],
            0.44,
            places=5,
        )
        self.assertAlmostEqual(
            output_df_list[4][DP.ESTIMATE_USING_EXTRAPOLATION_RATIO],
            0.4,
            places=5,
        )
        self.assertEqual(
            output_df_list[5][DP.ESTIMATE_USING_EXTRAPOLATION_RATIO],
            None,
        )
        self.assertEqual(
            output_df_list[6][DP.ESTIMATE_USING_EXTRAPOLATION_RATIO],
            None,
        )
        self.assertAlmostEqual(
            output_df_list[7][DP.ESTIMATE_USING_EXTRAPOLATION_RATIO],
            0.4,
            places=5,
        )
