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
import projects._04_direct_payment_recipients.utils._03_estimate_direct_payment_utils.models.interpolation as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestDPRInterpolation(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_model_interpolation_completes(self):
        rows = [
            ("area_1", 2021, 300.0, 0.3, 320.0),
            ("area_2", 2021, 300.0, 0.4, 320.0),
            ("area_1", 2020, None, None, None),
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
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
                StructField(DP.ESTIMATE_USING_MEAN, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.model_interpolation(df)
        self.assertEqual(df.count(), output_df.count())

    def test_filter_to_locations_with_known_service_users_employing_staff_filters_correctly(
        self,
    ):
        rows = [
            ("area_1", 2021, 300.0, 0.3),
            ("area_2", 2021, 300.0, 0.4),
            ("area_1", 2020, None, None),
            ("area_2", 2020, 300.0, 0.35),
            ("area_1", 2019, 300.0, 0.375),
            ("area_2", 2019, 300.0, 0.2),
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
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                    FloatType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.filter_to_locations_with_known_service_users_employing_staff(df)

        self.assertEqual(output_df.count(), 5)
        self.assertEqual(
            output_df.columns,
            [
                DP.LA_AREA,
                DP.YEAR_AS_INTEGER,
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
            ],
        )

    def test_calculate_first_and_last_submission_date_per_location(self):
        rows = [
            ("area_1", 2021, 0.5),
            ("area_1", 2019, 0.5),
            ("area_2", 2021, 0.5),
            ("area_2", 2020, 0.5),
            ("area_2", 2019, 0.5),
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
        output_df = job.calculate_first_and_last_submission_year_per_la_area(df)

        self.assertEqual(output_df.count(), 2)
        self.assertEqual(
            output_df.columns,
            [DP.LA_AREA, DP.FIRST_SUBMISSION_YEAR, DP.LAST_SUBMISSION_YEAR],
        )

        output_df_list = output_df.sort(DP.LA_AREA).collect()
        self.assertEqual(output_df_list[0][DP.FIRST_SUBMISSION_YEAR], 2019)
        self.assertEqual(output_df_list[0][DP.LAST_SUBMISSION_YEAR], 2021)
        self.assertEqual(output_df_list[1][DP.FIRST_SUBMISSION_YEAR], 2019)
        self.assertEqual(output_df_list[1][DP.LAST_SUBMISSION_YEAR], 2021)

    def test_convert_first_and_last_known_time_into_timeseries_df(self):
        rows = [
            ("area_1", 2019, 2021),
            ("area_2", 2019, 2021),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.FIRST_SUBMISSION_YEAR, IntegerType(), True),
                StructField(
                    DP.LAST_SUBMISSION_YEAR,
                    IntegerType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.convert_first_and_last_known_years_into_exploded_df(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.INTERPOLATION_YEAR).collect()

        self.assertEqual(output_df.count(), 6)
        self.assertEqual(
            output_df.columns,
            [DP.LA_AREA, DP.INTERPOLATION_YEAR],
        )

        self.assertEqual(output_df_list[0][DP.INTERPOLATION_YEAR], 2019)
        self.assertEqual(output_df_list[1][DP.INTERPOLATION_YEAR], 2020)
        self.assertEqual(output_df_list[2][DP.INTERPOLATION_YEAR], 2021)
        self.assertEqual(output_df_list[3][DP.INTERPOLATION_YEAR], 2019)
        self.assertEqual(output_df_list[4][DP.INTERPOLATION_YEAR], 2020)
        self.assertEqual(output_df_list[5][DP.INTERPOLATION_YEAR], 2021)

    def test_merge_known_values_with_exploded_dates_returns_correct_values(self):
        all_dates_rows = [
            ("area_1", 2021),
            ("area_2", 2021),
            ("area_1", 2020),
            ("area_2", 2020),
            ("area_1", 2019),
            ("area_2", 2019),
        ]
        all_dates_test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.INTERPOLATION_YEAR, IntegerType(), True),
            ]
        )
        all_dates_df = self.spark.createDataFrame(
            all_dates_rows, schema=all_dates_test_schema
        )

        known_values_rows = [
            ("area_1", 2021, 0.5),
            ("area_1", 2019, 0.5),
            ("area_2", 2021, 0.5),
            ("area_2", 2020, 0.5),
            ("area_2", 2019, 0.5),
        ]
        known_values_test_schema = StructType(
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
        known_service_users_employing_staff_df = self.spark.createDataFrame(
            known_values_rows, schema=known_values_test_schema
        )

        output_df = job.merge_known_values_with_exploded_dates(
            all_dates_df, known_service_users_employing_staff_df
        )
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()
        self.assertEqual(output_df.count(), 6)
        self.assertEqual(
            output_df.columns,
            [
                DP.LA_AREA,
                DP.YEAR_AS_INTEGER,
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED,
            ],
        )

        self.assertEqual(
            output_df_list[0][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5,
        )
        self.assertEqual(
            output_df_list[1][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            None,
        )
        self.assertEqual(
            output_df_list[2][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5,
        )
        self.assertEqual(
            output_df_list[3][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5,
        )
        self.assertEqual(
            output_df_list[4][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5,
        )
        self.assertEqual(
            output_df_list[5][DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
            0.5,
        )
        self.assertEqual(
            output_df_list[0][
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED
            ],
            2019,
        )
        self.assertEqual(
            output_df_list[1][
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED
            ],
            None,
        )
        self.assertEqual(
            output_df_list[2][
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED
            ],
            2021,
        )
        self.assertEqual(
            output_df_list[3][
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED
            ],
            2019,
        )
        self.assertEqual(
            output_df_list[4][
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED
            ],
            2020,
        )
        self.assertEqual(
            output_df_list[5][
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED
            ],
            2021,
        )

    def test_interpolate_values_for_all_dates_returns_interpolated_values(self):
        rows = [
            ("area_1", 2019, 0.5, 2019),
            ("area_1", 2020, None, None),
            ("area_1", 2021, 0.5, 2021),
            ("area_2", 2019, 0.3, 2019),
            ("area_2", 2020, None, None),
            ("area_2", 2021, 0.5, 2021),
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
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED,
                    IntegerType(),
                    True,
                ),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.interpolate_values_for_all_dates(df)
        output_df_list = output_df.sort(DP.LA_AREA, DP.YEAR_AS_INTEGER).collect()

        self.assertEqual(output_df.count(), 6)
        self.assertEqual(
            output_df.columns,
            [DP.LA_AREA, DP.YEAR_AS_INTEGER, DP.ESTIMATE_USING_INTERPOLATION],
        )

        self.assertAlmostEqual(
            output_df_list[0][DP.ESTIMATE_USING_INTERPOLATION], 0.5, places=5
        )
        self.assertAlmostEqual(
            output_df_list[1][DP.ESTIMATE_USING_INTERPOLATION], 0.5, places=5
        )
        self.assertAlmostEqual(
            output_df_list[2][DP.ESTIMATE_USING_INTERPOLATION], 0.5, places=5
        )
        self.assertAlmostEqual(
            output_df_list[3][DP.ESTIMATE_USING_INTERPOLATION], 0.3, places=5
        )
        self.assertAlmostEqual(
            output_df_list[4][DP.ESTIMATE_USING_INTERPOLATION], 0.4, places=5
        )
        self.assertAlmostEqual(
            output_df_list[5][DP.ESTIMATE_USING_INTERPOLATION], 0.5, places=5
        )
