import unittest
import warnings

import projects._04_direct_payment_recipients.utils._01_prepare_dpr_utils.calculate_pa_ratio as job
from projects._04_direct_payment_recipients.tests.utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.unittest_data.dpr_test_file_data import (
    CalculatePaRatioData as Data,
)
from projects._04_direct_payment_recipients.unittest_data.dpr_test_file_schemas import (
    CalculatePaRatioSchemas as Schemas,
)
from utils import utils


class TestCalculatePARatio(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_pa_ratio_completes(self):
        df = self.spark.createDataFrame(
            Data.calculate_pa_ratio_rows, schema=Schemas.total_staff_schema
        )
        output_df = job.calculate_pa_ratio(df)
        output_rows = output_df.count()
        minimum_expected_rows = 1

        self.assertGreaterEqual(output_rows, minimum_expected_rows)

    def test_exclude_outliers(self):
        df = self.spark.createDataFrame(
            Data.exclude_outliers_rows, schema=Schemas.total_staff_schema
        )
        output_df = job.exclude_outliers(df)
        output_rows = output_df.count()
        expected_rows = 2

        self.assertEqual(output_rows, expected_rows)

    def test_calculate_average_ratios(self):
        df = self.spark.createDataFrame(
            Data.calculate_average_ratio_rows, schema=Schemas.total_staff_schema
        )
        output_df = job.calculate_average_ratios(df)
        output_rows = output_df.sort(DP.YEAR_AS_INTEGER).collect()
        expected_rows = [2.0, 1.0, 1.5]

        self.assertEqual(output_rows[0][DP.AVERAGE_STAFF], expected_rows[0])
        self.assertEqual(output_rows[1][DP.AVERAGE_STAFF], expected_rows[1])
        self.assertEqual(output_rows[2][DP.AVERAGE_STAFF], expected_rows[2])
        self.assertEqual(len(output_rows), len(expected_rows))

    def test_add_in_missing_historic_ratios(self):
        df = self.spark.createDataFrame(
            Data.add_historic_rows, schema=Schemas.average_staff_schema
        )
        output_df = job.add_in_missing_historic_ratios(df)
        output_rows = output_df.sort(DP.YEAR_AS_INTEGER).collect()
        expected_rows = [
            1.98,
            1.98,
            1.98,
            1.0,
            2.00,
            2.01,
            1.0,
            1.96,
            1.0,
            1.0,
            1.0,
            1.6,
            2.2,
        ]

        self.assertAlmostEqual(output_rows[0][DP.AVERAGE_STAFF], expected_rows[0])
        self.assertAlmostEqual(output_rows[1][DP.AVERAGE_STAFF], expected_rows[1])
        self.assertAlmostEqual(output_rows[2][DP.AVERAGE_STAFF], expected_rows[2])
        self.assertAlmostEqual(output_rows[3][DP.AVERAGE_STAFF], expected_rows[3])
        self.assertAlmostEqual(output_rows[4][DP.AVERAGE_STAFF], expected_rows[4])
        self.assertAlmostEqual(output_rows[5][DP.AVERAGE_STAFF], expected_rows[5])
        self.assertAlmostEqual(output_rows[6][DP.AVERAGE_STAFF], expected_rows[6])
        self.assertAlmostEqual(output_rows[7][DP.AVERAGE_STAFF], expected_rows[7])
        self.assertAlmostEqual(output_rows[8][DP.AVERAGE_STAFF], expected_rows[8])
        self.assertAlmostEqual(output_rows[9][DP.AVERAGE_STAFF], expected_rows[9])
        self.assertAlmostEqual(output_rows[10][DP.AVERAGE_STAFF], expected_rows[10])
        self.assertAlmostEqual(output_rows[11][DP.AVERAGE_STAFF], expected_rows[11])
        self.assertAlmostEqual(output_rows[12][DP.AVERAGE_STAFF], expected_rows[12])

        self.assertEqual(len(output_rows), len(expected_rows))

    def test_apply_rolling_average(self):
        df = self.spark.createDataFrame(
            Data.apply_rolling_average_rows, schema=Schemas.average_staff_schema
        )
        output_df = job.apply_rolling_average(df)
        output_rows = output_df.sort(DP.YEAR_AS_INTEGER).collect()
        expected_rows = [1.0, 1.0, 1.0, 1.2, 1.6]

        self.assertAlmostEqual(
            output_rows[0][DP.RATIO_ROLLING_AVERAGE], expected_rows[0]
        )
        self.assertAlmostEqual(
            output_rows[1][DP.RATIO_ROLLING_AVERAGE], expected_rows[1]
        )
        self.assertAlmostEqual(
            output_rows[2][DP.RATIO_ROLLING_AVERAGE], expected_rows[2]
        )
        self.assertAlmostEqual(
            output_rows[3][DP.RATIO_ROLLING_AVERAGE], expected_rows[3]
        )
        self.assertAlmostEqual(
            output_rows[4][DP.RATIO_ROLLING_AVERAGE], expected_rows[4]
        )

        self.assertEqual(len(output_rows), len(expected_rows))

    def test_reduce_year_by_one_to_match_external_data_returns_correct_values(self):
        df = self.spark.createDataFrame(
            Data.reduce_year_by_one_rows, Schemas.reduce_year_by_one_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_reduce_year_by_one_rows, Schemas.reduce_year_by_one_schema
        )
        returned_df = job.reduce_year_by_one_to_match_external_data(df)
        expected_data = expected_df.sort(DP.YEAR_AS_INTEGER).collect()
        returned_data = returned_df.sort(DP.YEAR_AS_INTEGER).collect()
        self.assertEqual(returned_data, expected_data)
