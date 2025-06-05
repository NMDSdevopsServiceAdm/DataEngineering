import unittest
import warnings

from pyspark.sql import Window, functions as F

from projects.utils.utils import utils
from projects.utils.unittest_data import utils_test_file_data as Data
from projects.utils.unittest_data import utils_test_file_schemas as Schemas
from utils.utils import get_spark
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class UtilsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = get_spark()

        warnings.simplefilter("ignore", ResourceWarning)


class CalculateNewColumnTests(UtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.calculate_new_column_rows, Schemas.calculate_new_column_schema
        )

    def test_calculate_new_column_plus(self):
        returned_df = utils.calculate_new_column(
            self.test_df, Schemas.new_column, Schemas.column_1, "plus", Schemas.column_2
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_new_column_plus_rows,
            Schemas.expected_calculate_new_column_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_new_column_minus(self):
        returned_df = utils.calculate_new_column(
            self.test_df,
            Schemas.new_column,
            Schemas.column_1,
            "minus",
            Schemas.column_2,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_new_column_minus_rows,
            Schemas.expected_calculate_new_column_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_new_column_multiplication(self):
        returned_df = utils.calculate_new_column(
            self.test_df,
            Schemas.new_column,
            Schemas.column_1,
            "multiplied by",
            Schemas.column_2,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_new_column_multipy_rows,
            Schemas.expected_calculate_new_column_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_new_column_division(self):
        returned_df = utils.calculate_new_column(
            self.test_df,
            Schemas.new_column,
            Schemas.column_1,
            "divided by",
            Schemas.column_2,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_new_column_divide_rows,
            Schemas.expected_calculate_new_column_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_new_column_average(self):
        returned_df = utils.calculate_new_column(
            self.test_df,
            Schemas.new_column,
            Schemas.column_1,
            "average",
            Schemas.column_2,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_new_column_average_rows,
            Schemas.expected_calculate_new_column_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_new_column_absolute_difference(self):
        returned_df = utils.calculate_new_column(
            self.test_df,
            Schemas.new_column,
            Schemas.column_1,
            "absolute difference",
            Schemas.column_2,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_new_column_absolute_difference_rows,
            Schemas.expected_calculate_new_column_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_new_column_with_invalid_method(self):
        with self.assertRaises(ValueError) as context:
            utils.calculate_new_column(
                self.test_df,
                Schemas.new_column,
                Schemas.column_1,
                "other method",
                Schemas.column_2,
            )
        self.assertTrue("Invalid method: other method" in str(context.exception))

    def test_calculate_new_column_with_when_clause(self):
        test_with_when_df = self.spark.createDataFrame(
            Data.calculate_new_column_with_when_clause_rows,
            Schemas.calculate_new_column_schema,
        )

        condition = F.col(Schemas.column_1) < 15.0
        returned_df = utils.calculate_new_column(
            test_with_when_df,
            Schemas.new_column,
            Schemas.column_1,
            "plus",
            Schemas.column_2,
            condition,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_new_column_with_when_clause_rows,
            Schemas.expected_calculate_new_column_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class CalculateWindowedColumnTests(UtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.calculate_windowed_column_rows,
            Schemas.calculate_windowed_column_schema,
        )
        self.window = Window.partitionBy(IndCQC.care_home).orderBy(
            IndCQC.cqc_location_import_date
        )

    def test_calculate_windowed_column_avg(self):
        returned_df = utils.calculate_windowed_column(
            self.test_df,
            self.window,
            Schemas.new_column,
            IndCQC.ascwds_filled_posts,
            "avg",
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_windowed_column_avg_rows,
            Schemas.expected_calculate_windowed_column_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_windowed_column_count(self):
        returned_df = utils.calculate_windowed_column(
            self.test_df,
            self.window,
            Schemas.new_column,
            IndCQC.ascwds_filled_posts,
            "count",
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_windowed_column_count_rows,
            Schemas.expected_calculate_windowed_column_count_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_windowed_column_max(self):
        returned_df = utils.calculate_windowed_column(
            self.test_df,
            self.window,
            Schemas.new_column,
            IndCQC.ascwds_filled_posts,
            "max",
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_windowed_column_max_rows,
            Schemas.expected_calculate_windowed_column_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_windowed_column_min(self):
        returned_df = utils.calculate_windowed_column(
            self.test_df,
            self.window,
            Schemas.new_column,
            IndCQC.ascwds_filled_posts,
            "min",
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_windowed_column_min_rows,
            Schemas.expected_calculate_windowed_column_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_windowed_column_sum(self):
        returned_df = utils.calculate_windowed_column(
            self.test_df,
            self.window,
            Schemas.new_column,
            IndCQC.ascwds_filled_posts,
            "sum",
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_windowed_column_sum_rows,
            Schemas.expected_calculate_windowed_column_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_windowed_column_raises_error_with_invalid_method(self):
        with self.assertRaises(ValueError) as context:
            utils.calculate_windowed_column(
                self.test_df,
                self.window,
                Schemas.new_column,
                IndCQC.ascwds_filled_posts,
                "other",
            )
        self.assertTrue(
            "Error: The aggregation function 'other' was not found. Please use 'avg', 'count', 'max', 'min' or 'sum'."
            in str(context.exception)
        )
