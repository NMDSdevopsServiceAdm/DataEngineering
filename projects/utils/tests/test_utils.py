import unittest
import warnings

from pyspark.sql import functions as F

from projects.utils.utils import utils
from projects.utils.unittest_data import utils_test_file_data as Data
from projects.utils.unittest_data import utils_test_file_schemas as Schemas
from utils.utils import get_spark
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class UtilsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = get_spark()

        warnings.simplefilter("ignore", ResourceWarning)


class CalculateTests(UtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.calculate_rows, Schemas.calculate_schema
        )
        self.new_col: str = "new_column"
        self.col_1: str = "column_1"
        self.col_2: str = "column_2"

    def test_calculate_plus(self):
        returned_df = utils.calculate(
            self.test_df, self.new_col, self.col_1, "plus", self.col_2
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_plus_rows, Schemas.expected_calculate_schema
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_minus(self):
        returned_df = utils.calculate(
            self.test_df, self.new_col, self.col_1, "minus", self.col_2
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_minus_rows, Schemas.expected_calculate_schema
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_multiplication(self):
        returned_df = utils.calculate(
            self.test_df, self.new_col, self.col_1, "multiplied by", self.col_2
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_multipy_rows, Schemas.expected_calculate_schema
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_division(self):
        returned_df = utils.calculate(
            self.test_df, self.new_col, self.col_1, "divided by", self.col_2
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_divide_rows, Schemas.expected_calculate_schema
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_average(self):
        returned_df = utils.calculate(
            self.test_df, self.new_col, self.col_1, "average", self.col_2
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_average_rows, Schemas.expected_calculate_schema
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_calculate_with_invalid_method(self):
        with self.assertRaises(ValueError) as context:
            utils.calculate(
                self.test_df, self.new_col, self.col_1, "other method", self.col_2
            )
        self.assertTrue("Invalid method: other method" in str(context.exception))

    def test_calculate_with_when_clause(self):
        test_with_when_df = self.spark.createDataFrame(
            Data.calculate_with_when_clause_rows, Schemas.calculate_schema
        )

        condition = F.col(self.col_1) < 15.0
        returned_df = utils.calculate(
            test_with_when_df, self.new_col, self.col_1, "plus", self.col_2, condition
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_with_when_clause_rows,
            Schemas.expected_calculate_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()
        self.assertEqual(returned_data, expected_data)
