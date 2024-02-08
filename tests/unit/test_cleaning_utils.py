import unittest
import pyspark.sql.functions as F

from utils import utils

import utils.cleaning_utils as job

from tests.test_file_schemas import CleaningUtilsSchemas as Schemas
from tests.test_file_data import CleaningUtilsData as Data

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)


gender_labels: str = "gender_labels"
nationality_labels: str = "nationality_labels"


class TestCleaningUtilsCategorical(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.test_worker_df = self.spark.createDataFrame(
            Data.worker_rows, schema=Schemas.worker_schema
        )
        self.replace_labels_df = self.spark.createDataFrame(
            Data.replace_labels_rows, schema=Schemas.replace_labels_schema
        )
        self.label_df = self.spark.createDataFrame(Data.gender, Schemas.labels_schema)
        self.label_dict = {AWK.gender: Data.gender, AWK.nationality: Data.nationality}
        self.expected_df_with_new_columns = self.spark.createDataFrame(
            Data.expected_rows_with_new_columns,
            Schemas.expected_schema_with_new_columns,
        )
        self.expected_df_without_new_columns = self.spark.createDataFrame(
            Data.expected_rows_without_new_columns, Schemas.worker_schema
        )
        self.primary_df = self.spark.createDataFrame(Data.align_dates_primary_rows, Schemas.align_dates_schema)
        self.secondary_df = self.spark.createDataFrame(Data.align_dates_secondary_rows, Schemas.align_dates_schema)
        self.expected_alighned_dates = self.spark.createDataFrame(Data.expected_aligned_dates_rows, Schemas.expected_aligned_dates_schema)


    def test_apply_categorical_labels_completes(self):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )

        self.assertIsNotNone(returned_df)

    def test_apply_categorical_labels_adds_a_new_column_when_given_one_column_and_new_column_is_set_to_true(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender],
            add_as_new_column=True,
        )

        expected_columns = len(self.test_worker_df.columns) + 1

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_adds_two_new_columns_when_given_two_columns_and_new_column_is_set_to_true(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )

        expected_columns = len(self.test_worker_df.columns) + 2

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_adds_new_columns_with_replaced_values_when_new_column_is_set_to_true(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )
        returned_data = returned_df.sort(AWK.worker_id).collect()
        expected_data = self.expected_df_with_new_columns.sort(AWK.worker_id).collect()
        self.assertEqual(returned_data, expected_data)

    def test_apply_categorical_labels_does_not_add_a_new_column_when_given_one_column_and_new_column_is_set_to_false(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender],
            add_as_new_column=False,
        )

        expected_columns = len(self.test_worker_df.columns)

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_does_not_add_new_columns_when_given_two_columns_and_new_column_is_set_to_false(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=False,
        )

        expected_columns = len(self.test_worker_df.columns)

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_replaces_values_when_new_column_is_set_to_false(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=False,
        )
        returned_data = returned_df.sort(AWK.worker_id).collect()
        expected_data = self.expected_df_without_new_columns.sort(
            AWK.worker_id
        ).collect()
        self.assertEqual(returned_data, expected_data)

    def test_apply_categorical_labels_adds_a_new_column_when_given_one_column_and_new_column_is_undefined(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df, self.spark, self.label_dict, [AWK.gender]
        )

        expected_columns = len(self.test_worker_df.columns) + 1

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_adds_two_new_columns_when_given_two_columns_and_new_column_is_undefined(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
        )

        expected_columns = len(self.test_worker_df.columns) + 2

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_adds_new_columns_with_replaced_values_when_new_column_is_undefined(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
        )
        returned_data = returned_df.sort(AWK.worker_id).collect()
        expected_data = self.expected_df_with_new_columns.sort(AWK.worker_id).collect()
        self.assertEqual(returned_data, expected_data)

    def test_replace_labels_replaces_values_in_situe_when_new_column_name_is_null(self):
        returned_df = job.replace_labels(
            self.replace_labels_df,
            self.label_df,
            AWK.gender,
        )
        returned_data = returned_df.sort(AWK.worker_id).collect()

        expected_df = self.spark.createDataFrame(
            Data.expected_rows_replace_labels_in_situe, Schemas.replace_labels_schema
        )
        expected_data = expected_df.sort(AWK.worker_id).collect()
        self.assertEqual(returned_data, expected_data)

    def test_replace_labels_replaces_values_in_new_column_when_new_column_name_is_supplied(
        self,
    ):
        returned_df = job.replace_labels(
            self.replace_labels_df,
            self.label_df,
            AWK.gender,
            new_column_name=gender_labels,
        )
        returned_data = returned_df.sort(AWK.worker_id).collect()
        expected_df = self.spark.createDataFrame(
            Data.expected_rows_replace_labels_with_new_column,
            Schemas.expected_schema_replace_labels_with_new_columns,
        )
        expected_data = expected_df.sort(AWK.worker_id).collect()
        self.assertEqual(returned_data, expected_data)


class TestCleaningUtilsScale(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.test_scale_df = self.spark.createDataFrame(
            Data.scale_data, schema=Schemas.scale_schema
        )

    def test_set_column_bounds_no_int_outside_bound(self):
        returned_df = job.set_column_bounds(
            self.test_scale_df, "int", "bound_int", -9999, 9999
        )

        returned_int_values = returned_df.select("bound_int").collect()
        original_int_values = self.test_scale_df.select("int").collect()
        self.assertEqual(returned_int_values, original_int_values)

    def test_set_column_bounds_no_float_outside_bound(self):
        returned_df = job.set_column_bounds(
            self.test_scale_df, "float", "bound_float", -9999, 9999
        )

        returned_float_values = returned_df.select("bound_float").collect()
        original_float_values = self.test_scale_df.select("float").collect()
        self.assertEqual(returned_float_values, original_float_values)

    def test_set_column_bounds_int_value_equals_bound(self):
        returned_df = job.set_column_bounds(
            self.test_scale_df, "int", "bound_int", 23, 23
        )

        returned_bounded_int = (
            returned_df.where(F.col("int") == 23).select("bound_int").first()[0]
        )
        self.assertEqual(returned_bounded_int, 23)

    def test_set_column_bounds_float_value_equals_bound(self):
        returned_df = job.set_column_bounds(
            self.test_scale_df, "float", "bound_float", 10.1, 10.2
        )

        returned_bounded_int = (
            returned_df.where(F.round(F.col("float")) == 10)
            .select("bound_float")
            .first()[0]
        )
        self.assertAlmostEqual(returned_bounded_int, 10.1, 3)

    def test_set_column_bounds_int_below_lower_bound_are_set_to_null(self):
        returned_df = job.set_column_bounds(
            self.test_scale_df, "int", "bound_int", lower_limit=25, upper_limit=100
        )

        returned_bounded_int = (
            returned_df.where(F.col("int") == 23).select("bound_int").first()[0]
        )

        self.assertEqual(returned_bounded_int, None)

    def test_set_column_bounds_float_below_lower_bound_are_set_to_null(self):
        returned_df = job.set_column_bounds(
            self.test_scale_df, "float", "bound_float", lower_limit=25, upper_limit=100
        )

        returned_bounded_float = (
            returned_df.where(F.round(F.col("float")) == 10)
            .select("bound_float")
            .first()[0]
        )

        self.assertEqual(returned_bounded_float, None)

    def test_set_column_bounds_float_raises_error_if_lower_limit_is_greater_than_upper(
        self,
    ):
        with self.assertRaises(Exception) as context:
            job.set_column_bounds(
                self.test_scale_df,
                "float",
                "bound_float",
                lower_limit=100,
                upper_limit=1,
            ),

        self.assertTrue(
            "Lower limit (100) must be lower than upper limit (1)"
            in str(context.exception),
        )

    def test_set_column_bounds_int_above_upper_bound_are_set_to_null(self):
        returned_df = job.set_column_bounds(
            self.test_scale_df, "int", "bound_int", lower_limit=0, upper_limit=10
        )

        returned_bounded_int = (
            returned_df.where(F.col("int") == 23).select("bound_int").first()[0]
        )

        self.assertEqual(returned_bounded_int, None)

    def test_set_column_bounds_float_above_upper_bound_are_set_to_null(self):
        returned_df = job.set_column_bounds(
            self.test_scale_df, "float", "bound_float", lower_limit=1, upper_limit=5
        )

        returned_bounded_float = (
            returned_df.where(F.round(F.col("float")) == 10)
            .select("bound_float")
            .first()[0]
        )

        self.assertEqual(returned_bounded_float, None)

    def test_set_column_bounds_return_orginal_df_when_both_limits_are_none(self):
        returned_df = job.set_column_bounds(
            self.test_scale_df,
            "int",
            "bound_int",
            lower_limit=None,
            upper_limit=None,
        )

        self.assertEqual(returned_df, self.test_scale_df)

    def test_set_column_bounds_does_not_alter_the_original_df(self):
        copy_of_test_df = self.test_scale_df.alias("copy")
        job.set_column_bounds(
            self.test_scale_df,
            "int",
            "bound_int",
            lower_limit=None,
            upper_limit=None,
        )

        expected_data = copy_of_test_df.sort("int").collect()
        returned_data = self.test_scale_df.sort("int").collect()

        self.assertEqual(returned_data, expected_data)

    def test_set_bounds_for_columns_raises_error_if_columns_dont_match_names(self):
        with self.assertRaises(Exception) as context:
            job.set_bounds_for_columns(
                self.test_scale_df,
                ["int", "float"],
                ["bound_int", "bound_float", "another_column"],
                lower_limit=1,
                upper_limit=100,
            ),

        self.assertTrue(
            "Column list size (2) must match new column list size (3)"
            in str(context.exception),
        )

    def test_set_bounds_for_columns_set_bounds_for_all_columns(self):
        returned_df = job.set_bounds_for_columns(
            self.test_scale_df,
            ["int", "float"],
            ["bound_int", "bound_float"],
            lower_limit=1,
            upper_limit=100,
        )

        returned_data = returned_df.sort("int").collect()
        expected_df = self.spark.createDataFrame(
            Data.expected_scale_data, Schemas.expected_scale_schema
        )
        expected_data = expected_df.sort("int").collect()

        self.assertEqual(returned_data, expected_data)
