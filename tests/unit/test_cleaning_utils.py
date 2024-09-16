import unittest


import pyspark.sql.functions as F
from pyspark.sql import DataFrame


from utils import utils

import utils.cleaning_utils as job

from tests.test_file_schemas import CleaningUtilsSchemas as Schemas
from tests.test_file_data import CleaningUtilsData as Data

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys,
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


gender_labels: str = "gender_labels"
nationality_labels: str = "nationality_labels"


class TestCleaningUtilsCategorical(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.test_worker_df = self.spark.createDataFrame(
            Data.worker_rows, schema=Schemas.worker_schema
        )
        self.label_dict = {AWK.gender: Data.gender, AWK.nationality: Data.nationality}
        self.expected_df_with_new_columns = self.spark.createDataFrame(
            Data.expected_rows_with_new_columns,
            Schemas.expected_schema_with_new_columns,
        )
        self.expected_df_without_new_columns = self.spark.createDataFrame(
            Data.expected_rows_without_new_columns, Schemas.worker_schema
        )

    def test_apply_categorical_labels_completes(self):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
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
            self.test_worker_df, self.label_dict, [AWK.gender]
        )

        expected_columns = len(self.test_worker_df.columns) + 1

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_adds_two_new_columns_when_given_two_columns_and_new_column_is_undefined(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
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
            self.label_dict,
            [AWK.gender, AWK.nationality],
        )
        returned_data = returned_df.sort(AWK.worker_id).collect()
        expected_data = self.expected_df_with_new_columns.sort(AWK.worker_id).collect()
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
        with self.assertRaises(ValueError) as context:
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

    def test_set_column_bounds_works_with_only_lower_limit_specified(
        self,
    ):
        returned_df = (
            job.set_column_bounds(
                self.test_scale_df,
                "int",
                "bound_int",
                lower_limit=0,
            ),
        )
        self.assertTrue(type(returned_df), DataFrame)
        self.assertTrue(len(returned_df), 2)

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


class TestCleaningUtilsColumnToDate(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.sample_df = self.spark.createDataFrame(
            Data.column_to_date_data, Schemas.sample_col_to_date_schema
        )

    def test_changes_data_type_to_date(self):
        returned_df = job.column_to_date(self.sample_df, "input_string")
        self.assertEqual(returned_df.dtypes[0], ("input_string", "date"))

    def test_inserts_new_column_if_specified(self):
        returned_df = job.column_to_date(self.sample_df, "input_string", "new_column")
        self.assertTrue("new_column" in returned_df.columns)
        self.assertEqual(returned_df.dtypes[2], ("new_column", "date"))

    def test_old_column_is_untouched_if_new_col_given(self):
        returned_df = job.column_to_date(self.sample_df, "input_string", "new_column")
        self.assertEqual(
            self.sample_df.select("input_string").collect(),
            returned_df.select("input_string").collect(),
        )

    def test_new_value_is_equal_to_expected_value(self):
        returned_df = job.column_to_date(self.sample_df, "input_string", "new_column")
        self.assertEqual(
            self.sample_df.select("expected_value")
            .withColumnRenamed("expected_value", "new_column")
            .collect(),
            returned_df.select("new_column").collect(),
        )


class TestCleaningUtilsAlignDates(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.primary_column = AWPClean.ascwds_workplace_import_date
        self.secondary_column = CQCLClean.cqc_location_import_date
        self.primary_df = self.spark.createDataFrame(
            Data.align_dates_primary_rows, Schemas.align_dates_primary_schema
        )
        self.secondary_df = self.spark.createDataFrame(
            Data.align_dates_secondary_rows, Schemas.align_dates_secondary_schema
        )
        self.expected_aligned_dates = self.spark.createDataFrame(
            Data.expected_aligned_dates_rows, Schemas.expected_aligned_dates_schema
        )
        self.expected_cross_join_df = self.spark.createDataFrame(
            Data.expected_cross_join_rows, Schemas.expected_aligned_dates_schema
        )
        self.expected_best_matches = self.spark.createDataFrame(
            Data.expected_aligned_dates_rows, Schemas.expected_aligned_dates_schema
        )
        self.later_secondary_df = self.spark.createDataFrame(
            Data.align_later_dates_secondary_rows, Schemas.align_dates_secondary_schema
        )
        self.expected_later_aligned_dates = self.spark.createDataFrame(
            Data.expected_later_aligned_dates_rows,
            Schemas.expected_aligned_dates_schema,
        )
        self.merged_dates_df = self.spark.createDataFrame(
            Data.expected_merged_rows, Schemas.expected_merged_dates_schema
        )
        self.later_merged_dates_df = self.spark.createDataFrame(
            Data.expected_later_merged_rows, Schemas.expected_merged_dates_schema
        )
        self.column_order_for_assertion = [
            self.primary_column,
            self.secondary_column,
        ]

    def test_align_import_dates_completes(self):
        returned_df = job.align_import_dates(
            self.primary_df,
            self.secondary_df,
            self.primary_column,
            self.secondary_column,
        )

        self.assertTrue(returned_df)

    def test_align_import_dates_aligns_dates_correctly_when_secondary_data_starts_before_primary(
        self,
    ):
        returned_df = job.align_import_dates(
            self.primary_df,
            self.secondary_df,
            self.primary_column,
            self.secondary_column,
        )
        returned_data = returned_df.sort(self.primary_column).collect()
        expected_data = self.expected_aligned_dates.sort(self.primary_column).collect()
        self.assertEqual(returned_data, expected_data)

    def test_align_import_dates_aligns_dates_correctly_when_secondary_data_starts_later_than_primary(
        self,
    ):
        returned_df = job.align_import_dates(
            self.primary_df,
            self.later_secondary_df,
            self.primary_column,
            self.secondary_column,
        )
        returned_data = returned_df.sort(self.primary_column).collect()

        expected_data = self.expected_later_aligned_dates.sort(
            self.primary_column
        ).collect()
        self.assertEqual(returned_data, expected_data)

    def test_cross_join_unique_dates_joins_correctly(self):
        returned_df = job.cross_join_unique_dates(
            self.primary_df,
            self.secondary_df,
            self.primary_column,
            self.secondary_column,
        )
        returned_data = returned_df.sort(
            self.primary_column, self.secondary_column
        ).collect()
        expected_data = self.expected_cross_join_df.sort(
            self.primary_column, self.secondary_column
        ).collect()
        self.assertEqual(returned_data, expected_data)

    def test_determine_best_date_matches_selects_best_matches_correctly(self):
        returned_df = job.determine_best_date_matches(
            self.expected_cross_join_df,
            self.primary_column,
            self.secondary_column,
        )
        returned_data = returned_df.sort(
            self.primary_column, self.secondary_column
        ).collect()
        expected_data = self.expected_best_matches.sort(
            self.primary_column, self.secondary_column
        ).collect()
        self.assertEqual(returned_data, expected_data)

    def test_add_aligned_date_column_columns_completes(self):
        returned_df = job.add_aligned_date_column(
            self.primary_df,
            self.secondary_df,
            self.primary_column,
            self.secondary_column,
        )

        self.assertTrue(returned_df)

    def test_add_aligned_date_column_joins_secondary_date_correctly(
        self,
    ):
        returned_df = job.add_aligned_date_column(
            self.primary_df,
            self.secondary_df,
            self.primary_column,
            self.secondary_column,
        )
        returned_data = returned_df.sort(self.primary_column).collect()
        expected_data = (
            self.merged_dates_df.select(returned_df.columns)
            .sort(self.primary_column)
            .collect()
        )
        self.assertEqual(returned_data, expected_data)

    def test_add_aligned_date_column_joins_correctly_when_secondary_data_start_later_than_primary(
        self,
    ):
        returned_df = job.add_aligned_date_column(
            self.primary_df,
            self.later_secondary_df,
            self.primary_column,
            self.secondary_column,
        )
        returned_data = (
            returned_df.select(self.column_order_for_assertion)
            .sort(self.primary_column)
            .collect()
        )
        expected_data = (
            self.later_merged_dates_df.select(self.column_order_for_assertion)
            .sort(self.primary_column)
            .collect()
        )
        self.assertEqual(returned_data, expected_data)

    def test_add_aligned_date_column_returns_the_correct_number_of_rows(
        self,
    ):
        returned_df = job.add_aligned_date_column(
            self.primary_df,
            self.later_secondary_df,
            self.primary_column,
            self.secondary_column,
        )
        returned_rows = returned_df.count()

        expected_rows = self.primary_df.count()

        self.assertEqual(returned_rows, expected_rows)

    def test_add_aligned_date_column_returns_the_correct_number_of_columns(
        self,
    ):
        returned_df = job.add_aligned_date_column(
            self.primary_df,
            self.later_secondary_df,
            self.primary_column,
            self.secondary_column,
        )
        returned_columns = len(returned_df.columns)

        expected_columns = len(self.later_merged_dates_df.columns)

        self.assertEqual(returned_columns, expected_columns)


class ReduceDatasetToEarliestFilePerMonthTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

    def test_reduce_dataset_to_earliest_file_per_month_returns_correct_rows(self):
        test_df = self.spark.createDataFrame(
            Data.reduce_dataset_to_earliest_file_per_month_rows,
            Schemas.reduce_dataset_to_earliest_file_per_month_schema,
        )
        returned_df = job.reduce_dataset_to_earliest_file_per_month(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_reduce_dataset_to_earliest_file_per_month_rows,
            Schemas.reduce_dataset_to_earliest_file_per_month_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(),
            expected_df.collect(),
        )


class CastToIntTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.filled_posts_columns = [AWP.total_staff, AWP.worker_records]

    def test_cast_to_int_returns_strings_formatted_as_ints_to_ints(self):
        cast_to_int_df = self.spark.createDataFrame(
            Data.cast_to_int_rows, Schemas.cast_to_int_schema
        )
        cast_to_int_expected_df = self.spark.createDataFrame(
            Data.cast_to_int_expected_rows, Schemas.cast_to_int_expected_schema
        )

        returned_df = job.cast_to_int(cast_to_int_df, self.filled_posts_columns)

        returned_data = returned_df.sort(AWP.location_id).collect()
        expected_data = cast_to_int_expected_df.sort(AWP.location_id).collect()

        self.assertEqual(expected_data, returned_data)

    def test_cast_to_int_returns_strings_not_formatted_as_ints_as_none(self):
        cast_to_int_with_errors_df = self.spark.createDataFrame(
            Data.cast_to_int_errors_rows, Schemas.cast_to_int_schema
        )
        cast_to_int_with_errors_expected_df = self.spark.createDataFrame(
            Data.cast_to_int_errors_expected_rows, Schemas.cast_to_int_expected_schema
        )

        returned_df = job.cast_to_int(
            cast_to_int_with_errors_df, self.filled_posts_columns
        )

        returned_data = returned_df.sort(AWP.location_id).collect()
        expected_data = cast_to_int_with_errors_expected_df.sort(
            AWP.location_id
        ).collect()

        self.assertEqual(expected_data, returned_data)


class CalculateFilledPostsPerBedRatioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()

    def test_calculate_filled_posts_per_bed_ratio(self):
        test_df = self.spark.createDataFrame(
            Data.filled_posts_per_bed_ratio_rows,
            Schemas.filled_posts_per_bed_ratio_schema,
        )
        returned_df = job.calculate_filled_posts_per_bed_ratio(
            test_df, IndCQC.ascwds_filled_posts_dedup
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_filled_posts_per_bed_ratio_rows,
            Schemas.expected_filled_posts_per_bed_ratio_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )


class CalculateFilledPostsFromBedsAndRatioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()

    def test_calculate_filled_posts_from_beds_and_ratio(self):
        test_df = self.spark.createDataFrame(
            Data.filled_posts_from_beds_and_ratio_rows,
            Schemas.filled_posts_from_beds_and_ratio_schema,
        )
        returned_df = job.calculate_filled_posts_from_beds_and_ratio(
            test_df, IndCQC.filled_posts_per_bed_ratio, IndCQC.care_home_model
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_filled_posts_from_beds_and_ratio_rows,
            Schemas.expected_filled_posts_from_beds_and_ratio_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )
