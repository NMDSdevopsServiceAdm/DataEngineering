import unittest

import polars as pl
import polars.testing as pl_testing

import polars_utils.cleaning_utils as job
from tests.test_polars_utils_data import CleaningUtilsData as Data
from tests.test_polars_utils_schemas import CleaningUtilsSchemas as Schemas
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class PolarsCleaningUtilsTests(unittest.TestCase):
    def setUp(self):
        pass


class AddAlignedDateColumnsTests(PolarsCleaningUtilsTests):
    def setUp(self):
        self.primary_column = CQCLClean.cqc_location_import_date
        self.secondary_column = AWPClean.ascwds_workplace_import_date

    def test_mix_of_misaligned_dates_joined_correctly(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_rows, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_primary_with_secondary_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )

    def test_exact_match_date_joined_correctly(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_single_row, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_exact_match_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_secondary_exact_match_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_closest_historical_date_joined_correctly(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_single_row, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_closest_historical_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_secondary_closest_historical_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )

    def test_future_date_only_returns_null_secondary_column(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_single_row, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_future_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_secondary_future_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )


gender_labels: str = "gender_labels"
nationality_labels: str = "nationality_labels"


class ApplyCategoricalLabelsTests(SparkBaseTest):
    def setUp(self):
        self.test_worker_df = self.spark.createDataFrame(
            Data.worker_rows, schema=Schemas.worker_schema
        )
        self.label_dict = {
            AWK.gender: Data.gender,
            AWK.nationality: Data.nationality,
            IndCQC.contemporary_cssr: Data.contemporary_cssr,
        }
        self.label_dict_missing_gender = {
            AWK.nationality: Data.nationality,
            IndCQC.contemporary_cssr: Data.contemporary_cssr,
        }
        self.expected_df_with_new_columns = self.spark.createDataFrame(
            Data.expected_rows_with_new_columns,
            Schemas.expected_schema_with_new_columns,
        )
        self.expected_df_with_new_code_columns = self.spark.createDataFrame(
            Data.expected_rows_with_new_code_columns,
            Schemas.expected_schema_with_new_code_columns,
        )
        self.expected_df_without_new_columns = self.spark.createDataFrame(
            Data.expected_rows_without_new_columns, Schemas.worker_schema
        )
        self.test_df_when_duplicate_values_in_label_dict = self.spark.createDataFrame(
            Data.worker_rows_for_testing_label_dict_with_duplicate_values,
            schema=Schemas.worker_schema_for_testing_label_dict_with_duplicate_values,
        )
        self.expected_df_when_duplicate_values_in_label_dict = self.spark.createDataFrame(
            Data.expected_worker_rows_for_testing_label_dict_with_duplicate_values,
            schema=Schemas.expected_worker_schema_for_testing_label_dict_with_duplicate_values,
        )

    def test_apply_categorical_labels_completes(self):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )

        self.assertIsNotNone(returned_df)

    def test_single_column_added_with_replaced_values_when_new_column_is_set_to_true(
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

    def test_multiple_columns_added_with_replaced_values_when_new_column_is_set_to_true(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )
        returned_data = (
            returned_df.select(self.expected_df_with_new_columns.columns)
            .sort(AWK.worker_id)
            .collect()
        )
        expected_data = self.expected_df_with_new_columns.sort(AWK.worker_id).collect()

        expected_columns = len(self.test_worker_df.columns) + 2

        self.assertEqual(len(returned_df.columns), expected_columns)
        self.assertEqual(returned_data, expected_data)

    def test_replaces_values_in_original_columns_when_new_column_is_set_to_false(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=False,
        )
        returned_data = (
            returned_df.select(self.expected_df_without_new_columns.columns)
            .sort(AWK.worker_id)
            .collect()
        )
        expected_data = self.expected_df_without_new_columns.sort(
            AWK.worker_id
        ).collect()

        expected_columns = len(self.test_worker_df.columns)

        self.assertEqual(len(returned_df.columns), expected_columns)
        self.assertEqual(returned_data, expected_data)

    def test_reverse_label_strings_into_code_strings_when_new_column_is_set_to_false(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.expected_df_without_new_columns,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=False,
            reverse_mapping=True,
        )
        returned_data = (
            returned_df.select(self.test_worker_df.columns)
            .sort(AWK.worker_id)
            .collect()
        )
        expected_data = self.test_worker_df.sort(AWK.worker_id).collect()

        expected_columns = len(self.test_worker_df.columns)

        self.assertEqual(len(returned_df.columns), expected_columns)
        self.assertEqual(returned_data, expected_data)

    def test_reverse_label_strings_into_code_strings_when_new_column_is_set_to_true(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.expected_df_without_new_columns,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
            reverse_mapping=True,
        )
        returned_data = (
            returned_df.select(self.expected_df_with_new_code_columns.columns)
            .sort(AWK.worker_id)
            .collect()
        )
        expected_data = self.expected_df_with_new_code_columns.sort(
            AWK.worker_id
        ).collect()

        expected_columns = len(self.test_worker_df.columns) + 2

        self.assertEqual(len(returned_df.columns), expected_columns)
        self.assertEqual(returned_data, expected_data)

    def test_reverse_label_strings_into_code_strings_when_label_dict_has_duplicate_values(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_df_when_duplicate_values_in_label_dict,
            self.label_dict,
            [IndCQC.contemporary_cssr],
            add_as_new_column=True,
            reverse_mapping=True,
        )
        returned_data = (
            returned_df.select(
                self.expected_df_when_duplicate_values_in_label_dict.columns
            )
            .sort(AWK.worker_id)
            .collect()
        )
        expected_data = self.expected_df_when_duplicate_values_in_label_dict.sort(
            AWK.worker_id
        ).collect()

        expected_columns = (
            len(self.test_df_when_duplicate_values_in_label_dict.columns) + 1
        )

        self.assertEqual(len(returned_df.columns), expected_columns)
        self.assertEqual(returned_data, expected_data)

    def test_add_as_new_column_defaults_to_true(self):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.label_dict,
            [AWK.gender, AWK.nationality],
        )
        expected_columns = len(self.test_worker_df.columns) + 2

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_original_values_not_in_label_dict_are_retained_when_new_col_added(self):
        input_df = self.spark.createDataFrame(
            Data.worker_rows_with_unmatched_labels,
            Schemas.worker_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_worker_rows_with_unmatched_labels_with_new_columns,
            Schemas.expected_schema_with_new_columns,
        )

        returned_df = job.apply_categorical_labels(
            input_df,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )

        returned_data = (
            returned_df.select(expected_df.columns).sort(AWK.worker_id).collect()
        )
        expected_data = expected_df.sort(AWK.worker_id).collect()

        self.assertEqual(returned_data, expected_data)

    def test_original_values_not_in_label_dict_are_retained_when_labels_added_into_original_col(
        self,
    ):
        input_df = self.spark.createDataFrame(
            Data.worker_rows_with_unmatched_labels,
            Schemas.worker_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_worker_rows_with_unmatched_labels_without_new_columns,
            Schemas.worker_schema,
        )

        returned_df = job.apply_categorical_labels(
            input_df,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=False,
        )

        returned_data = (
            returned_df.select(expected_df.columns).sort(AWK.worker_id).collect()
        )
        expected_data = expected_df.sort(AWK.worker_id).collect()

        self.assertEqual(returned_data, expected_data)

    def test_raises_value_error_when_column_not_in_dataframe(
        self,
    ):
        with self.assertRaises(ValueError) as context:
            job.apply_categorical_labels(
                self.test_worker_df,
                self.label_dict,
                [AWK.age],
                add_as_new_column=True,
            )

        self.assertEqual(
            str(context.exception),
            "Column age not found in DataFrame.",
        )

    def test_raises_key_error_when_column_not_in_labels_dict(
        self,
    ):
        with self.assertRaises(ValueError) as context:
            job.apply_categorical_labels(
                self.test_worker_df,
                self.label_dict_missing_gender,
                [AWK.gender],
                add_as_new_column=True,
            )

        self.assertEqual(
            str(context.exception),
            "No label mapping found for gender.",
        )


class ColumnToDateTests(unittest.TestCase):
    def test_converts_date_string_without_hyphens_to_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_string_without_hyphens_rows,
            schema=Schemas.col_to_date_string_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_rows,
            schema=Schemas.expected_col_to_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_converts_date_integer_without_hyphens_to_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_integer_without_hyphens_rows,
            schema=Schemas.col_to_date_integer_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_rows,
            schema=Schemas.expected_col_to_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_converts_date_string_with_hyphens_to_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_string_with_hyphens_rows,
            schema=Schemas.col_to_date_string_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_rows,
            schema=Schemas.expected_col_to_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_adds_a_new_column_with_converted_date(self):
        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_with_new_col_rows,
            schema=Schemas.expected_col_to_date_with_new_col_schema,
        )
        returned_lf = job.column_to_date(
            expected_lf.drop("new_date_col"), "date_col", "new_date_col"
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CalculateFilledPostsPerBedRatioTests(unittest.TestCase):
    def test_calculate_filled_posts_per_bed_ratio(self):
        expected_lf = pl.LazyFrame(
            Data.expected_filled_posts_per_bed_ratio_rows,
            Schemas.expected_filled_posts_per_bed_ratio_schema,
            orient="row",
        )
        returned_lf = job.calculate_filled_posts_per_bed_ratio(
            expected_lf.drop(IndCQC.filled_posts_per_bed_ratio),
            IndCQC.ascwds_filled_posts_dedup,
            IndCQC.filled_posts_per_bed_ratio,
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class ReduceDatasetToEarliestFilePerMonthTests(unittest.TestCase):
    def test_reduce_dataset_to_earliest_file_per_month_returns_correct_rows(self):
        test_lf = pl.LazyFrame(
            Data.reduce_dataset_to_earliest_file_per_month_rows,
            Schemas.reduce_dataset_to_earliest_file_per_month_schema,
            orient="row",
        )
        returned_lf = job.reduce_dataset_to_earliest_file_per_month(test_lf)
        expected_lf = pl.LazyFrame(
            Data.expected_reduce_dataset_to_earliest_file_per_month_rows,
            Schemas.reduce_dataset_to_earliest_file_per_month_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CreateBandedBedCountColumnTests(unittest.TestCase):
    def test_create_banded_bed_count_column(self):
        expected_lf = pl.LazyFrame(
            Data.expected_create_banded_bed_count_column_rows,
            Schemas.expected_create_banded_bed_count_column_schema,
            orient="row",
        )
        test_splits = [0, 1, 25, float("Inf")]
        returned_lf = job.create_banded_bed_count_column(
            expected_lf.drop(IndCQC.number_of_beds_banded),
            IndCQC.number_of_beds_banded,
            test_splits,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
