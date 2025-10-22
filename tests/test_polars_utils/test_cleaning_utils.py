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
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)


class PolarsCleaningUtilsTests(unittest.TestCase):
    def setUp(self):
        pass


class AddAlignedDateColumnsTests(PolarsCleaningUtilsTests):
    def setUp(self):
        self.primary_column = CQCLClean.cqc_location_import_date
        self.secondary_column = AWPClean.ascwds_workplace_import_date

        self.primary_lf = pl.LazyFrame(
            Data.align_dates_primary_rows, Schemas.align_dates_primary_schema
        )
        self.secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_rows,
            Schemas.align_dates_secondary_schema,
        )
        self.column_order_for_assertion = [
            self.primary_column,
            self.secondary_column,
        ]
        self.returned_lf = job.add_aligned_date_column(
            self.primary_lf,
            self.secondary_lf,
            self.primary_column,
            self.secondary_column,
        )

    def test_expected_columns_are_returned(self):
        returned_columns = sorted(self.returned_lf.collect_schema().names())
        expected_columns = sorted(
            self.primary_lf.collect_schema().names() + [self.secondary_column]
        )

        self.assertEqual(returned_columns, expected_columns)

    def test_mix_of_misaligned_dates_joined_correctly(self):
        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_primary_with_secondary_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(
            self.returned_lf.select(self.column_order_for_assertion).sort(
                self.primary_column
            ),
            expected_lf.select(self.column_order_for_assertion).sort(
                self.primary_column
            ),
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

        pl_testing.assert_frame_equal(
            returned_lf.select(self.column_order_for_assertion),
            expected_lf.select(self.column_order_for_assertion),
        )

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
            returned_lf.select(self.column_order_for_assertion),
            expected_lf.select(self.column_order_for_assertion),
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
            returned_lf.select(self.column_order_for_assertion),
            expected_lf.select(self.column_order_for_assertion),
        )


class ApplyCategoricalLabelsTests(unittest.TestCase):
    def setUp(self):
        self.test_worker_lf = pl.LazyFrame(
            data=Data.worker_rows, schema=Schemas.worker_schema
        )
        self.label_dict = {AWK.gender: Data.gender, AWK.nationality: Data.nationality}
        self.expected_lf_with_new_columns = pl.LazyFrame(
            data=Data.expected_rows_with_new_columns,
            schema=Schemas.expected_schema_with_new_columns,
        )
        self.expected_lf_without_new_columns = pl.LazyFrame(
            data=Data.expected_rows_without_new_columns, schema=Schemas.worker_schema
        )

    def test_apply_categorical_labels_completes(self):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )

        self.assertIsNotNone(returned_lf)

    def test_apply_categorical_labels_adds_a_new_column_when_given_one_column_and_new_column_is_set_to_true(
        self,
    ):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf,
            self.label_dict,
            [AWK.gender],
            add_as_new_column=True,
        )

        expected_columns = len(self.test_worker_lf.columns) + 1

        self.assertEqual(len(returned_lf.columns), expected_columns)

    def test_apply_categorical_labels_adds_two_new_columns_when_given_two_columns_and_new_column_is_set_to_true(
        self,
    ):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )

        expected_columns = len(self.test_worker_lf.columns) + 2

        self.assertEqual(len(returned_lf.columns), expected_columns)

    def test_apply_categorical_labels_adds_new_columns_with_replaced_values_when_new_column_is_set_to_true(
        self,
    ):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )
        pl_testing.assert_frame_equal(returned_lf, self.expected_lf_with_new_columns)

    def test_apply_categorical_labels_does_not_add_a_new_column_when_given_one_column_and_new_column_is_set_to_false(
        self,
    ):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf,
            self.label_dict,
            [AWK.gender],
            add_as_new_column=False,
        )

        expected_columns = len(self.test_worker_lf.columns)

        self.assertEqual(len(returned_lf.columns), expected_columns)

    def test_apply_categorical_labels_does_not_add_new_columns_when_given_two_columns_and_new_column_is_set_to_false(
        self,
    ):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=False,
        )

        expected_columns = len(self.test_worker_lf.columns)

        self.assertEqual(len(returned_lf.columns), expected_columns)

    def test_apply_categorical_labels_replaces_values_when_new_column_is_set_to_false(
        self,
    ):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=False,
        )

        pl_testing.assert_frame_equal(returned_lf, self.expected_lf_without_new_columns)

    def test_apply_categorical_labels_adds_a_new_column_when_given_one_column_and_new_column_is_undefined(
        self,
    ):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf, self.label_dict, [AWK.gender]
        )

        expected_columns = len(self.test_worker_lf.columns) + 1

        self.assertEqual(len(returned_lf.columns), expected_columns)

    def test_apply_categorical_labels_adds_two_new_columns_when_given_two_columns_and_new_column_is_undefined(
        self,
    ):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf,
            self.label_dict,
            [AWK.gender, AWK.nationality],
        )

        expected_columns = len(self.test_worker_lf.columns) + 2

        self.assertEqual(len(returned_lf.columns), expected_columns)

    def test_apply_categorical_labels_adds_new_columns_with_replaced_values_when_new_column_is_undefined(
        self,
    ):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf,
            self.label_dict,
            [AWK.gender, AWK.nationality],
        )

        pl_testing.assert_frame_equal(returned_lf, self.expected_lf_with_new_columns)

    def test_apply_categorical_labels_adds_1_new_column_with_1_custom_name(
        self,
    ):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf,
            self.label_dict,
            [AWK.gender],
            True,
            ["custom_gender_column_name"],
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_rows_with_1_new_column_and_1_custom_column_name,
            schema=Schemas.expected_schema_with_new_1_column_and_1_custom_column_name,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_apply_categorical_labels_adds_2_new_column_with_2_custom_name(
        self,
    ):
        returned_lf = job.apply_categorical_labels(
            self.test_worker_lf,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            True,
            ["custom_gender_column_name", "custom_nationality_column_name"],
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_rows_with_2_new_columns_and_2_custom_column_names,
            schema=Schemas.expected_schema_with_new_2_columns_and_2_custom_column_names,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
