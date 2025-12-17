import io
import unittest
from contextlib import redirect_stdout

import numpy as np
import polars as pl
import polars.testing as pl_testing

from projects._03_independent_cqc._04_model.utils import training_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ModelTrainingUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ModelTrainingUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class SplitTestTrainTests(unittest.TestCase):
    def setUp(self):
        self.seed = 99
        self.fraction = 0.5
        self.test_df = pl.DataFrame(
            Data.split_train_test_rows, Schemas.split_train_test_schema, orient="row"
        )

    def test_produces_same_dataframes_with_same_fraction_and_seed(self):
        train1, test1 = job.split_train_test(self.test_df, self.fraction, self.seed)
        train2, test2 = job.split_train_test(self.test_df, self.fraction, self.seed)

        pl_testing.assert_frame_equal(train1, train2)
        pl_testing.assert_frame_equal(test1, test2)

    def test_locations_do_not_appear_in_both_test_and_train(self):
        train_df, test_df = job.split_train_test(self.test_df, self.fraction, self.seed)

        train_groups = set(train_df[IndCQC.location_id].unique().to_list())
        test_groups = set(test_df[IndCQC.location_id].unique().to_list())

        self.assertTrue(train_groups.isdisjoint(test_groups))

    def test_all_original_rows_are_included_in_train_or_train(self):
        train_df, test_df = job.split_train_test(self.test_df, self.fraction, self.seed)

        combined_df = pl.concat([train_df, test_df])

        pl_testing.assert_frame_equal(combined_df, self.test_df, check_row_order=False)


class ConvertDataframeToNumpyTests(unittest.TestCase):
    def setUp(self):
        self.test_df = pl.DataFrame(
            Data.convert_dataframe_to_numpy_basic_rows,
            Schemas.convert_dataframe_to_numpy_basic_schema,
            orient="row",
        )

        self.returned_multiple_feature_cols_X, self.returned_multiple_feature_cols_y = (
            job.convert_dataframe_to_numpy(
                self.test_df,
                Schemas.multiple_feature_cols,
                Schemas.dependent_col,
            )
        )
        self.expected_multiple_feature_cols_X = (
            Data.expected_numpy_multiple_feature_cols_X
        )

        self.expected_col_y = Data.expected_numpy_col_y

        self.returned_single_feature_col_X, self.returned_single_feature_col_y = (
            job.convert_dataframe_to_numpy(
                self.test_df,
                Schemas.single_feature_col,
                Schemas.dependent_col,
            )
        )
        self.expected_single_feature_col_X = Data.expected_numpy_single_feature_col_X

    def test_when_multiple_feature_cols_returns_numpy_arrays(self):
        """Tests that a DataFrame converts correctly to NumPy arrays when multiple features are selected."""

        self.assertIsInstance(self.returned_multiple_feature_cols_X, np.ndarray)
        self.assertIsInstance(self.returned_multiple_feature_cols_y, np.ndarray)

    def test_when_multiple_feature_cols_returns_expected_shaped_arrays(self):
        """Tests that a DataFrame returns the expected shapes of NumPy arrays when multiple features are selected."""

        self.assertEqual(self.returned_multiple_feature_cols_X.shape, (3, 2))
        self.assertEqual(self.returned_multiple_feature_cols_y.shape, (3,))

    def test_when_multiple_feature_cols_returns_expected_arrays(self):
        """Tests that a DataFrame returns the expected NumPy arrays when multiple features are selected."""

        self.assertTrue(
            np.array_equal(
                self.returned_multiple_feature_cols_X,
                self.expected_multiple_feature_cols_X,
            )
        )
        self.assertTrue(
            np.array_equal(
                self.returned_multiple_feature_cols_y,
                self.expected_col_y,
            )
        )

    def test_when_single_feature_col_returns_numpy_arrays(self):
        """Tests that a DataFrame converts correctly to NumPy arrays when a single feature is selected."""

        self.assertIsInstance(self.returned_single_feature_col_X, np.ndarray)
        self.assertIsInstance(self.returned_single_feature_col_y, np.ndarray)

    def test_when_single_feature_col_returns_expected_shaped_arrays(self):
        """Tests that a DataFrame returns the expected shapes of NumPy arrays when a single feature is selected."""

        self.assertEqual(self.returned_single_feature_col_X.shape, (3, 1))
        self.assertEqual(self.returned_single_feature_col_y.shape, (3,))

    def test_when_single_feature_col_returns_expected_arrays(self):
        """Tests that a DataFrame returns the expected NumPy arrays when a single feature is selected."""

        self.assertTrue(
            np.array_equal(
                self.returned_single_feature_col_X,
                self.expected_single_feature_col_X,
            )
        )
        self.assertTrue(
            np.array_equal(
                self.returned_single_feature_col_y,
                self.expected_col_y,
            )
        )

    def test_convert_dataframe_to_numpy_raises_on_missing_dependent_column(self):
        """Verifies an error is raised if the dependent column is missing."""

        f = io.StringIO()
        with redirect_stdout(f):
            with self.assertRaises(pl.exceptions.ColumnNotFoundError):
                job.convert_dataframe_to_numpy(
                    self.test_df,
                    Schemas.multiple_feature_cols,
                    "unrecognised_dependent_col",
                )

    def test_convert_dataframe_to_numpy_raises_on_missing_feature_column(self):
        """Verifies an error is raised if a feature column is missing."""

        f = io.StringIO()
        with redirect_stdout(f):
            with self.assertRaises(pl.exceptions.ColumnNotFoundError):
                job.convert_dataframe_to_numpy(
                    self.test_df,
                    ["unrecognised_feature_col"],
                    Schemas.dependent_col,
                )
