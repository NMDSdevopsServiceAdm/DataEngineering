import unittest

import numpy as np
import polars as pl
import polars.testing as pl_testing
from sklearn.discriminant_analysis import StandardScaler
from sklearn.linear_model import Lasso, LinearRegression
from sklearn.pipeline import Pipeline

from projects._03_independent_cqc._04_model.utils import model_utils as job
from projects._03_independent_cqc._04_model.utils.value_labels import ModelTypes
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ModelUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ModelUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class BuildModelTests(unittest.TestCase):
    def test_build_model_linear_returns_linear_regression(self):
        model = job.build_model(ModelTypes.linear_regression)

        self.assertIsInstance(model, LinearRegression)

    def test_build_model_linear_passes_parameters(self):
        params = {"fit_intercept": False, "positive": True}

        model = job.build_model(ModelTypes.linear_regression, params)

        self.assertFalse(model.fit_intercept)
        self.assertTrue(model.positive)

    def test_build_model_lasso_returns_pipeline(self):
        model = job.build_model(ModelTypes.lasso)

        self.assertIsInstance(model, Pipeline)

    def test_build_model_lasso_pipeline_steps(self):
        model = job.build_model(ModelTypes.lasso)

        steps = model.named_steps

        self.assertIn("scaler", steps)
        self.assertIn(ModelTypes.lasso, steps)

        self.assertIsInstance(steps["scaler"], StandardScaler)
        self.assertIsInstance(steps[ModelTypes.lasso], Lasso)

    def test_build_model_lasso_passes_parameters_to_lasso(self):
        params = {"alpha": 0.5}

        model = job.build_model(ModelTypes.lasso, params)

        lasso = model.named_steps[ModelTypes.lasso]

        self.assertEqual(lasso.alpha, 0.5)

    def test_build_model_invalid_model_type_raises(self):
        with self.assertRaises(ValueError) as context:
            job.build_model("invalid")

        self.assertIn("Unknown model type: invalid", str(context.exception))


class MetricsTests(unittest.TestCase):
    def test_calculate_metrics_perfect_prediction(self):
        y_known = np.array([1.0, 2.0, 3.0])
        y_predicted = np.array([1.0, 2.0, 3.0])

        metrics = job.calculate_metrics(y_known, y_predicted)

        self.assertEqual(metrics["r2"], 1.0)
        self.assertEqual(metrics["rmse"], 0.0)

    def test_calculate_metrics_known_values(self):
        y_known = np.array([0.0, 1.0, 2.0])
        y_predicted = np.array([0.0, 2.0, 1.0])

        metrics = job.calculate_metrics(y_known, y_predicted)

        # R2 should be less than 1 for imperfect predictions
        self.assertLess(metrics["r2"], 1.0)

        # RMSE should be positive
        self.assertGreater(metrics["rmse"], 0.0)

    def test_calculate_metrics_output_schema(self):
        y_known = np.array([1, 2, 3])
        y_predicted = np.array([1, 2, 4])

        metrics = job.calculate_metrics(y_known, y_predicted)

        self.assertEqual(set(metrics.keys()), {"r2", "rmse"})
        self.assertIsInstance(metrics["r2"], float)
        self.assertIsInstance(metrics["rmse"], float)

    def test_calculate_metrics_integer_inputs(self):
        y_known = np.array([1, 2, 3])
        y_predicted = np.array([2, 2, 2])

        metrics = job.calculate_metrics(y_known, y_predicted)

        self.assertIsInstance(metrics["r2"], float)
        self.assertIsInstance(metrics["rmse"], float)


class CreatePredictionsDataFrameTests(unittest.TestCase):
    def setUp(self) -> None:
        self.features_df = pl.DataFrame(
            Data.features_rows, Schemas.features_schema, orient="row"
        )

        self.predictions = Data.predictions

        self.index_col = "index"
        self.model_name = "model_A"
        self.model_version = "1.2.0"
        self.run_number = 7

        self.run_id_col_name = f"{self.model_name}_predictions_run_id"
        self.expected_run_id = "model_A_v1.2.0_r7"

    def test_returns_expected_predictions_dataframe(self):
        returned_df = job.create_predictions_dataframe(
            self.features_df,
            self.predictions,
            self.index_col,
            self.model_name,
            self.model_version,
            self.run_number,
        )

        expected_df = pl.DataFrame(
            Data.expected_predictions_dataframe_rows,
            Schemas.expected_predictions_dataframe_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_raises_value_error_when_predictions_length_mismatch(self):
        with self.assertRaises(ValueError) as context:
            job.create_predictions_dataframe(
                self.features_df,
                Data.mismatch_predictions,
                self.index_col,
                self.model_name,
                self.model_version,
                self.run_number,
            )

        self.assertIn("Predictions length", str(context.exception))
        self.assertIn("does not match DataFrame row count", str(context.exception))

    def test_run_id_column_is_correct_string(self):
        returned_df = job.create_predictions_dataframe(
            self.features_df,
            self.predictions,
            self.index_col,
            self.model_name,
            self.model_version,
            self.run_number,
        )

        self.assertEqual(returned_df[self.run_id_col_name].dtype, pl.Utf8)
        self.assertTrue(
            all(
                run_id == self.expected_run_id
                for run_id in returned_df[self.run_id_col_name]
            )
        )
