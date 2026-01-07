import unittest

import numpy as np
from sklearn.discriminant_analysis import StandardScaler
from sklearn.linear_model import Lasso, LinearRegression
from sklearn.pipeline import Pipeline

from projects._03_independent_cqc._04_model.utils import model_utils as job
from projects._03_independent_cqc._04_model.utils.value_labels import ModelTypes


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
