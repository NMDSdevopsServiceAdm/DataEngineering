import unittest

import numpy as np

from projects._03_independent_cqc._04_model.utils import metrics as job


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
