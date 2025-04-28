from dataclasses import dataclass

from pyspark.ml.linalg import Vectors


@dataclass
class ModelMetrics:
    model_metrics_rows = [
        ("1-001", None, 50.0, Vectors.dense([10.0, 1.0, 0.0])),
        ("1-002", 37, 40.0, Vectors.dense([20.0, 0.0, 1.0])),
    ]

    generate_metric_rows = [
        ("1-001", 50.0, 46.8),
        ("1-002", 10.0, 12.2),
    ]

    calculate_residual_non_res_rows = [
        ("1-001", None, 50.0, 46.8),
        ("1-002", None, 10.0, 43.2),
    ]
    expected_calculate_residual_non_res_rows = [
        ("1-001", None, 50.0, 46.8, 3.2),
        ("1-002", None, 10.0, 43.2, -33.2),
    ]

    calculate_residual_care_home_rows = [
        ("1-001", 50, 60.0, 1.1),
        ("1-002", 2, 5.0, 6.0),
    ]
    expected_calculate_residual_care_home_rows = [
        ("1-001", 50, 60.0, 1.1, 5.0),
        ("1-002", 2, 5.0, 6.0, -7.0),
    ]
