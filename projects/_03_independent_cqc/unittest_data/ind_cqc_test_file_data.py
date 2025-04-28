from dataclasses import dataclass

from pyspark.ml.linalg import Vectors


@dataclass
class TrainLinearRegressionModelData:
    feature_rows = [
        ("1-001", Vectors.dense([12.0, 0.0, 1.0])),
        ("1-002", Vectors.dense([50.0, 1.0, 1.0])),
        ("1-003", None),
    ]


@dataclass
class RunLinearRegressionModelData:
    feature_rows = [
        ("1-001", 10, Vectors.dense([12.0, 0.0, 1.0])),
        ("1-002", 40, Vectors.dense([50.0, 1.0, 1.0])),
        ("1-003", None),
    ]
