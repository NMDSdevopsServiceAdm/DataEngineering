import unittest
import warnings
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._05_model.jobs.train_linear_regression_model as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    TrainLinearRegressionModelData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    TrainLinearRegressionModelSchema as Schemas,
)
from utils import utils

PATCH_PATH: str = (
    "projects._03_independent_cqc._05_model.jobs.train_linear_regression_model"
)


class Main(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.feature_rows, Schemas.feature_schema
        )

        warnings.simplefilter("ignore", ResourceWarning)
