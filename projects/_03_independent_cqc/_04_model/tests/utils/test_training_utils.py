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


class SplitTrainTestTests(unittest.TestCase):
    pass


class ConvertDataframeToNumpyTests(unittest.TestCase):
    pass
