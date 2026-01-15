import unittest
import warnings

from projects._03_independent_cqc._04_feature_engineering.utils import helper as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelFeatures as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelFeatures as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.utils import get_spark


class LocationsFeatureEngineeringTests(unittest.TestCase):
    def setUp(self):
        self.spark = get_spark()

        warnings.simplefilter("ignore", ResourceWarning)
