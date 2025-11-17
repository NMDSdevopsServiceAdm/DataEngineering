import unittest

from projects._03_independent_cqc._04_feature_engineering.fargate.utils import (
    feature_utils as job,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class ExpandEncodeAndExtractFeaturesTests(unittest.TestCase):
    pass


class AddArrayColumnCountTests(unittest.TestCase):
    pass


class CapIntegerAtMaxValueTests(unittest.TestCase):
    pass


class AddDateIndexColumnTests(unittest.TestCase):
    pass


class GroupRuralUrbanSparseCategoriesTests(unittest.TestCase):
    pass
