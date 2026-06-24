import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils as job
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_data import (
    SLVMergeTestData as Data,
)
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_schemas import (
    SLVMergeTestSchemas as Schema,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils"


class TestPlaceholderFunction:
    def test_placeholder_function_does_something(self):
        test_lf = pl.LazyFrame(Data.test, Schema.test, orient="row")
        pass
