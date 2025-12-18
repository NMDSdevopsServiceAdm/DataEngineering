import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._01_merge.fargate.utils.merge_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    MergeUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    MergeUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._01_merge.fargate.utils.merge_utils"


class MergeUtilsTests(unittest.TestCase):

    def test_function(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.test,
            schema=Schemas.test,
        )
        returned_lf = job.a_function(lf=test_lf)
        expected_lf = test_lf

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
