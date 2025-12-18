import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._01_merge.fargate.utils.merge_utils as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._01_merge.fargate.utils.merge_utils"


class MergeUtilsTests(unittest.TestCase):

    def test_main_runs_successfully(
        self,
    ):
        test_lf = #TODO
        returned_lf = job.a_function(lf=test_lf)
        expected_lf = #TODO

        self.assertEqual(returned_lf, expected_lf)
