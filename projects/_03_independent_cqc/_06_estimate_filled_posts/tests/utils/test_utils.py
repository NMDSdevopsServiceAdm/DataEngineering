import unittest
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

import projects._03_independent_cqc._06_estimate_filled_posts.utils.utils as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    EstimatesFilledPostUtils as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    EstimatesFilledPostUtils as Schemas,
)


class TestClassifySpecialisms(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

    def test_classify_specialisms(self):
        test_df = self.spark.createDataFrame(
            Data.classify_specialisms_rows, Schemas.classify_specialisms_schema
        )
        returned_df = job.classify_specialisms(
            test_df,
            IndCQC.specialist_generalist_other_dementia,
            IndCQC.specialist_generalist_other_lda,
            IndCQC.specialist_generalist_other_mh,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_classify_specialisms_rows,
            Schemas.expected_classify_specialisms_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())
