import unittest

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.null_posts_per_bed_ratio_outliers as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    NullCtPostsToBedsOutliers as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    NullCtPostsToBedsOutliers as Schemas,
)


class TestNullCtPostsToBedsOutliers(unittest.TestCase):
    def setUp(self) -> None:
        test_lf = pl.LazyFrame(
            Data.null_ct_posts_to_beds_outliers_rows,
            Schemas.null_ct_posts_to_beds_outliers_schema,
            orient="row",
        )
        self.returned_lf = job.null_posts_per_bed_outliers(test_lf)
        self.expected_lf = pl.LazyFrame(
            Data.expected_null_ct_posts_to_beds_outliers_rows,
            Schemas.null_ct_posts_to_beds_outliers_schema,
            orient="row",
        )

    def test_function_returns_expected_values(self):
        pl_testing.assert_frame_equal(self.returned_lf, self.expected_lf)


class RatioCutoffValueTests(unittest.TestCase):
    def test_ratio_cutoffs_are_correct(self):
        self.assertEqual(job.MINIMUM_RATIO_CUTOFF, 0.66)
        self.assertEqual(job.MAXIMUM_RATIO_CUTOFF, 6.0)
