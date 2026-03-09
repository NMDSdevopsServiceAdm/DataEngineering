import projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.null_posts_per_bed_ratio_outliers as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    NullCtPostsToBedsOutliers as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    NullCtPostsToBedsOutliers as Schemas,
)
from tests.base_test import SparkBaseTest


class TestCleanCtCareHomeOutliers(SparkBaseTest):
    def setUp(self): ...


class TestNullCtPostsToBedsOutliers(TestCleanCtCareHomeOutliers):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.null_ct_posts_to_beds_outliers_rows,
            Schemas.null_ct_posts_to_beds_outliers_schema,
        )
        self.returned_df = job.null_posts_per_bed_outliers(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_null_ct_posts_to_beds_outliers_rows,
            Schemas.null_ct_posts_to_beds_outliers_schema,
        )

    def test_null_ct_posts_to_beds_outliers_returns_expected_values(self):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())


class RatioCutoffValueTests(TestCleanCtCareHomeOutliers):
    def test_ratio_cutoffs_are_correct(self):
        self.assertEqual(job.MINIMUM_RATIO_CUTOFF, 0.66)
        self.assertEqual(job.MAXIMUM_RATIO_CUTOFF, 6.0)
