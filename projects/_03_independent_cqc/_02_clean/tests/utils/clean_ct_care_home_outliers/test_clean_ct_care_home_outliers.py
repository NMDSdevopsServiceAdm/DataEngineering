import unittest

import projects._03_independent_cqc._02_clean.utils.clean_ct_care_home_outliers.clean_ct_care_home_outliers as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    NullCtPostsToBedsOutliers as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    NullCtPostsToBedsOutliers as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestCleanCtCareHomeOutliers(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class TestNullCtPostsToBedsOutliers(TestCleanCtCareHomeOutliers):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.null_ct_posts_to_beds_outliers_rows,
            Schemas.null_ct_posts_to_beds_outliers_schema,
        )
        self.returned_df = job.null_ct_posts_to_beds_outliers(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_null_ct_posts_to_beds_outliers_rows,
            Schemas.expected_null_ct_posts_to_beds_outliers_schema,
        )
        self.new_columns_added = [
            column
            for column in self.returned_df.columns
            if column not in test_df.columns
        ]

    def test_null_ct_posts_to_beds_outliers_adds_1_expected_column(
        self,
    ):
        self.assertEqual(len(self.new_columns_added), 1)
        self.assertEqual(
            self.new_columns_added[0], IndCQC.ct_care_home_total_employed_dedup_cleaned
        )

    def test_null_ct_posts_to_beds_outliers_returns_expected_values(
        self,
    ):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())
