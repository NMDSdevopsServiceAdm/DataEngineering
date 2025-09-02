import unittest
from pyspark.sql import functions as F

import projects._03_independent_cqc._03_impute.utils.utils as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ImputeUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ImputeUtilsSchema as Schemas,
)

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome


class TestImputeUtils(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class CombineCareHomeAndNonResValuesIntoSingleColumnTests(TestImputeUtils):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.combine_care_home_and_non_res_values_into_single_column_rows,
            Schemas.combine_care_home_and_non_res_values_into_single_column_schema,
        )
        self.returned_df = job.combine_care_home_and_non_res_values_into_single_column(
            test_df,
            IndCQC.filled_posts_per_bed_ratio,
            IndCQC.ascwds_filled_posts_dedup_clean,
            IndCQC.combined_ratio_and_filled_posts,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_combine_care_home_and_non_res_values_into_single_column_rows,
            Schemas.expected_combine_care_home_and_non_res_values_into_single_column_schema,
        )

    def test_combine_care_home_and_non_res_values_into_single_column_returns_expected_columns(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_column_values_match_expected_when_care_home(self):
        returned_care_home_data = (
            self.returned_df.where(F.col(IndCQC.care_home) == CareHome.care_home)
            .sort(IndCQC.location_id)
            .collect()
        )
        expected_care_home_data = self.expected_df.where(
            F.col(IndCQC.care_home) == CareHome.care_home
        ).collect()

        for i in range(len(returned_care_home_data)):
            self.assertEqual(
                returned_care_home_data[i][IndCQC.combined_ratio_and_filled_posts],
                expected_care_home_data[i][IndCQC.combined_ratio_and_filled_posts],
                f"Returned row {i} does not match expected",
            )

    def test_returned_column_values_match_expected_when_not_care_home(self):
        returned_not_care_home_data = (
            self.returned_df.where(F.col(IndCQC.care_home) != CareHome.care_home)
            .sort(IndCQC.location_id)
            .collect()
        )
        expected_not_care_home_data = self.expected_df.where(
            F.col(IndCQC.care_home) != CareHome.care_home
        ).collect()

        for i in range(len(returned_not_care_home_data)):
            self.assertEqual(
                returned_not_care_home_data[i][IndCQC.combined_ratio_and_filled_posts],
                expected_not_care_home_data[i][IndCQC.combined_ratio_and_filled_posts],
                f"Returned row {i} does not match expected",
            )
