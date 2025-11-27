import unittest
import warnings

from projects._03_independent_cqc._02_clean.utils import filtering_utils as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    CleanFilteringUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    CleanFilteringUtilsSchemas as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import AscwdsFilteringRule


class CleanFilteringUtilsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)


class AddFilteringRuleColumnTests(CleanFilteringUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_filtering_rule_column(self):
        test_df = self.spark.createDataFrame(
            Data.add_filtering_column_rows, Schemas.add_filtering_column_schema
        )
        returned_df = job.add_filtering_rule_column(
            test_df,
            IndCQC.ascwds_filtering_rule,
            IndCQC.ascwds_filled_posts_dedup_clean,
            AscwdsFilteringRule.populated,
            AscwdsFilteringRule.missing_data,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_add_filtering_column_rows,
            Schemas.expected_add_filtering_column_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )


class UpdateFilteringRuleTests(CleanFilteringUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_returns_expected_labels_when_populated_values_are_nulled(self):
        test_df = self.spark.createDataFrame(
            Data.update_filtering_rule_populated_to_nulled_rows,
            Schemas.update_filtering_rule_schema,
        )
        returned_df = job.update_filtering_rule(
            test_df,
            IndCQC.ascwds_filtering_rule,
            IndCQC.ascwds_filled_posts_dedup,
            IndCQC.ascwds_filled_posts_dedup_clean,
            AscwdsFilteringRule.populated,
            AscwdsFilteringRule.contained_invalid_missing_data_code,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_update_filtering_rule_populated_to_nulled_rows,
            Schemas.update_filtering_rule_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_returns_expected_labels_when_populated_values_are_changed(self):
        test_df = self.spark.createDataFrame(
            Data.update_filtering_rule_populated_to_winsorized_rows,
            Schemas.update_filtering_rule_schema,
        )
        returned_df = job.update_filtering_rule(
            test_df,
            IndCQC.ascwds_filtering_rule,
            IndCQC.ascwds_filled_posts_dedup,
            IndCQC.ascwds_filled_posts_dedup_clean,
            AscwdsFilteringRule.populated,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_update_filtering_rule_populated_to_winsorized_rows,
            Schemas.update_filtering_rule_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_returns_expected_labels_when_populated_values_are_nulled_after_winsorization(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.update_filtering_rule_winsorized_to_nulled_rows,
            Schemas.update_filtering_rule_schema,
        )
        returned_df = job.update_filtering_rule(
            test_df,
            IndCQC.ascwds_filtering_rule,
            IndCQC.ascwds_filled_posts_dedup,
            IndCQC.ascwds_filled_posts_dedup_clean,
            AscwdsFilteringRule.populated,
            AscwdsFilteringRule.contained_invalid_missing_data_code,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_update_filtering_rule_winsorized_to_nulled_rows,
            Schemas.update_filtering_rule_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )
