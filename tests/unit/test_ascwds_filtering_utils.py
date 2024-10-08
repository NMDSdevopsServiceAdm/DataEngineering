import unittest
import warnings

from tests.test_file_data import (
    ASCWDSFilteringUtilsData as Data,
)
from tests.test_file_schemas import (
    ASCWDSFilteringUtilsSchemas as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers import (
    ascwds_filtering_utils as job,
)


class ASCWDSFilteringUtilsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)


class AddFilteringRuleColumnTests(ASCWDSFilteringUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_filtering_rule_column(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.add_filtering_column_rows, Schemas.add_filtering_column_schema
        )
        returned_df = job.add_filtering_rule_column(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_add_filtering_column_rows,
            Schemas.expected_add_filtering_column_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )


class UpdateFilteringRuleTests(ASCWDSFilteringUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_update_filtering_rule_returns_correct_labels_when_populated_values_are_nulled(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.update_filtering_rule_populated_to_nulled_rows,
            Schemas.update_filtering_rule_schema,
        )
        returned_df = job.update_filtering_rule(
            test_df,
            AscwdsFilteringRule.contained_invalid_missing_data_code,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_update_filtering_rule_populated_to_nulled_rows,
            Schemas.update_filtering_rule_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_update_filtering_rule_returns_correct_labels_when_populated_values_are_winsorized(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.update_filtering_rule_populated_to_winsorized_rows,
            Schemas.update_filtering_rule_schema,
        )
        returned_df = job.update_filtering_rule(
            test_df,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_update_filtering_rule_populated_to_winsorized_rows,
            Schemas.update_filtering_rule_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_update_filtering_rule_returns_correct_labels_when_winsorized_values_are_nulled(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.update_filtering_rule_winsorized_to_nulled_rows,
            Schemas.update_filtering_rule_schema,
        )
        returned_df = job.update_filtering_rule(
            test_df,
            AscwdsFilteringRule.contained_invalid_missing_data_code,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_update_filtering_rule_winsorized_to_nulled_rows,
            Schemas.update_filtering_rule_schema,
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )
