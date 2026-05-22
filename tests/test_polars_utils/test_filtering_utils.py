import unittest

import polars as pl
import polars.testing as pl_testing

from polars_utils import filtering_utils as job
from tests.test_polars_utils_data import (
    FilteringUtilsData as Data,
)
from tests.test_polars_utils_schemas import (
    FilteringUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    AscwdsFilteringRule,
    JobRoleFilteringRule,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as JRValues,
)


class AddFilteringRuleColumnTests(unittest.TestCase):
    def test_returned_values_are_populated_or_missing_when_data_is_populated_or_missing(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.add_filtering_column_rows,
            schema=Schemas.add_filtering_column_schema,
            orient="row",
        )
        returned_lf = job.add_filtering_rule_column(
            test_lf,
            IndCQC.ascwds_filtering_rule,
            IndCQC.ascwds_filled_posts_dedup_clean,
            AscwdsFilteringRule.populated,
            AscwdsFilteringRule.missing_data,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_add_filtering_column_rows,
            Schemas.expected_add_filtering_column_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_enum_col_when_provided(self):
        test_lf = pl.LazyFrame(
            data=Data.returns_enum_col_rows,
            schema=Schemas.returns_enum_col_schema,
            orient="row",
        )
        returned_lf = job.add_filtering_rule_column(
            test_lf,
            filter_rule_col_name=IndCQC.job_role_filtering_rule,
            col_to_filter=IndCQC.ascwds_job_role_counts,
            populated_rule=JobRoleFilteringRule.populated,
            missing_rule=JobRoleFilteringRule.missing_raw_data,
            enum_values=JRValues.job_role_filtering_rule_column_values.categorical_values,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_return_enum_col_rows,
            schema=Schemas.expected_returns_enum_col_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class UpdateFilteringRuleTests(unittest.TestCase):
    def test_returns_expected_labels_when_populated_values_are_nulled(self):
        test_lf = pl.LazyFrame(
            Data.update_filtering_rule_populated_to_nulled_rows,
            Schemas.update_filtering_rule_schema,
            orient="row",
        )
        returned_lf = job.update_filtering_rule(
            test_lf,
            IndCQC.ascwds_filtering_rule,
            IndCQC.ascwds_filled_posts_dedup,
            IndCQC.ascwds_filled_posts_dedup_clean,
            AscwdsFilteringRule.populated,
            AscwdsFilteringRule.contained_invalid_missing_data_code,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_update_filtering_rule_populated_to_nulled_rows,
            schema=Schemas.update_filtering_rule_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_expected_labels_when_populated_values_are_changed(self):
        test_lf = pl.LazyFrame(
            data=Data.update_filtering_rule_populated_to_winsorized_rows,
            schema=Schemas.update_filtering_rule_schema,
            orient="row",
        )
        returned_lf = job.update_filtering_rule(
            test_lf,
            IndCQC.ascwds_filtering_rule,
            IndCQC.ascwds_filled_posts_dedup,
            IndCQC.ascwds_filled_posts_dedup_clean,
            AscwdsFilteringRule.populated,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_update_filtering_rule_populated_to_winsorized_rows,
            schema=Schemas.update_filtering_rule_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(
            returned_lf.sort(IndCQC.location_id).collect(), expected_lf.collect()
        )

    def test_returns_expected_labels_when_populated_values_are_nulled_after_winsorization(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.update_filtering_rule_winsorized_to_nulled_rows,
            schema=Schemas.update_filtering_rule_schema,
            orient="row",
        )
        returned_lf = job.update_filtering_rule(
            test_lf,
            IndCQC.ascwds_filtering_rule,
            IndCQC.ascwds_filled_posts_dedup,
            IndCQC.ascwds_filled_posts_dedup_clean,
            AscwdsFilteringRule.populated,
            AscwdsFilteringRule.contained_invalid_missing_data_code,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_update_filtering_rule_winsorized_to_nulled_rows,
            schema=Schemas.update_filtering_rule_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)
