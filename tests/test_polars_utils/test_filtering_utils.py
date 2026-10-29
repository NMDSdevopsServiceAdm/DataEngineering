import unittest

import polars as pl
import polars.testing as pl_testing
import pytest

from polars_utils import filtering_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from tests.test_polars_utils_data import FilteringUtilsData as Data
from tests.test_polars_utils_schemas import FilteringUtilsSchemas as Schemas
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    AscwdsFilteringRule,
    JobRoleFilteringRule,
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

    def test_returns_categorical_col_when_provided(self):
        test_lf = pl.LazyFrame(
            data=Data.returns_categorical_col_rows,
            schema=Schemas.returns_categorical_col_schema,
            orient="row",
        )
        test_categorical = CatColType.JobRoleFilteringRuleCatType
        returned_lf = job.add_filtering_rule_column(
            test_lf,
            filter_rule_col_name=IndCQC.job_role_filtering_rule,
            col_to_filter=IndCQC.ascwds_job_role_counts,
            populated_rule=JobRoleFilteringRule.populated,
            missing_rule=JobRoleFilteringRule.missing_raw_data,
            categorical_type=test_categorical,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_returns_categorical_col_rows,
            schema=Schemas.expected_returns_categorical_col_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestUpdateFilteringRule:
    @staticmethod
    def run(rows, expected_rows, schema=Schemas.update_filtering_rule_schema, **kwargs):
        returned_lf = job.update_filtering_rule(
            pl.LazyFrame(rows, schema, orient="row"),
            filter_rule_col_name=IndCQC.ascwds_filtering_rule,
            clean_col_name=IndCQC.ascwds_filled_posts_dedup_clean,
            populated_rule=AscwdsFilteringRule.populated,
            **kwargs,
        )
        expected_lf = pl.LazyFrame(expected_rows, schema, orient="row")
        pl_testing.assert_frame_equal(
            returned_lf.sort(IndCQC.location_id), expected_lf.sort(IndCQC.location_id)
        )

    def test_updates_rule_when_populated_values_are_nulled_without_uncapped_col(self):
        self.run(
            Data.update_filtering_rule_populated_to_nulled_rows,
            Data.expected_update_filtering_rule_populated_to_nulled_rows,
            new_rule_name=AscwdsFilteringRule.contained_invalid_missing_data_code,
        )

    def test_ignores_changed_values_without_uncapped_col(self):
        self.run(
            Data.update_filtering_rule_populated_to_winsorized_rows,
            Data.update_filtering_rule_populated_to_winsorized_rows,
            new_rule_name=AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        )

    def test_updates_rule_when_populated_values_are_nulled_with_uncapped_col(self):
        self.run(
            Data.update_filtering_rule_populated_to_nulled_rows,
            Data.expected_update_filtering_rule_populated_to_nulled_rows,
            new_rule_name=AscwdsFilteringRule.contained_invalid_missing_data_code,
            uncapped_col_name=IndCQC.ascwds_filled_posts_dedup,
        )

    def test_updates_rule_when_populated_values_are_changed_with_uncapped_col(self):
        self.run(
            Data.update_filtering_rule_populated_to_winsorized_rows,
            Data.expected_update_filtering_rule_populated_to_winsorized_rows,
            new_rule_name=AscwdsFilteringRule.winsorized_beds_ratio_outlier,
            uncapped_col_name=IndCQC.ascwds_filled_posts_dedup,
        )

    def test_updates_rule_when_winsorized_values_are_nulled(self):
        self.run(
            Data.update_filtering_rule_winsorized_to_nulled_rows,
            Data.expected_update_filtering_rule_winsorized_to_nulled_rows,
            new_rule_name=AscwdsFilteringRule.contained_invalid_missing_data_code,
            winsorized_rule=AscwdsFilteringRule.winsorized_beds_ratio_outlier,
            uncapped_col_name=IndCQC.ascwds_filled_posts_dedup,
        )

    def test_ignores_winsorized_rows_when_winsorized_rule_is_none(self):
        self.run(
            Data.update_filtering_rule_winsorized_to_nulled_rows,
            Data.update_filtering_rule_winsorized_to_nulled_rows,
            new_rule_name=AscwdsFilteringRule.contained_invalid_missing_data_code,
            winsorized_rule=None,
        )

    @pytest.mark.parametrize(
        "rows, expected_rows, new_rule_name, uncapped",
        [
            pytest.param(
                Data.update_filtering_rule_populated_to_nulled_rows,
                Data.expected_update_filtering_rule_populated_to_nulled_rows,
                AscwdsFilteringRule.contained_invalid_missing_data_code,
                None,
                id="nulled",
            ),
            pytest.param(
                Data.update_filtering_rule_populated_to_winsorized_rows,
                Data.expected_update_filtering_rule_populated_to_winsorized_rows,
                AscwdsFilteringRule.winsorized_beds_ratio_outlier,
                IndCQC.ascwds_filled_posts_dedup,
                id="changed",
            ),
        ],
    )
    def test_returns_categorical_rule_col_when_type_provided(
        self, rows, expected_rows, new_rule_name, uncapped
    ):
        self.run(
            rows,
            expected_rows,
            Schemas.update_filtering_rule_schema_categorical,
            new_rule_name=new_rule_name,
            uncapped_col_name=uncapped,
            categorical_type=CatColType.JobRoleFilteringRuleCatType,
        )


class TestLookbackCapFilterExpr:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.lookback_cap_filter_test_cases
        ],
    )
    def test_function_returns_expected_values(self, case):
        date_col = IndCQC.cqc_location_import_date

        expr = job.lookback_cap_filter_expr(
            today=case.today,
            fy_start_month=case.fy_start_month,
            lookback_fy_years=case.lookback_fy_years,
        )

        df = pl.DataFrame({date_col: case.input_data})

        result = df.with_columns(expr.alias("keep"))

        assert result["keep"].to_list() == case.expected


class TestEarliestFilePerMonthFilterExpr:
    def test_returns_only_earliest_file_per_month(self):
        test_lf = pl.LazyFrame(Data.earliest_file_per_month_rows)
        expected_lf = pl.LazyFrame(Data.expected_earliest_file_per_month_rows)

        returned_lf = test_lf.filter(job.earliest_file_per_month_filter_expr())

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_uses_the_passed_date_col(self):
        expr = job.earliest_file_per_month_filter_expr("some_other_column")

        assert set(expr.meta.root_names()) == {"some_other_column"}


class TestNotNullFilterExpr:
    def test_returns_only_rows_where_column_is_not_null(self):
        test_lf = pl.LazyFrame({"location_id": [1, None, 3]})
        expected_lf = pl.LazyFrame({"location_id": [1, 3]})

        returned_lf = test_lf.filter(job.not_null_filter_expr("location_id"))

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_uses_the_passed_column(self):
        expr = job.not_null_filter_expr("some_other_column")

        assert set(expr.meta.root_names()) == {"some_other_column"}
