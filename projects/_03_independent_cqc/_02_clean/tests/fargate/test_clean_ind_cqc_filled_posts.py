import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._03_independent_cqc._02_clean.fargate.clean_ind_cqc_filled_posts as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import AscwdsFilteringRule

PATCH_PATH = "projects._03_independent_cqc._02_clean.fargate.clean_ind_cqc_filled_posts"


class CleanIndFilledPostsTests(unittest.TestCase):
    MERGE_IND_CQC_SOURCE = "input_dir"
    CLEANED_IND_CQC_DESTINATION = "output_dir"
    GROUPED_PROVIDERS_DESTINATION = "another_dir"


class MainTests(CleanIndFilledPostsTests):
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.calculate_care_home_status_count")
    @patch(f"{PATCH_PATH}.clean_capacity_tracker_non_res_outliers")
    @patch(f"{PATCH_PATH}.clean_capacity_tracker_care_home_outliers")
    @patch(f"{PATCH_PATH}.clean_ascwds_filled_post_outliers")
    @patch(f"{PATCH_PATH}.cUtils.create_banded_bed_count_column")
    @patch(f"{PATCH_PATH}.cUtils.calculate_filled_posts_per_bed_ratio")
    @patch(f"{PATCH_PATH}.cUtils.remove_repeated_values_over_time")
    @patch(f"{PATCH_PATH}.calculate_ascwds_filled_posts")
    @patch(f"{PATCH_PATH}.populate_missing_care_home_number_of_beds")
    @patch(f"{PATCH_PATH}.replace_zero_beds_with_null")
    @patch(f"{PATCH_PATH}.remove_dual_registration_cqc_care_homes")
    @patch(f"{PATCH_PATH}.calculate_time_registered_for")
    @patch(f"{PATCH_PATH}.calculate_time_since_dormant")
    @patch(f"{PATCH_PATH}.earliest_file_per_month_filter_expr")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main(
        self,
        scan_parquet_mock: Mock,
        earliest_file_per_month_filter_expr_mock: Mock,
        calculate_time_since_dormant_mock: Mock,
        calculate_time_registered_for_mock: Mock,
        remove_dual_registration_cqc_care_homes_mock: Mock,
        replace_zero_beds_with_null_mock: Mock,
        populate_missing_care_home_number_of_beds_mock: Mock,
        calculate_ascwds_filled_posts_mock: Mock,
        remove_repeated_values_over_time_mock: Mock,
        calculate_filled_posts_per_bed_ratio_mock: Mock,
        create_banded_bed_count_column_mock: Mock,
        clean_ascwds_filled_post_outliers_mock: Mock,
        clean_capacity_tracker_care_home_outliers_mock: Mock,
        clean_capacity_tracker_non_res_outliers_mock: Mock,
        calculate_care_home_status_count_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        scan_parquet_mock.return_value = Mock(name="merge_ind_cqc_data")

        clean_ascwds_filled_post_outliers_mock.return_value = [
            Mock(name="clean_ind_cqc_data"),
            Mock(name="grouped_providers"),
        ]

        job.main(
            self.MERGE_IND_CQC_SOURCE,
            self.CLEANED_IND_CQC_DESTINATION,
            self.GROUPED_PROVIDERS_DESTINATION,
        )

        earliest_file_per_month_filter_expr_mock.assert_called_once()
        calculate_time_registered_for_mock.assert_called_once()
        calculate_time_since_dormant_mock.assert_called_once()
        remove_dual_registration_cqc_care_homes_mock.assert_called_once()
        replace_zero_beds_with_null_mock.assert_called_once()
        populate_missing_care_home_number_of_beds_mock.assert_called_once()
        calculate_ascwds_filled_posts_mock.assert_called_once()
        remove_repeated_values_over_time_mock.assert_called_once()
        self.assertEqual(calculate_filled_posts_per_bed_ratio_mock.call_count, 2)
        create_banded_bed_count_column_mock.assert_called_once()
        clean_ascwds_filled_post_outliers_mock.assert_called_once()
        clean_capacity_tracker_care_home_outliers_mock.assert_called_once()
        clean_capacity_tracker_non_res_outliers_mock.assert_called_once()
        calculate_care_home_status_count_mock.assert_called_once()

        self.assertEqual(sink_to_parquet_mock.call_count, 2)
        sink_to_parquet_mock.assert_has_calls(
            [
                call(
                    ANY,
                    self.CLEANED_IND_CQC_DESTINATION,
                ),
                call(
                    ANY,
                    self.GROUPED_PROVIDERS_DESTINATION,
                ),
            ]
        )


class AscwdsFilledPostsSourceCastExprTests(unittest.TestCase):
    def test_casts_ascwds_filled_posts_source_to_enum(self):
        lf = pl.LazyFrame(
            {IndCQC.ascwds_filled_posts_source: ["worker_records_and_total_staff"]}
        )
        result_lf = lf.with_columns(job.ascwds_filled_posts_source_cast_expr())
        self.assertEqual(
            result_lf.collect_schema()[IndCQC.ascwds_filled_posts_source],
            CatColType.AscwdsFilledPostsSourceEnumType,
        )


class AscwdsFilteringRuleCastExprTests(unittest.TestCase):
    def test_casts_ascwds_filtering_rule_to_enum(self):
        lf = pl.LazyFrame(
            {IndCQC.ascwds_filtering_rule: [AscwdsFilteringRule.populated]}
        )
        result_lf = lf.with_columns(job.ascwds_filtering_rule_cast_expr())
        self.assertEqual(
            result_lf.collect_schema()[IndCQC.ascwds_filtering_rule],
            CatColType.AscwdsFilteringRuleEnumType,
        )

    def test_raises_on_unrecognised_filtering_rule_value(self):
        lf = pl.LazyFrame({IndCQC.ascwds_filtering_rule: ["not_a_real_filtering_rule"]})
        result_lf = lf.with_columns(job.ascwds_filtering_rule_cast_expr())
        with self.assertRaises(pl.exceptions.InvalidOperationError):
            result_lf.collect()


if __name__ == "__main__":
    unittest.main(warnings="ignore")
