import unittest

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.clean_utils as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    PrimaryServiceType,
    MainJobRoleLabels,
)


class NullifyJobRoleCountWhenSourceNotAscwds(unittest.TestCase):
    def setUp(self) -> None:
        self.test_schema = {
            IndCQC.ascwds_filled_posts_dedup_clean: pl.Float64,
            IndCQC.estimate_filled_posts: pl.Float64,
            IndCQC.estimate_filled_posts_source: pl.String,
            IndCQC.ascwds_job_role_counts: pl.Int64,
        }
        self.input_rows_that_meet_condition = [
            (10.0, 10.0, EstimateFilledPostsSource.ascwds_pir_merged, 1),
            (10.0, 10.0, EstimateFilledPostsSource.ascwds_pir_merged, 2),
        ]

        self.expected_rows_that_meet_condition = self.input_rows_that_meet_condition

    def _create_input_lf(self, extra_rows: list[tuple]) -> pl.LazyFrame:
        """Set the input LazyFrame up with rows that meet condition + given rows."""
        return pl.LazyFrame(
            [*self.input_rows_that_meet_condition, *extra_rows],
            self.test_schema,
            orient="row",
        )

    def _create_expected_lf(self, extra_rows: list[tuple]) -> pl.LazyFrame:
        """Set the expected LazyFrame up with rows that meet condition + given rows."""
        return pl.LazyFrame(
            [*self.expected_rows_that_meet_condition, *extra_rows],
            self.test_schema,
            orient="row",
        )

    def test_nullifies_when_source_not_ascwds(self):
        input_lf = self._create_input_lf(
            # Row with non-ASCWDS source
            [(10.0, 10.0, EstimateFilledPostsSource.care_home_model, 2)],
        )
        expected_lf = self._create_expected_lf(
            # Row with non-ASCWDS source (counts nullified)
            [(10.0, 10.0, EstimateFilledPostsSource.care_home_model, None)],
        )
        returned_lf = job.nullify_job_role_count_when_source_not_ascwds(input_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_nullifies_when_estimate_doesnt_match_ascwds(self):
        input_lf = self._create_input_lf(
            # Row that doesn't match estimate filled posts.
            [(None, 20.0, EstimateFilledPostsSource.ascwds_pir_merged, 1)],
        )
        expected_lf = self._create_expected_lf(
            # Row that doesn't match estimate filled posts (counts nullified)
            [(None, 20.0, EstimateFilledPostsSource.ascwds_pir_merged, None)],
        )
        returned_lf = job.nullify_job_role_count_when_source_not_ascwds(input_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestFilterAscwdsJobRoleCountWhenJobGroupRatiosOutsidePercentileBounds(
    unittest.TestCase
):
    test_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.primary_service_type: pl.String,
        IndCQC.main_job_role_clean_labelled: pl.String,
        IndCQC.ascwds_job_role_counts: pl.Int64,
    }

    test_data = [
        # Placeholder test data - to be implemented when function is implemented.
        ("loc1", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 20),
        ("loc1", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 1 ),
        ("loc1", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_manager, 1),
        ("loc1", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.other_non_care_related_staff, 1),
        ("loc2", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 1),
        ("loc2", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 1 ),
        ("loc2", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_manager, 1),
        ("loc2", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.other_non_care_related_staff, 1),
        ("loc3", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 1),
        ("loc3", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 1 ),
        ("loc3", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_manager, 1),
        ("loc3", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.other_non_care_related_staff, 1),
    ] # fmt:skip

    test_lf = pl.LazyFrame(test_data, test_schema, orient="row")
    expected_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.primary_service_type: pl.String,
        IndCQC.main_job_role_clean_labelled: pl.String,
        IndCQC.ascwds_job_role_counts: pl.Int64,
        IndCQC.ascwds_job_role_counts_cleaned: pl.Int64,
    }

    expected_data = [
        # Placeholder test data - to be implemented when function is implemented.
        ("loc1", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 20, None),
        ("loc1", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 1 , None),
        ("loc1", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_manager, 1, None),
        ("loc1", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.other_non_care_related_staff, 1, None),
        ("loc2", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 1, 1),
        ("loc2", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 1 , 1),
        ("loc2", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_manager, 1, 1),
        ("loc2", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.other_non_care_related_staff, 1, 1),
        ("loc3", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 1, 1),
        ("loc3", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 1 , 1),
        ("loc3", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_manager, 1, 1),
        ("loc3", "2024-01-01", PrimaryServiceType.care_home_only, MainJobRoleLabels.other_non_care_related_staff, 1, 1),
    ] # fmt:skip
    expected_lf = pl.LazyFrame(expected_data, expected_schema, orient="row")
    upper_percentile_bound = 0.8
    lower_percentile_bound = 0.2

    def test_placeholder(self):
        # Placeholder test - to be implemented when function is implemented.
        returned_lf = job.filter_placeholder(self.test_lf)
        pl_testing.assert_frame_equal(returned_lf, self.expected_lf)
