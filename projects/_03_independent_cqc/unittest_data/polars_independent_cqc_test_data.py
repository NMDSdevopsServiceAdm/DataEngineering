from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)
from utils.column_values.categorical_column_values import PublishedJobRoleLabels


@dataclass
class RemoveRepeatedValuesOverTimeAsGroupTestCase:
    id: str
    input_data: dict[str, Any]
    columns_to_clean: list[str]
    partition_by_columns: str | list[str]
    date_column: str
    expected_data: dict[str, Any]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


@dataclass
class PercentageShareHorizontalTestCase:
    id: str
    input_data: dict[str, Any]
    columns: list[str]
    output_columns: list[str]
    expected_data: dict[str, Any]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


class TestCleaningUtilsData:
    remove_repeated_values_over_time_as_group_test_cases = [
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="first_row_of_partition_is_kept",
            input_data={
                "location_id": ["loc_1"],
                "date": [date(2024, 1, 1)],
                "first_value": [1],
                "second_value": [2],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns="location_id",
            date_column="date",
            expected_data={
                "location_id": ["loc_1"],
                "date": [date(2024, 1, 1)],
                "first_value": [1],
                "second_value": [2],
                "first_value_dedup": [1],
                "second_value_dedup": [2],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="full_group_repeat_is_nulled",
            input_data={
                "location_id": ["loc_1", "loc_1"],
                "date": [date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [1, 1],
                "second_value": [2, 2],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns="location_id",
            date_column="date",
            expected_data={
                "location_id": ["loc_1", "loc_1"],
                "date": [date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [1, 1],
                "second_value": [2, 2],
                "first_value_dedup": [1, None],
                "second_value_dedup": [2, None],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="single_column_change_keeps_the_whole_group",
            input_data={
                "location_id": ["loc_1", "loc_1"],
                "date": [date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [1, 3],
                "second_value": [2, 2],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns="location_id",
            date_column="date",
            expected_data={
                "location_id": ["loc_1", "loc_1"],
                "date": [date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [1, 3],
                "second_value": [2, 2],
                "first_value_dedup": [1, 3],
                "second_value_dedup": [2, 2],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="consecutive_all_null_groups_are_treated_as_unchanged_and_nulled",
            input_data={
                "location_id": ["loc_1", "loc_1"],
                "date": [date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [None, None],
                "second_value": [None, None],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns="location_id",
            date_column="date",
            expected_data={
                "location_id": ["loc_1", "loc_1"],
                "date": [date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [None, None],
                "second_value": [None, None],
                "first_value_dedup": [None, None],
                "second_value_dedup": [None, None],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="transition_from_all_null_to_populated_group_is_kept",
            input_data={
                "location_id": ["loc_1", "loc_1"],
                "date": [date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [None, 5],
                "second_value": [None, 3],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns="location_id",
            date_column="date",
            expected_data={
                "location_id": ["loc_1", "loc_1"],
                "date": [date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [None, 5],
                "second_value": [None, 3],
                "first_value_dedup": [None, 5],
                "second_value_dedup": [None, 3],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="multiple_partitions_deduplicated_independently",
            input_data={
                "location_id": ["loc_1", "loc_1", "loc_2", "loc_2"],
                "job_role": ["care_worker"] * 4,
                "date": [
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, 1, 2],
                "second_value": [2, 2, 2, 2],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            expected_data={
                "location_id": ["loc_1", "loc_1", "loc_2", "loc_2"],
                "job_role": ["care_worker"] * 4,
                "date": [
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, 1, 2],
                "second_value": [2, 2, 2, 2],
                "first_value_dedup": [1, None, 1, 2],
                "second_value_dedup": [2, None, 2, 2],
            },
        ),
    ]

    percentage_share_horizontal_test_cases = [
        PercentageShareHorizontalTestCase(
            id="typical_split_returns_proportional_shares",
            input_data={"p": [3], "t": [1], "o": [4]},
            columns=["p", "t", "o"],
            output_columns=["p_pct", "t_pct", "o_pct"],
            expected_data={"p_pct": [0.375], "t_pct": [0.125], "o_pct": [0.5]},
        ),
        PercentageShareHorizontalTestCase(
            id="single_nonzero_category_returns_100_percent_share",
            input_data={"p": [5], "t": [0], "o": [0]},
            columns=["p", "t", "o"],
            output_columns=["p_pct", "t_pct", "o_pct"],
            expected_data={"p_pct": [1.0], "t_pct": [0.0], "o_pct": [0.0]},
        ),
        PercentageShareHorizontalTestCase(
            id="zero_sum_returns_null_for_all_outputs",
            input_data={"p": [0], "t": [0], "o": [0]},
            columns=["p", "t", "o"],
            output_columns=["p_pct", "t_pct", "o_pct"],
            expected_data={"p_pct": [None], "t_pct": [None], "o_pct": [None]},
        ),
        PercentageShareHorizontalTestCase(
            id="partial_null_row_nulls_all_outputs_despite_nonzero_sum",
            input_data={"p": [None], "t": [2], "o": [2]},
            columns=["p", "t", "o"],
            output_columns=["p_pct", "t_pct", "o_pct"],
            expected_data={"p_pct": [None], "t_pct": [None], "o_pct": [None]},
        ),
        PercentageShareHorizontalTestCase(
            id="all_null_row_returns_null_for_all_outputs",
            input_data={"p": [None], "t": [None], "o": [None]},
            columns=["p", "t", "o"],
            output_columns=["p_pct", "t_pct", "o_pct"],
            expected_data={"p_pct": [None], "t_pct": [None], "o_pct": [None]},
        ),
        PercentageShareHorizontalTestCase(
            id="output_columns_are_explicit_pairs_not_derived_from_input_names",
            input_data={"a": [1], "b": [3]},
            columns=["a", "b"],
            output_columns=["z_share", "y_share"],
            expected_data={"z_share": [0.25], "y_share": [0.75]},
        ),
    ]


FOLD_SEED = 42


@dataclass
class AssignLocationFoldsTestCase:
    id: str
    location_ids: list[str]
    n_folds: int

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


@dataclass
class AddNeverSubmittedFlagTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


@dataclass
class MeanPeriodToPeriodChangeTestCase:
    id: str
    input_data: dict[str, Any]
    columns: list[str]
    partition_columns: list[str]
    expected_data: dict[str, Any]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


class TestModelEvaluationUtilsData:
    location_rows_share_a_fold_test_cases = [
        AssignLocationFoldsTestCase(
            id="three_rows_per_location",
            location_ids=["loc1", "loc2", "loc3", "loc4"] * 3,
            n_folds=2,
        ),
        AssignLocationFoldsTestCase(
            id="different_numbers_of_rows_per_location",
            location_ids=["loc1"] * 4 + ["loc2"] * 3 + ["loc3", "loc4", "loc5", "loc6"],
            n_folds=5,
        ),
    ]

    folds_are_balanced_test_cases = [
        AssignLocationFoldsTestCase(
            id="locations_that_dont_divide_evenly_into_folds",
            location_ids=[f"loc{i}" for i in range(7)],
            n_folds=3,
        ),
        # loc0 has far more rows than the others, so folds balanced by rows would differ.
        AssignLocationFoldsTestCase(
            id="balanced_by_locations_not_rows",
            location_ids=["loc0"] * 20 + [f"loc{i}" for i in range(1, 10)],
            n_folds=5,
        ),
    ]

    folds_repeat_for_same_seed_test_case = AssignLocationFoldsTestCase(
        id="rows_in_a_different_order",
        location_ids=[f"loc{i}" for i in range(10)] * 2,
        n_folds=3,
    )

    existing_folds_replaced_test_case = AssignLocationFoldsTestCase(
        id="folds_from_an_earlier_run",
        location_ids=[f"loc{i}" for i in range(6)],
        n_folds=2,
    )

    never_submitted_test_cases = [
        AddNeverSubmittedFlagTestCase(
            id="one_known_value_counts_for_the_whole_location",
            input_data={
                IndCQC.location_id: ["loc1"] * 3,
                IndCQC.ascwds_filled_posts_dedup_clean: [None, 5.0, None],
            },
            expected_data={
                IndCQC.location_id: ["loc1"] * 3,
                IndCQC.ascwds_filled_posts_dedup_clean: [None, 5.0, None],
                ModelEvaluation.never_submitted: [False] * 3,
            },
        ),
        AddNeverSubmittedFlagTestCase(
            id="location_without_any_known_value_is_never_submitted",
            input_data={
                IndCQC.location_id: ["loc1", "loc2", "loc2"],
                IndCQC.ascwds_filled_posts_dedup_clean: [5.0, None, None],
            },
            expected_data={
                IndCQC.location_id: ["loc1", "loc2", "loc2"],
                IndCQC.ascwds_filled_posts_dedup_clean: [5.0, None, None],
                ModelEvaluation.never_submitted: [False, True, True],
            },
        ),
    ]

    steady_predictions_test_cases = [
        MeanPeriodToPeriodChangeTestCase(
            id="values_that_stay_the_same_every_period",
            input_data={
                IndCQC.location_id: ["loc1"] * 3,
                IndCQC.cqc_location_import_date: [
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 3, 1),
                ],
                IndCQC.estimate_filled_posts: [10.0] * 3,
                IndCQC.posts_rolling_average_model: [12.0] * 3,
            },
            columns=[
                IndCQC.estimate_filled_posts,
                IndCQC.posts_rolling_average_model,
            ],
            partition_columns=[IndCQC.location_id],
            expected_data={
                ModelEvaluation.column_name: [
                    IndCQC.estimate_filled_posts,
                    IndCQC.posts_rolling_average_model,
                ],
                ModelEvaluation.mean_period_to_period_change: [0.0, 0.0],
            },
        ),
    ]

    # Rows are in date order across partitions, so a change measured from one row to the next,
    # ignoring the partition, would jump between their different values.
    change_within_partition_test_cases = [
        MeanPeriodToPeriodChangeTestCase(
            id="not_measured_across_job_roles",
            input_data={
                IndCQC.location_id: ["loc1"] * 4,
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.care_worker,
                    PublishedJobRoleLabels.registered_nurse,
                ]
                * 2,
                IndCQC.cqc_location_import_date: [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                IndCQC.estimate_filled_posts_by_job_role: [2.0, 8.0, 2.0, 8.0],
            },
            columns=[IndCQC.estimate_filled_posts_by_job_role],
            partition_columns=[IndCQC.location_id, IndCQC.published_job_role_label],
            expected_data={
                ModelEvaluation.column_name: [IndCQC.estimate_filled_posts_by_job_role],
                ModelEvaluation.mean_period_to_period_change: [0.0],
            },
        ),
        # loc1 changes by 10 posts and loc2 doesn't change, so the mean change is 5 posts.
        MeanPeriodToPeriodChangeTestCase(
            id="not_measured_across_locations",
            input_data={
                IndCQC.location_id: ["loc1", "loc2", "loc1", "loc2"],
                IndCQC.cqc_location_import_date: [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                IndCQC.estimate_filled_posts: [20.0, 80.0, 30.0, 80.0],
            },
            columns=[IndCQC.estimate_filled_posts],
            partition_columns=[IndCQC.location_id],
            expected_data={
                ModelEvaluation.column_name: [IndCQC.estimate_filled_posts],
                ModelEvaluation.mean_period_to_period_change: [5.0],
            },
        ),
    ]
