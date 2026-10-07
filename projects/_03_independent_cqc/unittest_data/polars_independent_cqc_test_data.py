from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pytest

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ShareModelColumns as ShareModel,
)
from utils.column_values.ascwds_labelled_vocab import PublishedJobRoleLabels
from utils.column_values.categorical_column_values import CareHome, PrimaryServiceType


@dataclass
class RemoveRepeatedValuesOverTimeAsGroupTestCase:
    id: str
    input_data: dict[str, Any]
    columns_to_clean: list[str]
    partition_by_columns: str | list[str]
    date_column: str
    expected_data: dict[str, Any]
    workplace_columns: list[str] | None = None

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


@dataclass
class NullColumnsWhereGroupShareTooLowTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]
    partition_by_columns: list[str] = field(default_factory=lambda: ["grp"])

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
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="one_role_changes_keeps_all_roles_for_the_workplace",
            input_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, 1, 5],
                "second_value": [2, 2, 2, 2],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            workplace_columns=["location_id", "date"],
            expected_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, 1, 5],
                "second_value": [2, 2, 2, 2],
                "first_value_dedup": [1, 1, 1, 5],
                "second_value_dedup": [2, 2, 2, 2],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="no_role_changes_nulls_all_roles_for_the_workplace",
            input_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, 1, 1],
                "second_value": [2, 2, 2, 2],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            workplace_columns=["location_id", "date"],
            expected_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, 1, 1],
                "second_value": [2, 2, 2, 2],
                "first_value_dedup": [1, 1, None, None],
                "second_value_dedup": [2, 2, None, None],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="null_to_value_counts_as_change",
            input_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [None, None, 1, None],
                "second_value": [None, None, 2, None],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            workplace_columns=["location_id", "date"],
            expected_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [None, None, 1, None],
                "second_value": [None, None, 2, None],
                "first_value_dedup": [None, None, 1, None],
                "second_value_dedup": [None, None, 2, None],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="value_to_null_counts_as_change",
            input_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, None, 1],
                "second_value": [2, 2, None, 2],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            workplace_columns=["location_id", "date"],
            expected_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, None, 1],
                "second_value": [2, 2, None, 2],
                "first_value_dedup": [1, 1, None, 1],
                "second_value_dedup": [2, 2, None, 2],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="all_null_workplace_is_unchanged_and_stays_null",
            input_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [None] * 4,
                "second_value": [None] * 4,
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            workplace_columns=["location_id", "date"],
            expected_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [None] * 4,
                "second_value": [None] * 4,
                "first_value_dedup": [None] * 4,
                "second_value_dedup": [None] * 4,
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="changes_in_one_workplace_do_not_affect_another",
            input_data={
                "location_id": ["loc_1", "loc_2", "loc_1", "loc_2"],
                "job_role": ["a"] * 4,
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, 1, 2],
                "second_value": [2, 2, 2, 2],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            workplace_columns=["location_id", "date"],
            expected_data={
                "location_id": ["loc_1", "loc_2", "loc_1", "loc_2"],
                "job_role": ["a"] * 4,
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, 1, 2],
                "second_value": [2, 2, 2, 2],
                "first_value_dedup": [1, 1, None, 2],
                "second_value_dedup": [2, 2, None, 2],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="null_location_rows_are_judged_individually",
            input_data={
                "location_id": [None, None, None, None],
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, 1, 5],
                "second_value": [2, 2, 2, 2],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            workplace_columns=["location_id", "date"],
            expected_data={
                "location_id": [None, None, None, None],
                "job_role": ["a", "b", "a", "b"],
                "date": [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                ],
                "first_value": [1, 1, 1, 5],
                "second_value": [2, 2, 2, 2],
                "first_value_dedup": [1, 1, None, 5],
                "second_value_dedup": [2, 2, None, 2],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="first_snapshot_is_kept",
            input_data={
                "location_id": ["loc_1"] * 2,
                "job_role": ["a", "b"],
                "date": [date(2024, 1, 1), date(2024, 1, 1)],
                "first_value": [1, 3],
                "second_value": [2, 4],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            workplace_columns=["location_id", "date"],
            expected_data={
                "location_id": ["loc_1"] * 2,
                "job_role": ["a", "b"],
                "date": [date(2024, 1, 1), date(2024, 1, 1)],
                "first_value": [1, 3],
                "second_value": [2, 4],
                "first_value_dedup": [1, 3],
                "second_value_dedup": [2, 4],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="string_partition_column_nulls_repeat_at_workplace_level",
            input_data={
                "location_id": ["loc_1"] * 2,
                "job_role": ["a"] * 2,
                "date": [date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [1, 1],
                "second_value": [2, 2],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns="location_id",
            date_column="date",
            workplace_columns=["location_id", "date"],
            expected_data={
                "location_id": ["loc_1"] * 2,
                "job_role": ["a"] * 2,
                "date": [date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [1, 1],
                "second_value": [2, 2],
                "first_value_dedup": [1, None],
                "second_value_dedup": [2, None],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="unsorted_rows_are_ordered_by_date_within_timeline",
            input_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "a", "b", "b"],
                "date": [
                    date(2024, 2, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 1, 1),
                ],
                "first_value": [1, 1, 7, 7],
                "second_value": [2, 2, 8, 8],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            workplace_columns=["location_id", "date"],
            expected_data={
                "location_id": ["loc_1"] * 4,
                "job_role": ["a", "a", "b", "b"],
                "date": [
                    date(2024, 2, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 1, 1),
                ],
                "first_value": [1, 1, 7, 7],
                "second_value": [2, 2, 8, 8],
                "first_value_dedup": [None, 1, None, 7],
                "second_value_dedup": [None, 2, None, 8],
            },
        ),
        RemoveRepeatedValuesOverTimeAsGroupTestCase(
            id="role_present_in_one_snapshot_only_is_judged_on_its_own_timeline",
            input_data={
                "location_id": ["loc_1"] * 3,
                "job_role": ["a", "b", "a"],
                "date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [1, 4, 1],
                "second_value": [2, 5, 2],
            },
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            workplace_columns=["location_id", "date"],
            expected_data={
                "location_id": ["loc_1"] * 3,
                "job_role": ["a", "b", "a"],
                "date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 2, 1)],
                "first_value": [1, 4, 1],
                "second_value": [2, 5, 2],
                "first_value_dedup": [1, 4, None],
                "second_value_dedup": [2, 5, None],
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

    # Groups are keyed on "grp"; totals are a + b + c, share is a + b.
    null_columns_where_group_share_too_low_test_cases = [
        NullColumnsWhereGroupShareTooLowTestCase(
            id="nulls_group_at_boundary_share",
            input_data={"grp": ["g", "g"], "a": [1, 0], "b": [0, 0], "c": [9, 10]},
            expected_data={
                "grp": ["g", "g"],
                "a": [None, None],
                "b": [None, None],
                "c": [None, None],
            },
        ),
        NullColumnsWhereGroupShareTooLowTestCase(
            id="keeps_group_with_share_above_maximum",
            input_data={"grp": ["g"], "a": [1], "b": [1], "c": [8]},
            expected_data={"grp": ["g"], "a": [1], "b": [1], "c": [8]},
        ),
        NullColumnsWhereGroupShareTooLowTestCase(
            id="nulls_small_group_with_zero_share",
            input_data={"grp": ["g"], "a": [0], "b": [0], "c": [9]},
            expected_data={"grp": ["g"], "a": [None], "b": [None], "c": [None]},
        ),
        NullColumnsWhereGroupShareTooLowTestCase(
            id="sums_across_all_rows_in_the_group",
            input_data={"grp": ["g", "g"], "a": [1, 0], "b": [0, 0], "c": [4, 15]},
            expected_data={
                "grp": ["g", "g"],
                "a": [None, None],
                "b": [None, None],
                "c": [None, None],
            },
        ),
        NullColumnsWhereGroupShareTooLowTestCase(
            id="only_nulls_the_flagged_group",
            input_data={"grp": ["low", "ok"], "a": [0, 5], "b": [0, 5], "c": [10, 0]},
            expected_data={
                "grp": ["low", "ok"],
                "a": [None, 5],
                "b": [None, 5],
                "c": [None, 0],
            },
        ),
        NullColumnsWhereGroupShareTooLowTestCase(
            id="groups_by_every_partition_column",
            partition_by_columns=["grp", "period"],
            input_data={
                "grp": ["g", "g"],
                "period": ["p1", "p2"],
                "a": [0, 15],
                "b": [0, 0],
                "c": [20, 5],
            },
            expected_data={
                "grp": ["g", "g"],
                "period": ["p1", "p2"],
                "a": [None, 15],
                "b": [None, 0],
                "c": [None, 5],
            },
        ),
        NullColumnsWhereGroupShareTooLowTestCase(
            id="null_row_drops_out_of_group_total",
            input_data={
                "grp": ["g", "g"],
                "a": [10, None],
                "b": [0, None],
                "c": [0, None],
            },
            expected_data={
                "grp": ["g", "g"],
                "a": [10, None],
                "b": [0, None],
                "c": [0, None],
            },
        ),
        NullColumnsWhereGroupShareTooLowTestCase(
            id="does_not_pool_null_partition_keys_into_one_group",
            input_data={"grp": [None, None], "a": [0, 0], "b": [0, 0], "c": [20, 20]},
            expected_data={
                "grp": [None, None],
                "a": [0, 0],
                "b": [0, 0],
                "c": [20, 20],
            },
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


@dataclass
class AggregateTotalsByGroupTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


@dataclass
class ScoreGroupTotalsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]
    by_columns: list[str] | None = None

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


@dataclass
class AddFinancialYearTestCase:
    id: str
    dates: list[date]
    expected_years: list[int]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


@dataclass
class ScoreRowsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]
    by_columns: list[str] | None = None

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


@dataclass
class CalculatePeriodBiasTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


@dataclass
class FitBiasSlopePerYearTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


@dataclass
class WeightedBiasSlopeTestCase:
    id: str
    dates: list[date]
    biases: list[float]
    known_totals: list[float]

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
        # loc0 has most of the rows, so balancing by rows would differ.
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

    # Rows alternate between partitions, so changes across them would show as jumps.
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
        # loc1 changes by 10 and loc2 by 0, so the mean change is 5.
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

    # In date order the changes are 10 and 10; in row order they'd be 20 and 10.
    unordered_dates_test_case = MeanPeriodToPeriodChangeTestCase(
        id="rows_out_of_date_order",
        input_data={
            IndCQC.location_id: ["loc1"] * 3,
            IndCQC.cqc_location_import_date: [
                date(2024, 3, 1),
                date(2024, 1, 1),
                date(2024, 2, 1),
            ],
            IndCQC.estimate_filled_posts: [30.0, 10.0, 20.0],
        },
        columns=[IndCQC.estimate_filled_posts],
        partition_columns=[IndCQC.location_id],
        expected_data={
            ModelEvaluation.column_name: [IndCQC.estimate_filled_posts],
            ModelEvaluation.mean_period_to_period_change: [10.0],
        },
    )

    group_totals_sum_predicted_and_known_posts_test_cases = [
        AggregateTotalsByGroupTestCase(
            id="two_rows_in_a_group_are_summed",
            input_data={
                IndCQC.primary_service_type: [
                    PrimaryServiceType.non_residential,
                    PrimaryServiceType.non_residential,
                    PrimaryServiceType.care_home_only,
                ],
                IndCQC.estimate_filled_posts: [10.0, 20.0, 5.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [12.0, 18.0, 6.0],
            },
            expected_data={
                IndCQC.primary_service_type: [
                    PrimaryServiceType.non_residential,
                    PrimaryServiceType.care_home_only,
                ],
                IndCQC.estimate_filled_posts: [30.0, 5.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [30.0, 6.0],
                ModelEvaluation.number_of_rows: [2, 1],
            },
        ),
    ]

    rows_without_known_posts_excluded_from_totals_test_cases = [
        AggregateTotalsByGroupTestCase(
            id="row_missing_a_known_value_left_out_of_its_group_totals",
            input_data={
                IndCQC.primary_service_type: [
                    PrimaryServiceType.non_residential,
                    PrimaryServiceType.non_residential,
                ],
                IndCQC.estimate_filled_posts: [10.0, 20.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [12.0, None],
            },
            expected_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.estimate_filled_posts: [10.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [12.0],
                ModelEvaluation.number_of_rows: [1],
            },
        ),
    ]

    perfect_totals_test_cases = [
        ScoreGroupTotalsTestCase(
            id="every_group_total_predicted_exactly",
            input_data={
                IndCQC.estimate_filled_posts: [10.0, 20.0, 30.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [10.0, 20.0, 30.0],
            },
            expected_data={
                IndCQC.r2: [1.0],
                ModelEvaluation.weighted_absolute_percentage_error: [0.0],
            },
        ),
    ]

    # Fold 0 is predicted exactly; fold 1 misses every group by 10, a fifth of its total.
    total_scores_split_by_fold_test_cases = [
        ScoreGroupTotalsTestCase(
            id="each_fold_scored_on_its_own_groups",
            input_data={
                ModelEvaluation.fold: [0, 0, 1, 1],
                IndCQC.estimate_filled_posts: [10.0, 30.0, 20.0, 20.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [10.0, 30.0, 10.0, 30.0],
            },
            expected_data={
                ModelEvaluation.fold: [0, 1],
                IndCQC.r2: [1.0, 0.0],
                ModelEvaluation.weighted_absolute_percentage_error: [0.0, 0.5],
            },
            by_columns=[ModelEvaluation.fold],
        ),
    ]

    financial_year_test_cases = [
        AddFinancialYearTestCase(
            id="april_starts_the_financial_year",
            dates=[date(2024, 3, 31), date(2024, 4, 1)],
            expected_years=[2023, 2024],
        ),
        AddFinancialYearTestCase(
            id="january_to_march_belong_to_the_year_before",
            dates=[date(2023, 12, 1), date(2024, 1, 1), date(2024, 3, 1)],
            expected_years=[2023, 2023, 2023],
        ),
    ]

    # Errors are 10, 25, 30 and 0 against known 100, 200, 300 and 400 (total 1000).
    # R² is 1 - 1625 / 50000 and RMSE is the square root of 1625 / 4.
    score_rows_test_cases = [
        ScoreRowsTestCase(
            id="within_ten_and_twenty_five_include_the_limit",
            input_data={
                IndCQC.estimate_filled_posts: [110.0, 225.0, 330.0, 400.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [100.0, 200.0, 300.0, 400.0],
            },
            expected_data={
                ModelEvaluation.number_of_rows: [4],
                ModelEvaluation.bias: [0.065],
                ModelEvaluation.weighted_absolute_percentage_error: [0.065],
                IndCQC.r2: [0.9675],
                IndCQC.rmse: [20.155644370746373],
                IndCQC.proportion_of_model_predictions_within_ten: [0.5],
                IndCQC.proportion_of_model_predictions_within_twenty_five: [0.75],
            },
        ),
        # Over- and under-prediction cancel in bias but not in the weighted error.
        ScoreRowsTestCase(
            id="over_and_under_prediction_cancel_in_bias_only",
            input_data={
                IndCQC.estimate_filled_posts: [20.0, 20.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [10.0, 30.0],
            },
            expected_data={
                ModelEvaluation.number_of_rows: [2],
                ModelEvaluation.bias: [0.0],
                ModelEvaluation.weighted_absolute_percentage_error: [0.5],
                IndCQC.r2: [0.0],
                IndCQC.rmse: [10.0],
                IndCQC.proportion_of_model_predictions_within_ten: [1.0],
                IndCQC.proportion_of_model_predictions_within_twenty_five: [1.0],
            },
        ),
    ]

    rows_without_known_posts_not_scored_test_cases = [
        ScoreRowsTestCase(
            id="row_missing_a_known_value_left_out_of_every_score",
            input_data={
                IndCQC.estimate_filled_posts: [20.0, 20.0, 999.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [10.0, 30.0, None],
            },
            expected_data={
                ModelEvaluation.number_of_rows: [2],
                ModelEvaluation.bias: [0.0],
                ModelEvaluation.weighted_absolute_percentage_error: [0.5],
                IndCQC.r2: [0.0],
                IndCQC.rmse: [10.0],
                IndCQC.proportion_of_model_predictions_within_ten: [1.0],
                IndCQC.proportion_of_model_predictions_within_twenty_five: [1.0],
            },
        ),
    ]

    # Fold 0 is predicted exactly; fold 1 is 10 out in each row, in opposite directions.
    scores_split_by_fold_test_cases = [
        ScoreRowsTestCase(
            id="each_fold_scored_on_its_own_rows",
            input_data={
                ModelEvaluation.fold: [0, 0, 1, 1],
                IndCQC.estimate_filled_posts: [10.0, 30.0, 20.0, 20.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [10.0, 30.0, 10.0, 30.0],
            },
            expected_data={
                ModelEvaluation.fold: [0, 1],
                ModelEvaluation.number_of_rows: [2, 2],
                ModelEvaluation.bias: [0.0, 0.0],
                ModelEvaluation.weighted_absolute_percentage_error: [0.0, 0.5],
                IndCQC.r2: [1.0, 0.0],
                IndCQC.rmse: [0.0, 10.0],
                IndCQC.proportion_of_model_predictions_within_ten: [1.0, 1.0],
                IndCQC.proportion_of_model_predictions_within_twenty_five: [1.0, 1.0],
            },
            by_columns=[ModelEvaluation.fold],
        ),
    ]

    period_bias_test_cases = [
        # January: (110 + 95 - 200) / 200. February: (80 - 100) / 100.
        CalculatePeriodBiasTestCase(
            id="bias_relative_to_the_known_total_of_each_period",
            input_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 3,
                IndCQC.cqc_location_import_date: [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                ],
                IndCQC.estimate_filled_posts: [110.0, 95.0, 80.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [100.0, 100.0, 100.0],
            },
            expected_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 2,
                IndCQC.cqc_location_import_date: [date(2024, 1, 1), date(2024, 2, 1)],
                IndCQC.estimate_filled_posts: [205.0, 80.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [200.0, 100.0],
                ModelEvaluation.number_of_rows: [2, 1],
                ModelEvaluation.bias: [0.025, -0.2],
            },
        ),
        CalculatePeriodBiasTestCase(
            id="periods_measured_separately_for_each_group",
            input_data={
                IndCQC.primary_service_type: [
                    PrimaryServiceType.non_residential,
                    PrimaryServiceType.care_home_only,
                ],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)] * 2,
                IndCQC.estimate_filled_posts: [150.0, 50.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [100.0, 100.0],
            },
            expected_data={
                IndCQC.primary_service_type: [
                    PrimaryServiceType.non_residential,
                    PrimaryServiceType.care_home_only,
                ],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)] * 2,
                IndCQC.estimate_filled_posts: [150.0, 50.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [100.0, 100.0],
                ModelEvaluation.number_of_rows: [1, 1],
                ModelEvaluation.bias: [0.5, -0.5],
            },
        ),
        CalculatePeriodBiasTestCase(
            id="row_missing_a_known_value_left_out_of_both_totals",
            input_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 2,
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)] * 2,
                IndCQC.estimate_filled_posts: [110.0, 999.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [100.0, None],
            },
            expected_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                IndCQC.estimate_filled_posts: [110.0],
                IndCQC.ascwds_filled_posts_dedup_clean: [100.0],
                ModelEvaluation.number_of_rows: [1],
                ModelEvaluation.bias: [0.1],
            },
        ),
    ]

    # Bias is 0.2 at the first date and falls by 0.05 for every 365.25 days after it.
    bias_slope_test_cases = [
        FitBiasSlopePerYearTestCase(
            id="bias_that_falls_every_year_has_a_negative_slope",
            input_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 3,
                IndCQC.cqc_location_import_date: [
                    date(2020, 1, 1),
                    date(2022, 1, 1),
                    date(2024, 1, 1),
                ],
                ModelEvaluation.bias: [
                    0.2,
                    0.2 - 0.05 * (date(2022, 1, 1) - date(2020, 1, 1)).days / 365.25,
                    0.2 - 0.05 * (date(2024, 1, 1) - date(2020, 1, 1)).days / 365.25,
                ],
                IndCQC.ascwds_filled_posts_dedup_clean: [100.0, 300.0, 200.0],
                ModelEvaluation.number_of_rows: [5, 9, 7],
            },
            expected_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                ModelEvaluation.bias_slope_per_year: [-0.05],
                ModelEvaluation.number_of_periods: [3],
                ModelEvaluation.minimum_rows_in_period: [5],
            },
        ),
        FitBiasSlopePerYearTestCase(
            id="steady_bias_has_no_slope",
            input_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 3,
                IndCQC.cqc_location_import_date: [
                    date(2022, 1, 1),
                    date(2023, 1, 1),
                    date(2024, 1, 1),
                ],
                ModelEvaluation.bias: [0.1] * 3,
                IndCQC.ascwds_filled_posts_dedup_clean: [100.0, 100.0, 100.0],
                ModelEvaluation.number_of_rows: [2, 2, 2],
            },
            expected_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                ModelEvaluation.bias_slope_per_year: [0.0],
                ModelEvaluation.number_of_periods: [3],
                ModelEvaluation.minimum_rows_in_period: [2],
            },
        ),
        FitBiasSlopePerYearTestCase(
            id="each_group_gets_its_own_slope_and_counts",
            input_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 2
                + [PrimaryServiceType.care_home_only] * 3,
                IndCQC.cqc_location_import_date: [
                    date(2022, 1, 1),
                    date(2023, 1, 1),
                    date(2022, 1, 1),
                    date(2023, 1, 1),
                    date(2024, 1, 1),
                ],
                ModelEvaluation.bias: [0.1, 0.1, 0.2, 0.2, 0.2],
                IndCQC.ascwds_filled_posts_dedup_clean: [100.0] * 5,
                ModelEvaluation.number_of_rows: [4, 6, 3, 8, 9],
            },
            expected_data={
                IndCQC.primary_service_type: [
                    PrimaryServiceType.non_residential,
                    PrimaryServiceType.care_home_only,
                ],
                ModelEvaluation.bias_slope_per_year: [0.0, 0.0],
                ModelEvaluation.number_of_periods: [2, 3],
                ModelEvaluation.minimum_rows_in_period: [4, 3],
            },
        ),
    ]

    # The first period has little known data and an extreme bias.
    weighted_bias_slope_test_cases = [
        WeightedBiasSlopeTestCase(
            id="thin_early_periods_count_for_less",
            dates=[
                date(2020, 1, 1),
                date(2021, 1, 1),
                date(2022, 1, 1),
                date(2023, 1, 1),
            ],
            biases=[0.9, 0.1, 0.12, 0.1],
            known_totals=[10.0, 1000.0, 1200.0, 1100.0],
        ),
        WeightedBiasSlopeTestCase(
            id="equal_periods_count_equally",
            dates=[date(2020, 1, 1), date(2021, 1, 1), date(2022, 1, 1)],
            biases=[0.3, 0.1, -0.1],
            known_totals=[500.0, 500.0, 500.0],
        ),
    ]


ACTUAL_SHARES = ["actual_1", "actual_2"]
PREDICTED_SHARES = ["predicted_1", "predicted_2"]
NON_RES = PrimaryServiceType.non_residential
CARE_HOME = PrimaryServiceType.care_home_only


@dataclass
class ModelMetricsUtilsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


@dataclass
class ModelUtilsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


class TestModelMetricsUtilsData:
    group_shares_are_worker_weighted_test_cases = [
        # Unweighted, non-res would be 0.4 for both.
        ModelMetricsUtilsTestCase(
            id="weights_each_row_by_its_workers",
            input_data={
                IndCQC.primary_service_type: [NON_RES, NON_RES, CARE_HOME],
                PREDICTED_SHARES[0]: [0.3, 0.5, 0.7],
                ACTUAL_SHARES[0]: [0.2, 0.6, 0.8],
                IndCQC.estimate_filled_posts_by_job_role: [3.0, 1.0, 2.0],
            },
            expected_data={
                IndCQC.primary_service_type: [NON_RES, CARE_HOME],
                PREDICTED_SHARES[0]: [0.35, 0.7],
                ACTUAL_SHARES[0]: [0.3, 0.8],
                ShareModel.group_weight: [4.0, 2.0],
            },
        ),
    ]

    unknown_rows_excluded_from_groups_test_cases = [
        ModelMetricsUtilsTestCase(
            id="unknown_row_left_out_of_its_groups_shares_and_weight",
            input_data={
                IndCQC.primary_service_type: [NON_RES, NON_RES],
                PREDICTED_SHARES[0]: [0.5, 0.9],
                ACTUAL_SHARES[0]: [0.4, None],
                IndCQC.estimate_filled_posts_by_job_role: [2.0, 8.0],
            },
            expected_data={
                IndCQC.primary_service_type: [NON_RES],
                PREDICTED_SHARES[0]: [0.5],
                ACTUAL_SHARES[0]: [0.4],
                ShareModel.group_weight: [2.0],
            },
        ),
        # Keeping the row's workers without its prediction would give 0.1.
        ModelMetricsUtilsTestCase(
            id="row_without_a_prediction_left_out_of_its_groups_shares_and_weight",
            input_data={
                IndCQC.primary_service_type: [NON_RES, NON_RES],
                PREDICTED_SHARES[0]: [0.5, None],
                ACTUAL_SHARES[0]: [0.4, 0.9],
                IndCQC.estimate_filled_posts_by_job_role: [2.0, 8.0],
            },
            expected_data={
                IndCQC.primary_service_type: [NON_RES],
                PREDICTED_SHARES[0]: [0.5],
                ACTUAL_SHARES[0]: [0.4],
                ShareModel.group_weight: [2.0],
            },
        ),
        ModelMetricsUtilsTestCase(
            id="group_without_any_known_rows_is_left_out",
            input_data={
                IndCQC.primary_service_type: [NON_RES, CARE_HOME],
                PREDICTED_SHARES[0]: [0.5, 0.9],
                ACTUAL_SHARES[0]: [0.4, None],
                IndCQC.estimate_filled_posts_by_job_role: [2.0, 8.0],
            },
            expected_data={
                IndCQC.primary_service_type: [NON_RES],
                PREDICTED_SHARES[0]: [0.5],
                ACTUAL_SHARES[0]: [0.4],
                ShareModel.group_weight: [2.0],
            },
        ),
    ]

    perfect_predictions_test_cases = [
        ModelMetricsUtilsTestCase(
            id="every_share_predicted_exactly",
            input_data={
                PREDICTED_SHARES[0]: [0.2, 0.5, 0.7],
                PREDICTED_SHARES[1]: [0.3, 0.1, 0.2],
                ACTUAL_SHARES[0]: [0.2, 0.5, 0.7],
                ACTUAL_SHARES[1]: [0.3, 0.1, 0.2],
                ShareModel.group_weight: [1.0, 2.0, 3.0],
            },
            expected_data={
                ShareModel.share: ACTUAL_SHARES,
                IndCQC.r2: [1.0, 1.0],
                ShareModel.mean_absolute_error: [0.0, 0.0],
            },
        ),
    ]

    # One exact group and one 20 point miss: unweighted, that's R² 0.5 and error 10.
    larger_groups_weigh_more_test_cases = [
        ModelMetricsUtilsTestCase(
            id="miss_in_the_smaller_group_counts_for_less",
            input_data={
                PREDICTED_SHARES[0]: [0.5, 0.7],
                ACTUAL_SHARES[0]: [0.5, 0.9],
                ShareModel.group_weight: [3.0, 1.0],
            },
            expected_data={
                ShareModel.share: ACTUAL_SHARES[:1],
                IndCQC.r2: [2 / 3],
                ShareModel.mean_absolute_error: [5.0],
            },
        ),
        ModelMetricsUtilsTestCase(
            id="miss_in_the_larger_group_counts_for_more",
            input_data={
                PREDICTED_SHARES[0]: [0.5, 0.7],
                ACTUAL_SHARES[0]: [0.5, 0.9],
                ShareModel.group_weight: [1.0, 3.0],
            },
            expected_data={
                ShareModel.share: ACTUAL_SHARES[:1],
                IndCQC.r2: [0.0],
                ShareModel.mean_absolute_error: [15.0],
            },
        ),
    ]

    # Keeping the last group's weight without its error would give R² 0.94 and error 5.
    groups_missing_a_share_test_cases = [
        ModelMetricsUtilsTestCase(
            id="group_without_a_prediction_left_out",
            input_data={
                PREDICTED_SHARES[0]: [0.3, 0.5, None],
                ACTUAL_SHARES[0]: [0.2, 0.6, 0.9],
                ShareModel.group_weight: [1.0, 1.0, 2.0],
            },
            expected_data={
                ShareModel.share: ACTUAL_SHARES[:1],
                IndCQC.r2: [0.75],
                ShareModel.mean_absolute_error: [10.0],
            },
        ),
    ]

    scores_split_by_fold_test_cases = [
        ModelMetricsUtilsTestCase(
            id="each_fold_scored_on_its_own_groups",
            input_data={
                ModelEvaluation.fold: [0, 0, 1, 1],
                PREDICTED_SHARES[0]: [0.2, 0.6, 0.3, 0.5],
                ACTUAL_SHARES[0]: [0.2, 0.6, 0.2, 0.6],
                ShareModel.group_weight: [1.0, 1.0, 1.0, 1.0],
            },
            expected_data={
                ModelEvaluation.fold: [0, 1],
                ShareModel.share: ACTUAL_SHARES[:1] * 2,
                IndCQC.r2: [1.0, 0.75],
                ShareModel.mean_absolute_error: [0.0, 10.0],
            },
        ),
    ]


class TestModelUtilsData:
    add_date_index_test_cases = [
        ModelUtilsTestCase(
            id="repeated_dates_share_an_index",
            input_data={
                IndCQC.location_id: ["loc1", "loc1", "loc1"],
                IndCQC.cqc_location_import_date: [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                ],
            },
            expected_data={
                IndCQC.location_id: ["loc1", "loc1", "loc1"],
                IndCQC.cqc_location_import_date: [
                    date(2024, 1, 1),
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                ],
                IndCQC.cqc_location_import_date_indexed: [1, 1, 2],
            },
        ),
        ModelUtilsTestCase(
            id="index_is_partitioned_by_location",
            input_data={
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.cqc_location_import_date: [date(2024, 3, 1), date(2024, 1, 1)],
            },
            expected_data={
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.cqc_location_import_date: [date(2024, 3, 1), date(2024, 1, 1)],
                IndCQC.cqc_location_import_date_indexed: [1, 1],
            },
        ),
    ]


@dataclass
class ModelInterpolationTestCase:
    id: str
    data: list[Any]
    method: str = None
    max_days_between_submissions: int = None


@dataclass
class InterpolationData:
    interpolation_test_cases = [
        ModelInterpolationTestCase(
            id="trend_interpolation_single_gap",
            data=[
                ("1-001", date(2023, 3, 1), 40.0, None, None),
                ("1-001", date(2023, 6, 1), None, 46.0, 34.185792),
                ("1-001", date(2024, 3, 1), 5.0, 52.0, None),
            ],
            method="trend",
        ),
        ModelInterpolationTestCase(
            id="trend_interpolation_not_possible_with_single_point",
            data=[
                ("1-001", date(2023, 1, 1), None, None, None),
                ("1-001", date(2023, 2, 1), 10.0, None, None),
                ("1-001", date(2023, 3, 1), None, None, None),
            ],
            method="trend",
        ),
        ModelInterpolationTestCase(
            id="straight_interpolation_single_gap",
            data=[
                ("1-001", date(2023, 3, 1), 40.0, None, None),
                ("1-001", date(2023, 4, 1), None, 42.0, 37.035519),
                ("1-001", date(2024, 3, 1), 5.0, 52.0, None),
            ],
            method="straight",
        ),
        ModelInterpolationTestCase(
            id="straight_interpolation_not_possible_with_single_point",
            data=[
                ("1-001", date(2023, 1, 1), None, None, None),
                ("1-001", date(2023, 2, 1), 10.0, None, None),
                ("1-001", date(2023, 3, 1), None, None, None),
            ],
            method="straight",
        ),
    ]  # fmt: skip

    calculate_residual_test_cases = [
        ModelInterpolationTestCase(
            id="extrapolation_forwards_is_known_rows",
            data=[
                ("1-001", date(2023, 5, 1), None, 44.0, -47.0),
                ("1-001", date(2023, 4, 1), None, 42.0, -47.0),
                ("1-001", date(2024, 3, 1), 5.0, 52.0, -47.0),
                ("1-001", date(2024, 4, 1), None, 5.1, 9.8),
                ("1-001", date(2024, 5, 1), 15.0, 5.2, 9.8),
            ],
        ),
        ModelInterpolationTestCase(
            id="extrapolation_forwards_is_none_rows",
            data=[
                ("1-001", date(2023, 1, 1), None, None, None),
                ("1-001", date(2023, 3, 1), 40.0, None, None),
            ],
        ),
        ModelInterpolationTestCase(
            id="returns_none_date_after_final_non_null_submission_rows",
            data=[
                ("1-001", date(2024, 5, 1), 15.0, 5.2, 9.8),
                ("1-001", date(2024, 6, 1), None, 15.3, None),
            ],
        ),
    ]  # fmt: skip
    calculate_days_between_submissions_test_cases = [
        ModelInterpolationTestCase(
            id="one_location_data",
            data=[
                ("1-001", date(2024, 2, 1), None, None, None),
                ("1-001", date(2024, 3, 1), 5.0, None, None),
                ("1-001", date(2024, 4, 1), None, 122, 0.25),
                ("1-001", date(2024, 5, 1), None, 122, 0.5),
                ("1-001", date(2024, 6, 1), None, 122, 0.75),
                ("1-001", date(2024, 7, 1), 15.0, None, None),
                ("1-001", date(2023, 8, 1), None, None, None),
            ],
        ),
        ModelInterpolationTestCase(
            id="multiple_locations_single_submission_each",
            data=[
                ("1-001", date(2024, 1, 1), 5.0, None, None),
                ("1-001", date(2024, 2, 1), None, None, None),
                ("1-002", date(2024, 1, 5), 3.0, None, None),
                ("1-002", date(2024, 2, 5), None, None, None),
                ("1-003", date(2024, 3, 1), 8.0, None, None),
                ("1-003", date(2024, 4, 1), None, None, None),
            ],
        ),
        ModelInterpolationTestCase(
            id="two_locations_interleaved",
            data=[
                ("1-001", date(2024, 1, 1), 10.0, None, None),
                ("1-002", date(2024, 1, 2), 10.0, None, None),
                ("1-001", date(2024, 2, 1), None, 60, 0.51),
                ("1-002", date(2024, 3, 2), None, 90, 0.66),
                ("1-001", date(2024, 3, 1), 7.0, None, None),
                ("1-002", date(2024, 4, 1), 7.0, None, None),
            ],
        ),
    ]  # fmt: skip
    calculate_interpolated_values_test_cases = [
        ModelInterpolationTestCase(
            id="test_function_returns_expected_values_when_within_max_days",
            data=[
                ("1-001", date(2025, 1, 3), 20.0, None, None, None, None, None),
                ("1-001", date(2025, 1, 4), None, 20.0, 10.0, 4, 0.25, 22.5),
                ("1-001", date(2025, 1, 5), None, 20.0, 10.0, 4, 0.5, 25.0),
                ("1-001", date(2025, 1, 6), None, 20.0, 10.0, 4, 0.75, 27.5),
                ("1-001", date(2025, 1, 7), 30.0, 20.0, 10.0, None, None, None),
                ("1-001", date(2025, 1, 8), None, None, None, None, None, None),
            ],
            max_days_between_submissions=4,
        ),
        ModelInterpolationTestCase(
            id="test_function_returns_expected_values_when_outside_max_days",
            data=[
                ("1-001", date(2025, 1, 3), 20.0, None, None, None, None, None),
                ("1-001", date(2025, 1, 4), None, 20.0, 10.0, 3, 0.25, None),
                ("1-001", date(2025, 1, 5), None, 20.0, 10.0, 3, 0.5, None),
                ("1-001", date(2025, 1, 6), None, 20.0, 10.0, 3, 0.75, None),
                ("1-001", date(2025, 1, 7), 30.0, 20.0, 10.0, None, None, None),
                ("1-001", date(2025, 1, 8), None, None, None, None, None, None),
            ],
            max_days_between_submissions=2,
        ),
    ]  # fmt: skip


@dataclass
class ExtrapolationTestCase:
    id: str
    data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.data, id=self.id)


@dataclass
class ModelExtrapolation:
    extrapolation_when_nominal_test_cases = [
        ExtrapolationTestCase(
            id="when_one_later_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 20.0, 100.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, -60.0, -60.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_later_data_points_are_missing",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, 20.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 90.0, 90.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_earlier_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_earlier_data_points_are_missing",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_intermediate_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 15.0, 10.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, 25.0, None),
                ("1-001", date(2026, 3, 1), 30.0, 30.0, 35.0, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_only_one_observed_value_exists",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 30.0, 20.0, 20.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_model_extrapolation_only_applies_after_last_submission",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, None),
                ("1-001", date(2026, 3, 1), 50.0, 40.0, 30.0, None),
                ("1-001", date(2026, 4, 1), None, 50.0, 60.0, 60.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_gaps_exist_across_full_time_series",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 30.0, 20.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-001", date(2026, 4, 1), 20.0, 80.0, 70.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-001", date(2026, 5, 1), None, 100.0, 40.0, 40.0), # Forwards extrapolation from previously known value (20.0)
            ],
        ),
        ExtrapolationTestCase(
            id="when_more_than_one_location_needs_extrapolating",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, 20.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 90.0, 90.0),
                ("1-002", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-002", date(2026, 2, 1), None, 10.0, None, 0.0),
                ("1-002", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_no_data_points_are_available",
            data=[
                ("1-001", date(2026, 3, 1), None, None, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_dates_are_not_sorted_within_group",
            data=[
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, 20.0),
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 100.0, 90.0, 90.0),
            ],
)
    ] # fmt: skip
    extrapolation_when_ratio_test_cases = [
        ExtrapolationTestCase(
            id="when_one_later_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 20.0, 100.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, 4.0, 4.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_later_data_points_are_missing",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, 15.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 50.0, 50.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_earlier_data_point_is_missing",
            data=[
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_earlier_data_points_are_missing",
            data=[
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_intermediate_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 15.0, 10.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, 30.0, None),
                ("1-001", date(2026, 3, 1), 30.0, 30.0, 45.0, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_only_one_observed_value_exists",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 30.0, 15.0, 15.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_model_extrapolation_only_applies_after_last_submission",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, None),
                ("1-001", date(2026, 3, 1), 50.0, 40.0, 20.0, None),
                ("1-001", date(2026, 4, 1), None, 20.0, 25.0, 25.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_gaps_exist_across_full_time_series",
            data=[
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-002", date(2026, 3, 1), None, 30.0, 15.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-002", date(2026, 4, 1), 20.0, 80.0, 40.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-002", date(2026, 5, 1), None, 100.0, 25.0, 25.0), # Forwards extrapolation from previously known value (20.0)
            ],
        ),
        ExtrapolationTestCase(
            id="when_more_than_one_location_needs_extrapolating",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, 15.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 50.0, 50.0),
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_no_data_points_are_available",
            data=[
                ("1-003", date(2026, 3, 1), None, None, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_dates_are_not_sorted_within_group",
            data=[
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, 15.0),
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 100.0, 50.0, 50.0),
            ],
        ),
    ] # fmt: skip
    expected_extrapolation_when_error_rows = [
        ("1-001", date(2026, 1, 1), None, 10.0, None, None),
    ]

    expected_extrapolation_aggregates_rows = [
        ("1-001", date(2026, 1, 1), None, 10.0, date(2026, 2, 1), date(2026, 3, 1), 10.0, 20.0),
        ("1-001", date(2026, 2, 1), 10.0, 20.0, date(2026, 2, 1), date(2026, 3, 1), 10.0, 20.0),
        ("1-001", date(2026, 3, 1), 20.0, 30.0, date(2026, 2, 1), date(2026, 3, 1), 10.0, 20.0),
        ("1-002", date(2026, 1, 1), 15.0, 40.0, date(2026, 1, 1), date(2026, 1, 1), 15.0, 40.0),
        ("1-002", date(2026, 2, 1), None, 50.0, date(2026, 1, 1), date(2026, 1, 1), 15.0, 40.0),
        ("1-003", date(2026, 1, 1), None, None, None, None, None, None),
    ] # fmt: skip

    get_previous_value_test_cases = [
        ExtrapolationTestCase(
            id="when_values_are_sequential_no_nulls",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 2, 1), 20.0, 10.0),
                ("1-001", date(2026, 3, 1), 30.0, 20.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_values_have_null_gap",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 2, 1), None, 10.0),
                ("1-001", date(2026, 3, 1), None, 10.0),
                ("1-001", date(2026, 4, 1), 40.0, 10.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_values_have_leading_nulls",
            data=[
                ("1-001", date(2026, 1, 1), None, None),
                ("1-001", date(2026, 2, 1), None, None),
                ("1-001", date(2026, 3, 1), 30.0, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_groups_are_present",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 2, 1), None, 10.0),
                ("1-002", date(2026, 1, 1), 5.0, None),
                ("1-002", date(2026, 2, 1), None, 5.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_input_is_unsorted_within_group",
            data=[
                ("1-001", date(2026, 2, 1), None, 10.0),
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 3, 1), 30.0, 10.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_all_values_are_null",
            data=[
                ("1-001", date(2026, 1, 1), None, None),
                ("1-001", date(2026, 2, 1), None, None),
            ],
        ),
    ]


@dataclass
class ModelImputationTestCase:
    id: str
    expected_data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.expected_data, id=self.id)


@dataclass
class ModelImputation:
    column_with_null_values_name: str = "null_values"
    model_column_name: str = "trend_model"
    imputed_values_column_name: str = "imputed_values"

    expected_model_imputation_test_cases = [
        ModelImputationTestCase(
            id="when_values_will_be_extrapolated",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, None, 1.0, 19.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, 20.0, 2.0, 20.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, None, 3.0, 21.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_be_interpolated",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, 20.0, 1.0, 20.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, None, 2.0, 21.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, 22.0, 3.0, 22.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_be_extrapolated_and_interpolated",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, 20.0, 1.0, 20.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, None, 2.0, 21.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, 22.0, 3.0, 22.0),
                ("1-001", date(2023, 4, 1), CareHome.not_care_home, None, 4.0, 23.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_not_be_imputed_because_all_present",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, 30.0, 1.0, 30.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, 31.0, 2.0, 31.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, 32.0, 3.0, 32.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_not_be_imputed_because_all_null",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, None, 1.0, None),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, None, 2.0, None),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, None, 3.0, None),
            ],
        ),
        ModelImputationTestCase(
            id="when_given_multiple_service_types_only_non_res_gets_imputed_due_to_function_argument",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, None, 1.0, 19.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, 20.0, 2.0, 20.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, None, 3.0, 21.0),
                ("1-002", date(2023, 1, 1), CareHome.care_home, None, 1.0, None),
                ("1-002", date(2023, 2, 1), CareHome.care_home, 20.0, 2.0, None),
                ("1-002", date(2023, 3, 1), CareHome.care_home, None, 3.0, None),
            ],
        ),
        ModelImputationTestCase(
            id="when_location_changes_care_home_status_only_the_matching_period_is_imputed",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, 50.0, 1.0, 50.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, None, 2.0, 51.0),
                ("1-001", date(2023, 3, 1), CareHome.care_home, 50.0, 3.0, None),
                ("1-001", date(2023, 4, 1), CareHome.care_home, None, 4.0, None),
            ],
        ),
    ] # fmt: skip
