from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest


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
