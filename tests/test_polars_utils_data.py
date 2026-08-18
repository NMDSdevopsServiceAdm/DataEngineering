import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional

import polars as pl
import polars.selectors as cs

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.data_labels_columns import DataLabelsColumns as DLC
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from utils.column_values.categorical_column_values import (
    AscwdsFilteringRule,
    CareHome,
    ContemporaryCSSR,
    EstimateFilledPostsSource,
    JobRoleFilteringRule,
)
from utils.column_values.categorical_columns_by_dataset import (
    LocationsApiCleanedCategoricalValues as CQCLocationCatVals,
)

from tests.test_polars_utils_schemas import CleaningUtilsSchemas as Schemas


@dataclass
class CleaningUtilsTestCase:
    id: str
    test_data: list[Any]
    expected_data: list[Any]
    column_names: list[str]
    add_as_new_column: Optional[bool]


@dataclass
class RemoveRepeatedValuesOverTimeTestCase:
    id: str
    test_data: list[Any]
    test_schema: pl.Schema
    columns_to_clean: list[str] | cs.Selector
    partition_by_columns: str | list[str]
    date_column: str
    expected_data: list[Any]
    expected_schema: pl.Schema


@dataclass
class CleaningUtilsData:
    align_dates_primary_rows = [
        (date(2019, 1, 1), date(2021, 1, 15), date(2020, 1, 8), date(2021, 1, 15)),
        ("1-001", "1-001", "1-001", "1-002"),
    ]

    align_dates_secondary_rows = [
        (
            date(2020, 1, 1),
            date(2021, 1, 1),
            date(2020, 1, 5),
            date(2020, 1, 1),
            date(2021, 1, 5),
            date(2022, 1, 1),
        ),
        ("123", "123", "123", "456", "789", "789"),
    ]
    expected_align_dates_primary_with_secondary_rows = [
        (date(2019, 1, 1), date(2020, 1, 8), date(2021, 1, 15), date(2021, 1, 15)),
        ("1-001", "1-001", "1-001", "1-002"),
        (None, date(2020, 1, 5), date(2021, 1, 5), date(2021, 1, 5)),
    ]

    align_dates_primary_single_row = [(date(2025, 5, 1),), ("1-001",)]

    align_dates_secondary_exact_match_rows = [(date(2025, 5, 1),), ("123",)]
    expected_align_dates_secondary_exact_match_rows = [
        (date(2025, 5, 1),),
        ("1-001",),
        (date(2025, 5, 1),),
    ]

    align_dates_secondary_closest_historical_rows = [
        (date(2024, 4, 4), date(2023, 3, 3), date(2025, 5, 5)),
        ("123", "123", "123"),
    ]
    expected_align_dates_secondary_closest_historical_rows = [
        (date(2025, 5, 1),),
        ("1-001",),
        (date(2024, 4, 4),),
    ]

    align_dates_secondary_future_rows = [(date(2025, 5, 5),), ("123",)]
    expected_align_dates_secondary_future_rows = [
        (date(2025, 5, 1),),
        ("1-001",),
        (None,),
    ]

    labels_data = [
        (AWK.gender, "1", "male"),
        (AWK.gender, "2", "female"),
        (AWK.nationality, "100", "British"),
        (AWK.nationality, "101", "French"),
        (AWK.nationality, "102", "Spanish"),
        (IndCQC.contemporary_cssr, "902", ContemporaryCSSR.cornwall_and_isles_of_scilly),
        (IndCQC.contemporary_cssr, "906", ContemporaryCSSR.cornwall_and_isles_of_scilly),
        (IndCQC.contemporary_cssr, "407", ContemporaryCSSR.coventry),
    ] # fmt: skip

    gender_labels = AWK.gender + f"_{DLC.label}s"
    gender_codes = AWK.gender + f"_{DLC.code}s"
    nationality_labels = AWK.nationality + f"_{DLC.label}s"
    nationality_codes = AWK.nationality + f"_{DLC.code}s"
    contemporary_cssr_codes = IndCQC.contemporary_cssr + f"_{DLC.code}s"

    apply_catagorical_labels_test_cases = [
        CleaningUtilsTestCase(
            id="adds_single_column_with_labels_when_single_column_name_passed_and_add_as_new_column_is_true_and_reverse_mapping_is_false",
            test_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["1", "2", None, "2"],
                AWK.nationality: ["100", "101", "102", None],
            },
            expected_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["1", "2", None, "2"],
                AWK.nationality: ["100", "101", "102", None],
                gender_labels: ["male", "female", None, "female"],
            },
            column_names=[AWK.gender],
            add_as_new_column=True,
        ),
        CleaningUtilsTestCase(
            id="adds_multiple_columns_with_labels_when_multiple_column_names_passed_and_add_as_new_column_is_true_and_reverse_mapping_is_false",
            test_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["1", "2", None, "2"],
                AWK.nationality: ["100", "101", "102", None],
            },
            expected_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["1", "2", None, "2"],
                AWK.nationality: ["100", "101", "102", None],
                gender_labels: ["male", "female", None, "female"],
                nationality_labels: ["British", "French", "Spanish", None],
            },
            column_names=[AWK.gender, AWK.nationality],
            add_as_new_column=True,
        ),
        CleaningUtilsTestCase(
            id="replaces_single_column_with_labels_when_single_column_name_passed_and_add_as_new_column_is_false_and_reverse_mapping_is_false",
            test_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["1", "2", None, "2"],
                AWK.nationality: ["100", "101", "102", None],
            },
            expected_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["male", "female", None, "female"],
                AWK.nationality: ["100", "101", "102", None],
            },
            column_names=[AWK.gender],
            add_as_new_column=False,
        ),
        CleaningUtilsTestCase(
            id="replaces_multiple_columns_with_labels_when_multiple_column_names_passed_and_add_as_new_column_is_false_and_reverse_mapping_is_false",
            test_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["1", "2", None, "2"],
                AWK.nationality: ["100", "101", "102", None],
            },
            expected_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["male", "female", None, "female"],
                AWK.nationality: ["British", "French", "Spanish", None],
            },
            column_names=[AWK.gender, AWK.nationality],
            add_as_new_column=False,
        ),
        CleaningUtilsTestCase(
            id="rows_with_values_not_in_label_dict_are_retained_when_add_as_new_column_is_true",
            test_data={
                AWK.worker_id: ["1", "2"],
                AWK.gender: ["other value", "2"],
            },
            expected_data={
                AWK.worker_id: ["1", "2"],
                AWK.gender: ["other value", "2"],
                gender_labels: ["other value", "female"],
            },
            column_names=[AWK.gender],
            add_as_new_column=True,
        ),
        CleaningUtilsTestCase(
            id="rows_with_values_not_in_label_dict_are_retained_when_add_as_new_column_is_false",
            test_data={
                AWK.worker_id: ["1", "2"],
                AWK.gender: ["other value", "2"],
            },
            expected_data={
                AWK.worker_id: ["1", "2"],
                AWK.gender: ["other value", "female"],
            },
            column_names=[AWK.gender],
            add_as_new_column=False,
        ),
    ]

    column_to_date_string_with_hyphens_rows = [
        ("2023-01-02", "2022-05-04", "2019-12-07", "1908-12-05"),
    ]
    column_to_date_string_without_hyphens_rows = [
        ("20230102", "20220504", "20191207", "19081205"),
    ]
    column_to_date_integer_without_hyphens_rows = [
        (20230102, 20220504, 20191207, 19081205),
    ]
    expected_column_to_date_rows = [
        (date(2023, 1, 2), date(2022, 5, 4), date(2019, 12, 7), date(1908, 12, 5)),
    ]

    column_to_date_with_new_col_rows = [
        ("20230102", "20220504", "20191207", "19081205"),
    ]
    expected_column_to_date_with_new_col_rows = [
        ("20230102", "20220504", "20191207", "19081205"),
        (date(2023, 1, 2), date(2022, 5, 4), date(2019, 12, 7), date(1908, 12, 5)),
    ]

    filled_posts_per_bed_ratio_rows = [
        ("1-000000001", 5.0, 100, CareHome.care_home),
        ("1-000000002", 2.0, 1, CareHome.care_home),
        ("1-000000003", None, 100, CareHome.care_home),
        ("1-000000004", 0.0, 1, CareHome.care_home),
        ("1-000000005", 5.0, None, CareHome.care_home),
        ("1-000000006", 2.0, 0, CareHome.care_home),
        ("1-000000007", None, 0, CareHome.care_home),
        ("1-000000008", 0.0, None, CareHome.care_home),
        ("1-000000009", None, None, CareHome.care_home),
        ("1-000000010", 0.0, 0, CareHome.care_home),
        ("1-000000011", 4.0, 10, CareHome.not_care_home),
    ]
    expected_filled_posts_per_bed_ratio_rows = [
        ("1-000000001", 5.0, 100, CareHome.care_home, 0.05),
        ("1-000000002", 2.0, 1, CareHome.care_home, 2.0),
        ("1-000000003", None, 100, CareHome.care_home, None),
        ("1-000000004", 0.0, 1, CareHome.care_home, 0.0),
        ("1-000000005", 5.0, None, CareHome.care_home, None),
        ("1-000000006", 2.0, 0, CareHome.care_home, None),
        ("1-000000007", None, 0, CareHome.care_home, None),
        ("1-000000008", 0.0, None, CareHome.care_home, None),
        ("1-000000009", None, None, CareHome.care_home, None),
        ("1-000000010", 0.0, 0, CareHome.care_home, None),
        ("1-000000011", 4.0, 10, CareHome.not_care_home, None),
    ]
    create_banded_bed_count_column_rows = [
        ("1-001", CareHome.care_home, 1),
        ("1-002", CareHome.care_home, 24),
        ("1-003", CareHome.care_home, 500),
        ("1-004", CareHome.not_care_home, None),
        ("1-005", CareHome.not_care_home, 20),
    ]
    expected_create_banded_bed_count_column_rows = [
        ("1-001", CareHome.care_home, 1, 1.0),
        ("1-002", CareHome.care_home, 24, 1.0),
        ("1-003", CareHome.care_home, 500, 2.0),
        ("1-004", CareHome.not_care_home, None, 0.0),
        ("1-005", CareHome.not_care_home, 20, 0.0),
    ]

    remove_repeated_values_over_time_test_cases = [
        RemoveRepeatedValuesOverTimeTestCase(
            id="values_deduplicated_when_partitioned_by_location_id",
            test_data=[
                ("1-0001", date(2023, 2, 1), 1),
                ("1-0001", date(2023, 3, 1), 2),
                ("1-0001", date(2023, 4, 1), 2),
                ("1-0001", date(2023, 8, 1), 3),
                ("1-0002", date(2023, 2, 1), 3),
                ("1-0002", date(2023, 4, 1), 9),
                ("1-0002", date(2024, 1, 1), 3),
                ("1-0002", date(2024, 2, 1), 3),
            ],
            test_schema=Schemas.remove_repeated_values_over_time_schema,
            columns_to_clean=["value"],
            partition_by_columns="location_id",
            date_column="date",
            expected_data=[
                ("1-0001", date(2023, 2, 1), 1, 1),
                ("1-0001", date(2023, 3, 1), 2, 2),
                ("1-0001", date(2023, 4, 1), 2, None),
                ("1-0001", date(2023, 8, 1), 3, 3),
                ("1-0002", date(2023, 2, 1), 3, 3),
                ("1-0002", date(2023, 4, 1), 9, 9),
                ("1-0002", date(2024, 1, 1), 3, 3),
                ("1-0002", date(2024, 2, 1), 3, None),
            ],
            expected_schema=Schemas.expected_remove_repeated_values_over_time_schema,
        ),
        RemoveRepeatedValuesOverTimeTestCase(
            id="output_unchanged_when_no_consecutive_values_repeat",
            test_data=[
                ("1-0001", date(2023, 2, 1), 1),
                ("1-0001", date(2023, 3, 1), 2),
                ("1-0001", date(2023, 4, 1), 1),
                ("1-0001", date(2023, 8, 1), 3),
            ],
            test_schema=Schemas.remove_repeated_values_over_time_schema,
            columns_to_clean=["value"],
            partition_by_columns="location_id",
            date_column="date",
            expected_data=[
                ("1-0001", date(2023, 2, 1), 1, 1),
                ("1-0001", date(2023, 3, 1), 2, 2),
                ("1-0001", date(2023, 4, 1), 1, 1),
                ("1-0001", date(2023, 8, 1), 3, 3),
            ],
            expected_schema=Schemas.expected_remove_repeated_values_over_time_schema,
        ),
        RemoveRepeatedValuesOverTimeTestCase(
            id="values_deduplicated_when_partition_and_date_columns_are_not_ind_cqc_specific",
            test_data=[
                ("EST1", date(2023, 1, 1), 5),
                ("EST1", date(2023, 2, 1), 5),
                ("EST1", date(2023, 3, 1), 6),
                ("EST2", date(2023, 1, 1), 2),
                ("EST2", date(2023, 2, 1), 3),
            ],
            test_schema=Schemas.remove_repeated_values_over_time_generic_columns_schema,
            columns_to_clean=["value"],
            partition_by_columns="establishment_id",
            date_column="ascwds_workplace_import_date",
            expected_data=[
                ("EST1", date(2023, 1, 1), 5, 5),
                ("EST1", date(2023, 2, 1), 5, None),
                ("EST1", date(2023, 3, 1), 6, 6),
                ("EST2", date(2023, 1, 1), 2, 2),
                ("EST2", date(2023, 2, 1), 3, 3),
            ],
            expected_schema=Schemas.expected_remove_repeated_values_over_time_generic_columns_schema,
        ),
        RemoveRepeatedValuesOverTimeTestCase(
            id="multiple_columns_are_deduplicated_in_a_single_call",
            test_data=[
                ("1-0001", date(2023, 1, 1), 1, "a"),
                ("1-0001", date(2023, 2, 1), 1, "b"),
                ("1-0001", date(2023, 3, 1), 2, "b"),
                ("1-0002", date(2023, 1, 1), 5, "x"),
                ("1-0002", date(2023, 2, 1), 5, "x"),
            ],
            test_schema=Schemas.remove_repeated_values_over_time_multiple_columns_schema,
            columns_to_clean=["first_value", "second_value"],
            partition_by_columns="location_id",
            date_column="date",
            expected_data=[
                ("1-0001", date(2023, 1, 1), 1, "a", 1, "a"),
                ("1-0001", date(2023, 2, 1), 1, "b", None, "b"),
                ("1-0001", date(2023, 3, 1), 2, "b", 2, None),
                ("1-0002", date(2023, 1, 1), 5, "x", 5, "x"),
                ("1-0002", date(2023, 2, 1), 5, "x", None, None),
            ],
            expected_schema=Schemas.expected_remove_repeated_values_over_time_multiple_columns_schema,
        ),
        RemoveRepeatedValuesOverTimeTestCase(
            id="accepts_a_selector_as_well_as_a_list_of_column_names",
            test_data=[
                ("1-0001", date(2023, 1, 1), 1, "x"),
                ("1-0001", date(2023, 2, 1), 1, "y"),
                ("1-0002", date(2023, 1, 1), 2, "y"),
            ],
            test_schema=Schemas.remove_repeated_values_over_time_selector_schema,
            columns_to_clean=cs.starts_with("value_"),
            partition_by_columns="location_id",
            date_column="date",
            expected_data=[
                ("1-0001", date(2023, 1, 1), 1, "x", 1, "x"),
                ("1-0001", date(2023, 2, 1), 1, "y", None, "y"),
                ("1-0002", date(2023, 1, 1), 2, "y", 2, "y"),
            ],
            expected_schema=Schemas.expected_remove_repeated_values_over_time_selector_schema,
        ),
        RemoveRepeatedValuesOverTimeTestCase(
            id="values_deduplicated_independently_per_combination_of_multiple_partition_columns",
            test_data=[
                ("1-0001", "care_worker", date(2023, 1, 1), 5),
                ("1-0001", "care_worker", date(2023, 2, 1), 5),
                ("1-0001", "registered_nurse", date(2023, 1, 1), 5),
                ("1-0001", "registered_nurse", date(2023, 2, 1), 5),
            ],
            test_schema=Schemas.remove_repeated_values_over_time_multiple_partition_columns_schema,
            columns_to_clean=["value"],
            partition_by_columns=["location_id", "job_role"],
            date_column="date",
            expected_data=[
                ("1-0001", "care_worker", date(2023, 1, 1), 5, 5),
                ("1-0001", "care_worker", date(2023, 2, 1), 5, None),
                ("1-0001", "registered_nurse", date(2023, 1, 1), 5, 5),
                ("1-0001", "registered_nurse", date(2023, 2, 1), 5, None),
            ],
            expected_schema=Schemas.expected_remove_repeated_values_over_time_multiple_partition_columns_schema,
        ),
    ]


@dataclass
class RawDataAdjustmentsData:
    CONFIG = Path(__file__).parent.parent / "polars_utils" / "exclusions.json"
    EXCLUSIONS = json.loads(CONFIG.read_text())

    invalid_locations_list = EXCLUSIONS["locationId"].values()
    invalid_locations_list_tuples = [(i, "other") for i in invalid_locations_list]

    locations_data_with_multiple_rows_to_remove = (
        [("loc_1", "other")]
        + invalid_locations_list_tuples
        + invalid_locations_list_tuples
    )

    locations_data_with_only_rows_to_remove = (
        invalid_locations_list_tuples + invalid_locations_list_tuples
    )

    locations_data_without_rows_to_remove = [
        ("loc_1", "other"),
    ]

    expected_locations_data = locations_data_without_rows_to_remove


@dataclass
class ReducedDataFilterCase:
    id: str
    today: date | None
    fy_start_month: int
    lookback_fy_years: int
    quarter_months: tuple[int, ...]
    input_data: list[date]
    expected: list[bool]


@dataclass
class FilteringUtilsData:
    add_filtering_column_rows = [
        ("loc 1", 10.0),
        ("loc 2", None),
    ]
    expected_add_filtering_column_rows = [
        ("loc 1", 10.0, AscwdsFilteringRule.populated),
        ("loc 2", None, AscwdsFilteringRule.missing_data),
    ]

    returns_categorical_col_rows = [
        ("loc 1", 10.0),
        ("loc 2", None),
    ]
    expected_returns_categorical_col_rows = [
        ("loc 1", 10.0, JobRoleFilteringRule.populated),
        ("loc 2", None, JobRoleFilteringRule.missing_raw_data),
    ]

    update_filtering_rule_populated_to_nulled_rows = [
        ("loc 1", 10.0, 10.0, AscwdsFilteringRule.populated),
        ("loc 2", 10.0, None, AscwdsFilteringRule.populated),
        ("loc 3", 10.0, None, AscwdsFilteringRule.missing_data),
    ]
    expected_update_filtering_rule_populated_to_nulled_rows = [
        ("loc 1", 10.0, 10.0, AscwdsFilteringRule.populated),
        ("loc 2", 10.0, None, AscwdsFilteringRule.contained_invalid_missing_data_code),
        ("loc 3", 10.0, None, AscwdsFilteringRule.missing_data),
    ] # fmt: skip

    update_filtering_rule_populated_to_winsorized_rows = [
        ("loc 1", 10.0, 9.0, AscwdsFilteringRule.populated),
        ("loc 2", 10.0, 11.0, AscwdsFilteringRule.populated),
        ("loc 3", 10.0, 10.0, AscwdsFilteringRule.populated),
    ]
    expected_update_filtering_rule_populated_to_winsorized_rows = [
        ("loc 1", 10.0, 9.0, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("loc 2", 10.0, 11.0, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("loc 3", 10.0, 10.0, AscwdsFilteringRule.populated),
    ] # fmt: skip

    update_filtering_rule_winsorized_to_nulled_rows = [
        ("loc 1", 10.0, 9.0, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("loc 2", 10.0, None, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
    ]
    expected_update_filtering_rule_winsorized_to_nulled_rows = [
        ("loc 1", 10.0, 9.0, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("loc 2", 10.0, None, AscwdsFilteringRule.contained_invalid_missing_data_code),
    ] # fmt: skip

    reduced_data_filter_test_cases = [
        ReducedDataFilterCase(
            id="default args",
            today=date(2024, 6, 15),
            fy_start_month=4,
            lookback_fy_years=2,
            quarter_months=(1, 4, 7, 10),
            input_data=[
                date(2021, 4, 1), # before monthly_start but quarterly rule matches -> included
                date(2021, 5, 1), # before monthly_start, non-quarter -> excluded
                date(2022, 3, 31), # before monthly_start and quarterly rule does not match -> excluded
                date(2022, 4, 1), # at boundary (monthly_start) -> included
                date(2023, 6, 1), # within range -> included
            ],
            expected=[True, False, False, True, True],
        ),
        ReducedDataFilterCase(
            id="non_default_args",
            today=date(2024, 6, 15),
            fy_start_month=1,
            lookback_fy_years=1,
            quarter_months=(3, 6, 9, 12),
            input_data=[
                date(2022, 1, 1), # before monthly_start, non-quarter -> excluded
                date(2022, 2, 1), # before monthly_start, non-quarter -> excluded
                date(2022, 12, 1), # before monthly_start but quarterly rule matches -> included
                date(2023, 3, 1), # before monthly_start and quarterly rule matches -> included
                date(2024, 6, 1), # within range -> included
            ],
            expected=[False, False, True, True, True],
        ),
        ReducedDataFilterCase(
            id="today_defaults_to_current_date",
            today=None,
            fy_start_month=4,
            lookback_fy_years=2,
            quarter_months=(1, 4, 7, 10),
            input_data=[
                date.today(),  # should be included as it's the current date
                date(2021, 4, 1), # before monthly_start but quarterly rule matches -> included
                date(2021, 5, 1), # before monthly_start, non-quarter -> excluded
            ],
            expected=[True, True, False],
        ),
    ]  # fmt: skip

    earliest_file_per_month_rows = {
        CQCLClean.location_id: ["loc 1", "loc 2", "loc 3", "loc 4", "loc 5", "loc 6"],
        CQCLClean.cqc_location_import_date: [
            date(2022, 1, 1),
            date(2022, 1, 5),
            date(2022, 2, 5),
            date(2022, 2, 7),
            date(2022, 3, 1),
            date(2022, 4, 2),
        ],
    }
    expected_earliest_file_per_month_rows = {
        CQCLClean.location_id: ["loc 1", "loc 3", "loc 5", "loc 6"],
        CQCLClean.cqc_location_import_date: [
            date(2022, 1, 1),
            date(2022, 2, 5),
            date(2022, 3, 1),
            date(2022, 4, 2),
        ],
    }


@dataclass
class CategoricalColumnTypeCase:
    id: str
    actual: Any
    expected: Any


@dataclass
class ColumnTypesData:
    categorical_column_type_cases = [
        CategoricalColumnTypeCase(
            id="location_cat_type",
            actual=CatColType.LocationCatType,
            expected=pl.Categorical(
                pl.Categories("location", namespace="filled_posts")
            ),
        ),
        CategoricalColumnTypeCase(
            id="establishment_cat_type",
            actual=CatColType.EstablishmentCatType,
            expected=pl.Categorical(
                pl.Categories("establishment", namespace="filled_posts")
            ),
        ),
        CategoricalColumnTypeCase(
            id="provider_cat_type",
            actual=CatColType.ProviderCatType,
            expected=pl.Categorical(
                pl.Categories("provider", namespace="filled_posts")
            ),
        ),
        CategoricalColumnTypeCase(
            id="brand_cat_type",
            actual=CatColType.BrandCatType,
            expected=pl.Categorical(pl.Categories("brand", namespace="filled_posts")),
        ),
        CategoricalColumnTypeCase(
            id="job_role_cat_type",
            actual=CatColType.JobRoleCatType,
            expected=pl.Categorical(
                pl.Categories("job_role", namespace="filled_posts")
            ),
        ),
        CategoricalColumnTypeCase(
            id="job_group_cat_type",
            actual=CatColType.JobGroupCatType,
            expected=pl.Categorical(
                pl.Categories("job_group", namespace="filled_posts")
            ),
        ),
        CategoricalColumnTypeCase(
            id="published_job_role_label_cat_type",
            actual=CatColType.PublishedJobRoleLabelCatType,
            expected=pl.Categorical(
                pl.Categories("published_job_role_label", namespace="filled_posts")
            ),
        ),
        CategoricalColumnTypeCase(
            id="estimates_filled_post_source_enum_type",
            actual=CatColType.EstimatesFilledPostSourceEnumType,
            expected=pl.Enum(
                [
                    EstimateFilledPostsSource.imputed_pir_filled_posts_model,
                    EstimateFilledPostsSource.ascwds_pir_merged,
                    EstimateFilledPostsSource.imputed_posts_care_home_model,
                    EstimateFilledPostsSource.care_home_model,
                    EstimateFilledPostsSource.imputed_posts_non_res_combined_model,
                    EstimateFilledPostsSource.non_res_combined_model,
                    EstimateFilledPostsSource.posts_rolling_average_model,
                ]
            ),
        ),
        CategoricalColumnTypeCase(
            id="primary_service_enum_type",
            actual=CatColType.PrimaryServiceEnumType,
            expected=pl.Enum(
                CQCLocationCatVals.primary_service_type_column_values.categorical_values
            ),
        ),
        CategoricalColumnTypeCase(
            id="job_role_filtering_rule_cat_type",
            actual=CatColType.JobRoleFilteringRuleCatType,
            expected=pl.Categorical(
                pl.Categories(
                    "job_role_filtering_rule",
                    namespace="filled_posts",
                    physical=pl.UInt8,
                )
            ),
        ),
        CategoricalColumnTypeCase(
            id="care_home_enum_type",
            actual=CatColType.CareHomeEnumType,
            expected=pl.Enum(
                CQCLocationCatVals.care_home_column_values.categorical_values
            ),
        ),
        CategoricalColumnTypeCase(
            id="dormancy_enum_type",
            actual=CatColType.DormancyEnumType,
            expected=pl.Enum(
                CQCLocationCatVals.dormancy_column_values.categorical_values
            ),
        ),
        CategoricalColumnTypeCase(
            id="cqc_sector_enum_type",
            actual=CatColType.CqcSectorEnumType,
            expected=pl.Enum(
                CQCLocationCatVals.sector_column_values.categorical_values
            ),
        ),
        CategoricalColumnTypeCase(
            id="primary_service_type_second_level_cat_type",
            actual=CatColType.PrimaryServiceTypeSecondLevelCatType,
            expected=pl.Categorical(
                pl.Categories(
                    "primary_service_type_second_level", namespace="filled_posts"
                )
            ),
        ),
        CategoricalColumnTypeCase(
            id="ons_rural_urban_ind_11_enum_type",
            actual=CatColType.OnsRuralUrbanInd11EnumType,
            expected=pl.Enum(
                CQCLocationCatVals.current_rui_column_values.categorical_values
            ),
        ),
        CategoricalColumnTypeCase(
            id="ons_region_cat_type",
            actual=CatColType.OnsRegionCatType,
            expected=pl.Categorical(
                pl.Categories("ons_region", namespace="filled_posts")
            ),
        ),
        CategoricalColumnTypeCase(
            id="ons_cssr_cat_type",
            actual=CatColType.OnsCssrCatType,
            expected=pl.Categorical(
                pl.Categories("ons_cssr", namespace="filled_posts")
            ),
        ),
        CategoricalColumnTypeCase(
            id="ons_icb_cat_type",
            actual=CatColType.OnsIcbCatType,
            expected=pl.Categorical(pl.Categories("ons_icb", namespace="filled_posts")),
        ),
        CategoricalColumnTypeCase(
            id="ons_sub_icb_cat_type",
            actual=CatColType.OnsSubIcbCatType,
            expected=pl.Categorical(
                pl.Categories("ons_sub_icb", namespace="filled_posts")
            ),
        ),
        CategoricalColumnTypeCase(
            id="ons_icb_region_cat_type",
            actual=CatColType.OnsIcbRegionCatType,
            expected=pl.Categorical(
                pl.Categories("ons_icb_region", namespace="filled_posts")
            ),
        ),
    ]
