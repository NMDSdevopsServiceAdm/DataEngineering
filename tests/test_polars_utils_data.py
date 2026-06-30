import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from utils.column_values.categorical_column_values import (
    AscwdsFilteringRule,
    CareHome,
    ContemporaryCSSR,
    JobRoleFilteringRule,
)


@dataclass
class CleaningUtilsTestCase:
    id: str
    test_data: list[Any]
    expected_data: list[Any]
    column_names: list[str]
    add_as_new_column: Optional[bool]
    reverse_mapping: Optional[bool]


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

    gender_labels = AWK.gender + "_labels"
    gender_codes = AWK.gender + "_codes"
    nationality_labels = AWK.nationality + "_labels"
    nationality_codes = AWK.nationality + "_codes"
    contemporary_cssr_codes = IndCQC.contemporary_cssr + "_codes"

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
            reverse_mapping=False,
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
            reverse_mapping=False,
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
            reverse_mapping=False,
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
            reverse_mapping=False,
        ),
        CleaningUtilsTestCase(
            id="adds_single_column_with_codes_when_single_column_name_passed_and_add_as_new_column_is_true_and_reverse_mapping_is_true",
            test_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["male", "female", None, "female"],
                AWK.nationality: ["British", "French", "Spanish", None],
            },
            expected_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["male", "female", None, "female"],
                AWK.nationality: ["British", "French", "Spanish", None],
                gender_codes: ["1", "2", None, "2"],
            },
            column_names=[AWK.gender],
            add_as_new_column=True,
            reverse_mapping=True,
        ),
        CleaningUtilsTestCase(
            id="adds_multiple_columns_with_codes_when_multiple_column_names_passed_and_add_as_new_column_is_true_and_reverse_mapping_is_true",
            test_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["male", "female", None, "female"],
                AWK.nationality: ["British", "French", "Spanish", None],
            },
            expected_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["male", "female", None, "female"],
                AWK.nationality: ["British", "French", "Spanish", None],
                gender_codes: ["1", "2", None, "2"],
                nationality_codes: ["100", "101", "102", None],
            },
            column_names=[AWK.gender, AWK.nationality],
            add_as_new_column=True,
            reverse_mapping=True,
        ),
        CleaningUtilsTestCase(
            id="replaces_single_column_with_codes_when_single_column_name_passed_and_add_as_new_column_is_false_and_reverse_mapping_is_true",
            test_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["male", "female", None, "female"],
                AWK.nationality: ["British", "French", "Spanish", None],
            },
            expected_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["1", "2", None, "2"],
                AWK.nationality: ["British", "French", "Spanish", None],
            },
            column_names=[AWK.gender],
            add_as_new_column=False,
            reverse_mapping=True,
        ),
        CleaningUtilsTestCase(
            id="replaces_multiple_columns_with_codes_when_multiple_column_names_passed_and_add_as_new_column_is_false_and_reverse_mapping_is_true",
            test_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["male", "female", None, "female"],
                AWK.nationality: ["British", "French", "Spanish", None],
            },
            expected_data={
                AWK.worker_id: ["1", "2", "3", "4"],
                AWK.gender: ["1", "2", None, "2"],
                AWK.nationality: ["100", "101", "102", None],
            },
            column_names=[AWK.gender, AWK.nationality],
            add_as_new_column=False,
            reverse_mapping=True,
        ),
        CleaningUtilsTestCase(
            id="adds_single_column_with_first_code_in_dict_when_single_column_name_passed_and_label_dict_one_to_many_values_and_add_as_new_column_is_true_and_reverse_mapping_is_true",
            test_data={
                AWK.worker_id: ["1", "2", "3"],
                IndCQC.contemporary_cssr: [
                    ContemporaryCSSR.cornwall_and_isles_of_scilly,
                    ContemporaryCSSR.cornwall_and_isles_of_scilly,
                    ContemporaryCSSR.coventry,
                ],
            },
            expected_data={
                AWK.worker_id: ["1", "2", "3"],
                IndCQC.contemporary_cssr: [
                    ContemporaryCSSR.cornwall_and_isles_of_scilly,
                    ContemporaryCSSR.cornwall_and_isles_of_scilly,
                    ContemporaryCSSR.coventry,
                ],
                contemporary_cssr_codes: ["902", "902", "407"],
            },
            column_names=[IndCQC.contemporary_cssr],
            add_as_new_column=True,
            reverse_mapping=True,
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
            reverse_mapping=False,
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
            reverse_mapping=False,
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
    reduce_dataset_to_earliest_file_per_month_rows = [
        ("loc 1", date(2022, 1, 1)),
        ("loc 2", date(2022, 1, 5)),
        ("loc 3", date(2022, 2, 5)),
        ("loc 4", date(2022, 2, 7)),
        ("loc 5", date(2022, 3, 1)),
        ("loc 6", date(2022, 4, 2)),
    ]
    expected_reduce_dataset_to_earliest_file_per_month_rows = [
        ("loc 1", date(2022, 1, 1)),
        ("loc 3", date(2022, 2, 5)),
        ("loc 5", date(2022, 3, 1)),
        ("loc 6", date(2022, 4, 2)),
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
