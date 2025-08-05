from dataclasses import dataclass
from datetime import date

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    CareHome,
    PrimaryServiceType,
    Sector,
)
from utils.raw_data_adjustments import RecordsToRemoveInLocationsData
from utils.validation.validation_rule_custom_type import CustomValidationRules
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class UtilsData:
    cqc_pir_rows = [
        ("1-1199876096", "Y", date(2022, 2, 1), date(2021, 5, 7)),
        ("1-1199876096", "Y", date(2022, 7, 1), date(2022, 5, 20)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 12)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 24)),
        ("1-1199876096", "N", date(2023, 6, 1), date(2023, 5, 24)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 24)),
    ]

    filter_to_max_value_rows = [
        ("1", date(2024, 1, 1), "20220101"),
        ("2", date(2024, 1, 1), "20230101"),
        ("3", date(2023, 1, 1), "20240101"),
    ]
    expected_filter_to_max_date_rows = [
        ("1", date(2024, 1, 1), "20220101"),
        ("2", date(2024, 1, 1), "20230101"),
    ]
    expected_filter_to_max_string_rows = [
        ("3", date(2023, 1, 1), "20240101"),
    ]

    select_rows_with_value_rows = [
        ("id_1", "keep"),
        ("id_2", "remove"),
    ]

    select_rows_with_non_null_values_rows = [
        ("1-00001", None),
        ("1-00002", 12.34),
        ("1-00003", -1.0),
    ]
    expected_select_rows_with_non_null_values_rows = [
        ("1-00002", 12.34),
        ("1-00003", -1.0),
    ]


@dataclass
class CleaningUtilsData:
    worker_rows = [
        ("1", "1", "100"),
        ("2", "1", "101"),
        ("3", "2", "102"),
        ("4", "2", "103"),
        ("5", None, "103"),
        ("6", "2", None),
    ]

    gender = {
        "1": "male",
        "2": "female",
    }

    nationality = {
        "100": "British",
        "101": "French",
        "102": "Spanish",
        "103": "Portuguese",
    }

    expected_rows_with_new_columns = [
        ("1", "1", "100", "male", "British"),
        ("2", "1", "101", "male", "French"),
        ("3", "2", "102", "female", "Spanish"),
        ("4", "2", "103", "female", "Portuguese"),
        ("5", None, "103", None, "Portuguese"),
        ("6", "2", None, "female", None),
    ]

    expected_rows_without_new_columns = [
        ("1", "male", "British"),
        ("2", "male", "French"),
        ("3", "female", "Spanish"),
        ("4", "female", "Portuguese"),
        ("5", None, "Portuguese"),
        ("6", "female", None),
    ]

    scale_data = [
        (23, 10.1, "non scale"),
        (-1, 10.1, "non scale"),
        (24, -20.345, "non scale"),
        (-234, 999.99, "non scale"),
    ]

    expected_scale_data = [
        (23, 10.1, "non scale", 23, 10.1),
        (-1, 10.1, "non scale", None, 10.1),
        (24, -20.345, "non scale", 24, None),
        (-234, 999.99, "non scale", None, None),
    ]
    #
    align_dates_primary_rows = [
        (date(2020, 1, 1), "loc 1"),
        (date(2020, 1, 8), "loc 1"),
        (date(2021, 1, 1), "loc 1"),
        (date(2021, 1, 1), "loc 2"),
    ]

    align_dates_secondary_rows = [
        (date(2018, 1, 1), "loc 1"),
        (date(2019, 1, 1), "loc 1"),
        (date(2020, 1, 1), "loc 1"),
        (date(2020, 2, 1), "loc 1"),
        (date(2021, 1, 8), "loc 1"),
        (date(2020, 2, 1), "loc 2"),
    ]

    expected_aligned_dates_rows = [
        (
            date(2020, 1, 1),
            date(2020, 1, 1),
        ),
        (
            date(2020, 1, 8),
            date(2020, 1, 1),
        ),
        (
            date(2021, 1, 1),
            date(2020, 2, 1),
        ),
    ]

    align_later_dates_secondary_rows = [
        (date(2020, 2, 1), "loc 1"),
        (date(2021, 1, 8), "loc 1"),
        (date(2020, 2, 1), "loc 2"),
    ]

    expected_later_aligned_dates_rows = [
        (
            date(2021, 1, 1),
            date(2020, 2, 1),
        ),
    ]

    expected_cross_join_rows = [
        (
            date(2020, 1, 1),
            date(2019, 1, 1),
        ),
        (
            date(2020, 1, 8),
            date(2019, 1, 1),
        ),
        (
            date(2021, 1, 1),
            date(2019, 1, 1),
        ),
        (
            date(2020, 1, 1),
            date(2020, 1, 1),
        ),
        (
            date(2020, 1, 8),
            date(2020, 1, 1),
        ),
        (
            date(2021, 1, 1),
            date(2020, 1, 1),
        ),
        (
            date(2020, 1, 1),
            date(2020, 2, 1),
        ),
        (
            date(2020, 1, 8),
            date(2020, 2, 1),
        ),
        (
            date(2021, 1, 1),
            date(2020, 2, 1),
        ),
        (
            date(2020, 1, 1),
            date(2021, 1, 8),
        ),
        (
            date(2020, 1, 8),
            date(2021, 1, 8),
        ),
        (
            date(2021, 1, 1),
            date(2021, 1, 8),
        ),
        (
            date(2020, 1, 1),
            date(2018, 1, 1),
        ),
        (
            date(2020, 1, 8),
            date(2018, 1, 1),
        ),
        (
            date(2021, 1, 1),
            date(2018, 1, 1),
        ),
    ]

    expected_merged_rows = [
        (date(2020, 1, 1), date(2020, 1, 1), "loc 1"),
        (date(2020, 1, 8), date(2020, 1, 1), "loc 1"),
        (date(2021, 1, 1), date(2020, 2, 1), "loc 1"),
        (date(2021, 1, 1), date(2020, 2, 1), "loc 2"),
    ]

    expected_later_merged_rows = [
        (date(2020, 1, 1), None, "loc 1"),
        (date(2020, 1, 8), None, "loc 1"),
        (date(2021, 1, 1), date(2020, 2, 1), "loc 1"),
        (date(2021, 1, 1), date(2020, 2, 1), "loc 2"),
    ]

    column_to_date_data = [
        ("20230102", date(2023, 1, 2)),
        ("20220504", date(2022, 5, 4)),
        ("20191207", date(2019, 12, 7)),
        ("19081205", date(1908, 12, 5)),
    ]
    reduce_dataset_to_earliest_file_per_month_rows = [
        ("loc 1", "20220101", "2022", "01", "01"),
        ("loc 2", "20220105", "2022", "01", "05"),
        ("loc 3", "20220205", "2022", "02", "05"),
        ("loc 4", "20220207", "2022", "02", "07"),
        ("loc 5", "20220301", "2022", "03", "01"),
        ("loc 6", "20220402", "2022", "04", "02"),
    ]
    expected_reduce_dataset_to_earliest_file_per_month_rows = [
        ("loc 1", "20220101", "2022", "01", "01"),
        ("loc 3", "20220205", "2022", "02", "05"),
        ("loc 5", "20220301", "2022", "03", "01"),
        ("loc 6", "20220402", "2022", "04", "02"),
    ]

    cast_to_int_rows = [
        (
            "loc 1",
            "20",
            "18",
        ),
    ]

    cast_to_int_errors_rows = [
        (
            "loc 1",
            "20",
            "18",
        ),
        (
            "loc 2",
            "ZO",
            "18",
        ),
        (
            "loc 3",
            "20",
            "IB",
        ),
        (
            "loc 4",
            "ZO",
            "IB",
        ),
    ]

    cast_to_int_expected_rows = [
        (
            "loc 1",
            20,
            18,
        ),
    ]

    cast_to_int_errors_expected_rows = [
        (
            "loc 1",
            20,
            18,
        ),
        (
            "loc 2",
            None,
            18,
        ),
        (
            "loc 3",
            20,
            None,
        ),
        (
            "loc 4",
            None,
            None,
        ),
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

    filled_posts_from_beds_and_ratio_rows = [
        ("loc 1", 0.5, 10),
        ("loc 2", None, 10),
        ("loc 3", 0.5, None),
    ]
    expected_filled_posts_from_beds_and_ratio_rows = [
        ("loc 1", 0.5, 10, 5.0),
        ("loc 2", None, 10, None),
        ("loc 3", 0.5, None, None),
    ]

    remove_duplicate_locationids_rows = [
        (date(2024, 1, 1), "1-001", date(2023, 1, 1)),
        (date(2024, 1, 1), "1-001", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-001", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-002", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-002", date(2023, 2, 1)),
    ]
    expected_remove_duplicate_locationids_descending_rows = [
        (date(2024, 1, 1), "1-001", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-001", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-002", date(2023, 2, 1)),
    ]
    expected_remove_duplicate_locationids_ascending_rows = [
        (date(2024, 1, 1), "1-001", date(2023, 1, 1)),
        (date(2024, 2, 1), "1-001", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-002", date(2023, 2, 1)),
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
class EstimateIndCQCFilledPostsData:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-1783948", date(2022, 2, 1), "South East", "South East", 0, ["Domiciliary care service"], "non-residential", 5, None, None, "N", "Independent", "Rural hamlet and isolated dwellings in a sparse setting", "Rural hamlet and isolated dwellings in a sparse setting", "rule_1", "Registered"),
        ("1-1783948", date(2022, 1, 1), "South East", "South East", 0, ["Domiciliary care service"], "non-residential", 5, 67.0, 67.0, "N", "Independent", "Rural hamlet and isolated dwellings in a sparse setting", "Rural hamlet and isolated dwellings in a sparse setting", "rule_2", "Registered"),
        ("1-348374832", date(2022, 1, 12), "Merseyside", "Merseyside", 0, ["Extra Care housing services"], "non-residential", None, 34.0, 34.0, "N", "Local authority", "Rural hamlet and isolated dwellings", "Rural hamlet and isolated dwellings", "rule_3", "Registered"),
        ("1-683746776", date(2022, 1, 1), "Merseyside", "Merseyside", 0, ["Doctors treatment service", "Long term conditions services", "Shared Lives"], "non-residential", 34, None, None, "N", "Local authority", "Rural hamlet and isolated dwellings", "Rural hamlet and isolated dwellings", "rule_1", "Registered"),
        ("1-10478686", date(2022, 1, 1), "London Senate", "London Senate", 0, ["Community health care services - Nurses Agency only"], "non-residential", None, None, None, "N", "", "Rural hamlet and isolated dwellings", "Rural hamlet and isolated dwellings", "rule_1", "Registered"),
        ("1-10235302415", date(2022, 1, 12), "South West", "South West", 0, ["Urgent care services", "Supported living service"], "non-residential", 17, None, None, "N", "Independent", "Rural hamlet and isolated dwellings", "Rural hamlet and isolated dwellings", "rule_3", "Registered"),
        ("1-1060912125", date(2022, 1, 12), "Yorkshire and the Humber", "Yorkshire and the Humber", 0, ["Hospice services at home"], "non-residential", 34, None, None, "N", "Independent", "Rural hamlet and isolated dwellings", "Rural hamlet and isolated dwellings", "rule_2", "Registered"),
        ("1-107095666", date(2022, 3, 1), "Yorkshire and the Humber", "Yorkshire and the Humber", 0, ["Specialist college service", "Community based services for people who misuse substances", "Urgent care services'"], "non-residential", 34, None, None, "N", "Independent", "Urban city and town", "Urban city and town", "rule_3", "Registered"),
        ("1-108369587", date(2022, 3, 8), "South West", "South West", 0, ["Specialist college service"], "non-residential", 15, None, None, "N", "Independent", "Rural town and fringe in a sparse setting", "Rural town and fringe in a sparse setting", "rule_1", "Registered"),
        ("1-10758359583", date(2022, 3, 8), None, None, 0, ["Mobile doctors service"], "non-residential", 17, None, None, "N", "Local authority", "Urban city and town", "Urban city and town", "rule_2", "Registered"),
        ("1-000000001", date(2022, 3, 8), "Yorkshire and the Humber", "Yorkshire and the Humber", 67, ["Care home service with nursing"], "Care home with nursing", None, None, None, "Y", "Local authority", "Urban city and town", "Urban city and town", "rule_1", "Registered"),
        ("1-10894414510", date(2022, 3, 8), "Yorkshire and the Humber", "Yorkshire and the Humber", 10, ["Care home service with nursing"], "Care home with nursing", 0, 25.0, 25.0, "Y", "Independent", "Urban city and town", "Urban city and town", "rule_3", "Registered"),
        ("1-108950835", date(2022, 3, 15), "Merseyside", "Merseyside", 20, ["Care home service without nursing"], "Care home without nursing", 23, None, None, "Y", "", "Urban city and town", "Urban city and town", "rule_1", "Registered"),
        ("1-108967195", date(2022, 4, 22), "North West", "North West", 0, ["Supported living service", "Acute services with overnight beds"], "non-residential", 11, None, None, "N", "Independent", "Urban city and town", "Urban city and town", "rule_3", "Registered"),
    ]
    # fmt: on


@dataclass
class ValidationUtils:
    size_of_dataset_rule = {RuleName.size_of_dataset: 3}
    size_of_dataset_success_rows = [
        ("loc_1",),
        ("loc_2",),
        ("loc_3",),
    ]
    size_of_dataset_extra_rows = [
        ("loc_1",),
        ("loc_2",),
        ("loc_3",),
        ("loc_4",),
    ]
    size_of_dataset_missing_rows = [
        ("loc_1",),
        ("loc_2",),
    ]
    size_of_dataset_result_success_rows = [
        (
            "Size of dataset",
            "Warning",
            "Success",
            "SizeConstraint(Size(None))",
            "Success",
            "",
        )
    ]
    size_of_dataset_result_missing_rows = [
        (
            "Size of dataset",
            "Warning",
            "Warning",
            "SizeConstraint(Size(None))",
            "Failure",
            "Value: 2 does not meet the constraint requirement! DataFrame row count should be 3.",
        )
    ]
    size_of_dataset_result_extra_rows = [
        (
            "Size of dataset",
            "Warning",
            "Warning",
            "SizeConstraint(Size(None))",
            "Failure",
            "Value: 4 does not meet the constraint requirement! DataFrame row count should be 3.",
        )
    ]

    unique_index_columns_rule = {
        RuleName.index_columns: [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
        ]
    }
    unique_index_columns_success_rows = [
        (
            "loc_1",
            date(2024, 1, 1),
        ),
        (
            "loc_1",
            date(2024, 1, 2),
        ),
        (
            "loc_2",
            date(2024, 1, 1),
        ),
    ]
    unique_index_columns_not_unique_rows = [
        (
            "loc_1",
            date(2024, 1, 1),
        ),
        (
            "loc_1",
            date(2024, 1, 1),
        ),
    ]
    unique_index_columns_result_success_rows = [
        (
            "Index columns are unique",
            "Warning",
            "Success",
            "UniquenessConstraint(Uniqueness(Stream(locationId, ?),None,None))",
            "Success",
            "",
        ),
    ]
    unique_index_columns_result_not_unique_rows = [
        (
            "Index columns are unique",
            "Warning",
            "Warning",
            "UniquenessConstraint(Uniqueness(Stream(locationId, ?),None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Uniqueness should be 1.",
        ),
    ]

    one_complete_column_rule = {
        RuleName.complete_columns: [
            IndCQC.location_id,
        ]
    }
    two_complete_columns_rule = {
        RuleName.complete_columns: [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
        ]
    }
    one_complete_column_complete_rows = [
        ("loc_1",),
    ]
    one_complete_column_incomplete_rows = [
        (None,),
    ]
    two_complete_columns_both_complete_rows = [
        ("loc_1", date(2024, 1, 1)),
    ]
    two_complete_columns_one_incomplete_rows = [
        (None, date(2024, 1, 1)),
    ]
    two_complete_columns_both_incomplete_rows = [
        (None, None),
    ]

    one_complete_column_result_complete_rows = [
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Success",
            "",
        ),
    ]
    one_complete_column_result_incomplete_rows = [
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Completeness of locationId should be 1.",
        ),
    ]
    two_complete_columns_result_both_complete_rows = [
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Success",
            "",
        ),
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None,None))",
            "Success",
            "",
        ),
    ]
    two_complete_columns_result_one_incomplete_rows = [
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Completeness of locationId should be 1.",
        ),
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None,None))",
            "Success",
            "",
        ),
    ]
    two_complete_columns_result_both_incomplete_rows = [
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Completeness of locationId should be 1.",
        ),
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Completeness of cqc_location_import_date should be 1.",
        ),
    ]

    multiple_rules = {
        RuleName.size_of_dataset: 1,
        RuleName.index_columns: [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
        ],
        RuleName.complete_columns: [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
        ],
    }

    multiple_rules_rows = [
        ("loc_1", date(2024, 1, 1)),
    ]
    multiple_rules_results_rows = [
        (
            "Size of dataset",
            "Warning",
            "Success",
            "SizeConstraint(Size(None))",
            "Success",
            "",
        ),
        (
            "Index columns are unique",
            "Warning",
            "Success",
            "UniquenessConstraint(Uniqueness(Stream(locationId, ?),None,None))",
            "Success",
            "",
        ),
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Success",
            "",
        ),
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None,None))",
            "Success",
            "",
        ),
    ]

    unknown_rules = {
        RuleName.size_of_dataset: 1,
        "unknown_rule": "some_value",
    }

    min_values_rule = {
        RuleName.min_values: {
            IndCQC.number_of_beds: 1,
        }
    }
    min_values_multiple_columns_rule = {
        RuleName.min_values: {
            IndCQC.number_of_beds: 1,
            IndCQC.pir_people_directly_employed_cleaned: 0,
        }
    }
    min_values_below_minimum_rows = [
        ("loc_1", 0),
    ]
    min_values_equal_minimum_rows = [
        ("loc_1", 1),
    ]
    min_values_above_minimum_rows = [
        ("loc_1", 2),
    ]
    min_values_multiple_columns_rows = [
        ("loc_1", 0, 0),
    ]

    min_values_result_success_rows = [
        (
            "Min value in column",
            "Warning",
            "Success",
            "MinimumConstraint(Minimum(numberOfBeds,None,None))",
            "Success",
            "",
        ),
    ]
    min_values_result_below_minimum_rows = [
        (
            "Min value in column",
            "Warning",
            "Warning",
            "MinimumConstraint(Minimum(numberOfBeds,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! The minimum value for numberOfBeds should be 1.",
        ),
    ]
    min_values_result_multiple_columns_rows = [
        (
            "Min value in column",
            "Warning",
            "Warning",
            "MinimumConstraint(Minimum(numberOfBeds,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! The minimum value for numberOfBeds should be 1.",
        ),
        (
            "Min value in column",
            "Warning",
            "Success",
            "MinimumConstraint(Minimum(pir_people_directly_employed_cleaned,None,None))",
            "Success",
            "",
        ),
    ]

    max_values_rule = {
        RuleName.max_values: {
            IndCQC.number_of_beds: 10,
        }
    }
    max_values_multiple_columns_rule = {
        RuleName.max_values: {
            IndCQC.number_of_beds: 10,
            IndCQC.pir_people_directly_employed_cleaned: 20,
        }
    }
    max_values_below_maximum_rows = [
        ("loc_1", 9),
    ]
    max_values_equal_maximum_rows = [
        ("loc_1", 10),
    ]
    max_values_above_maximum_rows = [
        ("loc_1", 11),
    ]
    max_values_multiple_columns_rows = [
        ("loc_1", 20, 20),
    ]

    max_values_result_success_rows = [
        (
            "Max value in column",
            "Warning",
            "Success",
            "MaximumConstraint(Maximum(numberOfBeds,None,None))",
            "Success",
            "",
        ),
    ]
    max_values_result_above_maximum_rows = [
        (
            "Max value in column",
            "Warning",
            "Warning",
            "MaximumConstraint(Maximum(numberOfBeds,None,None))",
            "Failure",
            "Value: 11.0 does not meet the constraint requirement! The maximum value for numberOfBeds should be 10.",
        ),
    ]
    max_values_result_multiple_columns_rows = [
        (
            "Max value in column",
            "Warning",
            "Warning",
            "MaximumConstraint(Maximum(numberOfBeds,None,None))",
            "Failure",
            "Value: 20.0 does not meet the constraint requirement! The maximum value for numberOfBeds should be 10.",
        ),
        (
            "Max value in column",
            "Warning",
            "Success",
            "MaximumConstraint(Maximum(pir_people_directly_employed_cleaned,None,None))",
            "Success",
            "",
        ),
    ]

    categorical_values_rule = {
        RuleName.categorical_values_in_columns: {
            IndCQC.cqc_sector: [Sector.independent, Sector.local_authority]
        }
    }
    categorical_values_all_present_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", Sector.local_authority),
    ]
    categorical_values_some_present_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", None),
    ]
    categorical_values_extra_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", Sector.local_authority),
        ("loc_3", "other value"),
    ]

    categorical_values_result_success_rows = [
        (
            "Categorical values are in list of expected values",
            "Warning",
            "Success",
            "ComplianceConstraint(Compliance(cqc_sector contained in Independent,Local authority,`cqc_sector` IS NULL OR `cqc_sector` IN ('Independent','Local authority'),None,List(cqc_sector),None))",
            "Success",
            "",
        ),
    ]
    categorical_values_result_failure_rows = [
        (
            "Categorical values are in list of expected values",
            "Warning",
            "Warning",
            "ComplianceConstraint(Compliance(cqc_sector contained in Independent,Local authority,`cqc_sector` IS NULL OR `cqc_sector` IN ('Independent','Local authority'),None,List(cqc_sector),None))",
            "Failure",
            "Value: 0.6666666666666666 does not meet the constraint requirement! Values in cqc_sector should be one of :['Independent', 'Local authority'].",
        ),
    ]

    distinct_values_rule = {
        RuleName.distinct_values: {
            IndCQC.cqc_sector: 2,
        }
    }

    distinct_values_multiple_columns_rule = {
        RuleName.distinct_values: {
            IndCQC.cqc_sector: 2,
            IndCQC.dormancy: 3,
        }
    }

    distinct_values_success_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", Sector.local_authority),
    ]
    fewer_distinct_values_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", Sector.independent),
    ]
    more_distinct_values_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", Sector.local_authority),
        ("loc_3", None),
    ]
    distinct_values_multiple_columns_rows = [
        ("loc_1", Sector.independent, "Y"),
        ("loc_2", Sector.local_authority, "N"),
        ("loc_3", None, None),
    ]

    distinct_values_result_success_rows = [
        (
            "Column contains correct number of distinct values",
            "Warning",
            "Success",
            "HistogramBinConstraint(Histogram(cqc_sector,null,2,None,false,Count))",
            "Success",
            "",
        ),
    ]
    fewer_distinct_values_result_rows = [
        (
            "Column contains correct number of distinct values",
            "Warning",
            "Warning",
            "HistogramBinConstraint(Histogram(cqc_sector,null,2,None,false,Count))",
            "Failure",
            "Value: 1 does not meet the constraint requirement! The number of distinct values in cqc_sector should be 2.",
        ),
    ]
    more_distinct_values_result_rows = [
        (
            "Column contains correct number of distinct values",
            "Warning",
            "Warning",
            "HistogramBinConstraint(Histogram(cqc_sector,null,2,None,false,Count))",
            "Failure",
            "Value: 3 does not meet the constraint requirement! The number of distinct values in cqc_sector should be 2.",
        ),
    ]
    distinct_values_result_multiple_columns_rows = [
        (
            "Column contains correct number of distinct values",
            "Warning",
            "Warning",
            "HistogramBinConstraint(Histogram(cqc_sector,null,2,None,false,Count))",
            "Failure",
            "Value: 3 does not meet the constraint requirement! The number of distinct values in cqc_sector should be 2.",
        ),
        (
            "Column contains correct number of distinct values",
            "Warning",
            "Success",
            "HistogramBinConstraint(Histogram(dormancy,null,3,None,false,Count))",
            "Success",
            "",
        ),
    ]

    add_column_with_length_of_string_rows = [
        ("loc_1",),
    ]
    expected_add_column_with_length_of_string_rows = [
        ("loc_1", 5),
    ]

    check_rows = fewer_distinct_values_result_rows

    custom_type_rule = {
        RuleName.custom_type: CustomValidationRules.care_home_and_primary_service_type
    }

    custom_type_related_rows = [
        ("loc 1", CareHome.care_home, PrimaryServiceType.care_home_only),
        ("loc 2", CareHome.care_home, PrimaryServiceType.care_home_with_nursing),
        ("loc 3", CareHome.not_care_home, PrimaryServiceType.non_residential),
    ]
    expected_custom_type_related_rows = [
        (
            "custom type",
            "Warning",
            "Success",
            "ComplianceConstraint(Compliance(care_home_and_primary_service_type,(careHome = 'N' AND primary_service_type = 'non-residential') OR (careHome = 'Y' AND primary_service_type = 'Care home with nursing') OR (careHome = 'Y' AND primary_service_type = 'Care home without nursing'),None,List(),None))",
            "Success",
            "",
        ),
    ]

    custom_type_unrelated_rows = [
        ("loc 1", CareHome.care_home, PrimaryServiceType.non_residential),
        ("loc 2", CareHome.not_care_home, PrimaryServiceType.care_home_with_nursing),
        ("loc 3", CareHome.not_care_home, PrimaryServiceType.care_home_only),
    ]
    expected_custom_type_unrelated_rows = [
        (
            "custom type",
            "Warning",
            "Warning",
            "ComplianceConstraint(Compliance(care_home_and_primary_service_type,(careHome = 'N' AND primary_service_type = 'non-residential') OR (careHome = 'Y' AND primary_service_type = 'Care home with nursing') OR (careHome = 'Y' AND primary_service_type = 'Care home without nursing'),None,List(),None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! The data in carehome and primary_service_type should be related.",
        ),
    ]


@dataclass
class ValidateEstimatedIndCqcFilledPostsData:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1),),
        ("1-000000002", date(2024, 1, 1),),
        ("1-000000001", date(2024, 2, 1),),
        ("1-000000002", date(2024, 2, 1),),
    ]

    estimated_ind_cqc_filled_posts_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0),
    ]
    # fmt: on

    calculate_expected_size_rows = [
        (
            "1-000000001",
            date(2024, 1, 1),
        ),
    ]


@dataclass
class RawDataAdjustments:
    expected_worker_data = [
        ("worker_1", "20240101", "estab_1", "other"),
        ("1737540", "20240101", "estab_1", "other"),
        ("1737540", "20230802", "estab_1", "other"),
        ("1737540", "20240101", "28208", "other"),
        ("worker_1", "20230802", "estab_1", "other"),
        ("worker_1", "20230802", "28208", "other"),
        ("worker_1", "20240101", "28208", "other"),
    ]
    worker_data_with_single_row_to_remove = [
        *expected_worker_data,
        ("1737540", "20230802", "28208", "other"),
    ]
    worker_data_with_multiple_rows_to_remove = [
        *expected_worker_data,
        ("1737540", "20230802", "28208", "other"),
        ("1737540", "20230802", "28208", "something else"),
    ]

    worker_data_without_rows_to_remove = expected_worker_data

    workplace_data_with_duplicates_rows = [
        ("20250101", "48904", "other_data"),
        ("20250101", "49966", "other_data"),
        ("20250101", "49967", "other_data"),
        ("20250101", "49968", "other_data"),
        ("20250101", "50538", "other_data"),
        ("20250101", "50561", "other_data"),
        ("20250101", "50590", "other_data"),
        ("20250101", "50596", "other_data"),
        ("20250101", "12345", "other_data"),
        ("20250101", "50598", "other_data"),
        ("20250101", "50621", "other_data"),
        ("20250101", "50623", "other_data"),
        ("20250101", "50624", "other_data"),
        ("20250101", "50627", "other_data"),
        ("20250101", "50629", "other_data"),
        ("20250101", "50639", "other_data"),
        ("20250101", "50640", "other_data"),
        ("20250101", "50767", "other_data"),
        ("20250101", "50769", "other_data"),
        ("20250101", "50770", "other_data"),
        ("20250101", "50771", "other_data"),
        ("20250101", "50869", "other_data"),
        ("20250101", "50870", "other_data"),
        ("20250101", "67890", "other_data"),
    ]
    expected_workplace_data_with_duplicates_rows = [
        ("20250101", "12345", "other_data"),
        ("20250101", "67890", "other_data"),
    ]

    locations_data_with_multiple_rows_to_remove = [
        ("loc_1", "other"),
        (RecordsToRemoveInLocationsData.dental_practice, "other"),
        (RecordsToRemoveInLocationsData.dental_practice, "something else"),
        (RecordsToRemoveInLocationsData.temp_registration, "other"),
        (RecordsToRemoveInLocationsData.temp_registration, "something else"),
    ]

    locations_data_with_single_rows_to_remove = [
        ("loc_1", "other"),
        (RecordsToRemoveInLocationsData.dental_practice, "other"),
        (RecordsToRemoveInLocationsData.temp_registration, "other"),
    ]

    locations_data_without_rows_to_remove = [
        ("loc_1", "other"),
    ]

    expected_locations_data = locations_data_without_rows_to_remove
