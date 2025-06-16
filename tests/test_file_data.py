from dataclasses import dataclass
from datetime import date

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_columns_by_dataset import (
    DiagnosticOnKnownFilledPostsCategoricalValues as CatValues,
)
from utils.column_values.categorical_column_values import (
    AscwdsFilteringRule,
    CareHome,
    EstimateFilledPostsSource,
    MainJobRoleLabels,
    PrimaryServiceType,
    Sector,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_difference_within_range import (
    ascwds_filled_posts_difference_within_range_source_description,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_return_worker_record_count_if_equal_to_total_staff import (
    ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description,
)
from utils.raw_data_adjustments import RecordsToRemoveInLocationsData
from utils.validation.validation_rule_custom_type import CustomValidationRules
from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.column_values.categorical_column_values import MainJobRoleLabels


@dataclass
class ExtractRegisteredManagerNamesData:
    extract_registered_manager_rows = [
        (
            "1-001",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 1",
                    CQCL.code: "A1",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                        {
                            CQCL.person_family_name: "Surname_1b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
        (
            "1-002",
            date(2024, 1, 1),
            [
                {CQCL.name: "Activity 2a", CQCL.code: "A2a", CQCL.contacts: []},
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
    ]

    extract_contacts_information_two_activities_one_contact_each_rows = [
        (
            "1-002",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 2a",
                    CQCL.code: "A2a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
    ]
    expected_extract_contacts_information_two_activities_one_contact_each_rows = [
        (
            "1-002",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 2a",
                    CQCL.code: "A2a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            {
                CQCL.person_family_name: "Surname_2a",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
        (
            "1-002",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 2a",
                    CQCL.code: "A2a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            {
                CQCL.person_family_name: "Surname_2b",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]

    extract_contacts_information_one_activity_two_contacts_rows = [
        (
            "1-001",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 1",
                    CQCL.code: "A1",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                        {
                            CQCL.person_family_name: "Surname_1b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        )
    ]
    expected_extract_contacts_information_one_activity_two_contacts_rows = [
        (
            "1-001",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 1",
                    CQCL.code: "A1",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                        {
                            CQCL.person_family_name: "Surname_1b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            {
                CQCL.person_family_name: "Surname_1a",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
        (
            "1-001",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 1",
                    CQCL.code: "A1",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                        {
                            CQCL.person_family_name: "Surname_1b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            {
                CQCL.person_family_name: "Surname_1b",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]

    extract_contacts_information_two_activities_but_only_one_with_contacts_rows = [
        (
            "1-002",
            date(2024, 1, 1),
            [
                {CQCL.name: "Activity 2a", CQCL.code: "A2a", CQCL.contacts: []},
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
    ]
    expected_extract_contacts_information_two_activities_but_only_one_with_contacts_rows = [
        (
            "1-002",
            date(2024, 1, 1),
            [
                {CQCL.name: "Activity 2a", CQCL.code: "A2a", CQCL.contacts: []},
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            {
                CQCL.person_family_name: "Surname_2b",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]

    select_and_create_full_name_rows = [
        (
            "1-001",
            date(2024, 1, 1),
            "Y",
            {
                CQCL.person_family_name: "Surname_1",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
        (
            "1-002",
            date(2024, 1, 1),
            "Y",
            {
                CQCL.person_family_name: "Surname_2",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]
    expected_select_and_create_full_name_rows = [
        ("1-001", date(2024, 1, 1), "Name Surname_1"),
        ("1-002", date(2024, 1, 1), "Name Surname_2"),
    ]

    group_and_collect_names_with_duplicate_names_rows = [
        ("1-001", date(2024, 1, 1), "Y", "Name Surname_1"),
        ("1-001", date(2024, 1, 1), "Y", "Name Surname_1"),
        ("1-001", date(2024, 1, 1), "Y", "Name Surname_2"),
    ]
    expected_group_and_collect_names_with_duplicate_names_rows = [
        ("1-001", date(2024, 1, 1), ["Name Surname_2", "Name Surname_1"]),
    ]

    group_and_collect_names_with_different_ids_and_dates_rows = [
        ("1-001", date(2024, 1, 1), "Y", "Name Surname_1"),
        ("1-001", date(2024, 2, 1), "Y", "Name Surname_1"),
        ("1-001", date(2024, 2, 1), "Y", "Name Surname_2"),
        ("1-002", date(2024, 1, 1), "N", "Name Surname_3"),
    ]
    expected_group_and_collect_names_with_different_ids_and_dates_rows = [
        ("1-001", date(2024, 1, 1), ["Name Surname_1"]),
        ("1-001", date(2024, 2, 1), ["Name Surname_2", "Name Surname_1"]),
        ("1-002", date(2024, 1, 1), ["Name Surname_3"]),
    ]

    original_test_rows = [
        ("1-001", date(2024, 1, 1), "Y", 5),
        ("1-001", date(2024, 2, 1), "Y", 5),
        ("1-002", date(2024, 1, 1), "Y", 10),
        ("1-002", date(2024, 2, 1), "Y", 10),
    ]
    registered_manager_names_rows = [
        ("1-001", date(2024, 1, 1), ["Name Surname_1"]),
        ("1-001", date(2024, 2, 1), ["Name Surname_1", "Name Surname_2"]),
        ("1-002", date(2024, 2, 1), ["Name Surname_3"]),
    ]
    expected_join_with_original_rows = [
        ("1-001", date(2024, 1, 1), "Y", 5, ["Name Surname_1"]),
        ("1-001", date(2024, 2, 1), "Y", 5, ["Name Surname_1", "Name Surname_2"]),
        ("1-002", date(2024, 1, 1), "Y", 10, None),
        ("1-002", date(2024, 2, 1), "Y", 10, ["Name Surname_3"]),
    ]


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
    ]
    expected_create_banded_bed_count_column_rows = [
        ("1-001", CareHome.care_home, 1, 1.0),
        ("1-002", CareHome.care_home, 24, 6.0),
        ("1-003", CareHome.care_home, 500, 8.0),
        ("1-004", CareHome.not_care_home, None, None),
    ]


@dataclass
class LmEngagementUtilsData:
    # fmt: off
    add_columns_for_locality_manager_dashboard_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024"), # in ascwds on both dates
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024"),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024"), # joins ascwds
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024"),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024"), # leaves ascwds
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024"),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024"), # multiple locations in one cssr
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024"),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024"),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024"),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024"),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024"),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024"),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024"),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024"),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024"),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024"),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024"),
    ]
    expected_add_columns_for_locality_manager_dashboard_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0, None, 1, 1, 1),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0, 0.0, 0, 0, 1),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0, 1.0, 1, 1, 1),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0, None, 1, 1, 1),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0, -1.0, -1, 0, 1),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
    ]

    expected_calculate_la_coverage_monthly_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
    ]

    calculate_coverage_monthly_change_rows = expected_calculate_la_coverage_monthly_rows

    expected_calculate_coverage_monthly_change_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0, None),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0, 0.0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0, None),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0, 1.0),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0, None),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0, -1.0),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75, 0.75),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
    ]
    calculate_locations_monthly_change_rows = expected_calculate_coverage_monthly_change_rows

    expected_calculate_locations_monthly_change_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0, None, 0, 1),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0, 0.0, 1, 0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0, None, 0, 0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0, 1.0, 0, 1),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0, None, 0, 1),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0, -1.0, 1, -1),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 0, 3),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 0, 3),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 0, 3),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75, 0.75, 0, 3),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 0, 1),
    ]

    calculate_new_registrations_rows = expected_calculate_locations_monthly_change_rows

    expected_calculate_new_registrations_rows = expected_add_columns_for_locality_manager_dashboard_rows
    # fmt: on


@dataclass
class IndCQCDataUtils:
    input_rows_for_adding_estimate_filled_posts_and_source = [
        ("1-000001", 10.0, None, 80.0),
        ("1-000002", None, 30.0, 50.0),
        ("1-000003", 20.0, 70.0, 60.0),
        ("1-000004", None, None, 40.0),
        ("1-000005", None, 0.5, 40.0),
        ("1-000006", -1.0, 10.0, 30.0),
    ]

    expected_rows_with_estimate_filled_posts_and_source = [
        ("1-000001", 10.0, None, 80.0, 10.0, "model_name_1"),
        ("1-000002", None, 30.0, 50.0, 30.0, "model_name_2"),
        ("1-000003", 20.0, 70.0, 60.0, 20.0, "model_name_1"),
        ("1-000004", None, None, 40.0, 40.0, "model_name_3"),
        ("1-000005", None, 0.5, 40.0, 40.0, "model_name_3"),
        ("1-000006", -1.0, 10.0, 30.0, 10.0, "model_name_2"),
    ]

    merge_columns_in_order_when_df_has_columns_of_multiple_datatypes = [
        (
            "1-000001",
            10.0,
            {
                MainJobRoleLabels.care_worker: 0.5,
                MainJobRoleLabels.registered_nurse: 0.5,
            },
        )
    ]

    merge_columns_in_order_when_columns_are_datatype_string = [
        ("1-000001", "string", "string")
    ]

    list_of_map_columns_to_be_merged = [
        IndCQC.ascwds_job_role_ratios,
        IndCQC.ascwds_job_role_rolling_ratio,
    ]

    # fmt: off
    merge_map_columns_in_order_when_only_ascwds_known = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         None)
    ]
    # fmt: on

    # fmt: off
    expected_merge_map_columns_in_order_when_only_ascwds_known = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         None,
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         IndCQC.ascwds_job_role_ratios)
    ]
    # fmt: on

    # fmt: off
    merge_map_columns_in_order_when_only_primary_service_known = [
        ("1-001",
         None,
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4})
    ]
    # fmt: on

    # fmt: off
    expected_merge_map_columns_in_order_when_only_primary_service_known = [
        ("1-001",
         None,
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4},
         IndCQC.ascwds_job_role_rolling_ratio)
    ]
    # fmt: on

    # fmt: off
    merge_map_columns_in_order_when_both_map_columns_populated = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4})
    ]
    # fmt: on

    # fmt: off
    expected_merge_map_columns_in_order_when_both_map_columns_populated = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4},
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         IndCQC.ascwds_job_role_ratios)
    ]
    # fmt: on

    merge_map_columns_in_order_when_both_null = [("1-001", None, None)]
    expected_merge_map_columns_in_order_when_both_null = [
        ("1-001", None, None, None, None)
    ]
    # fmt: on

    # fmt: off
    merge_map_columns_in_order_when_both_map_columns_populated_at_multiple_locations = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4}),
        ("1-002",
         {MainJobRoleLabels.care_worker: 0.7, MainJobRoleLabels.registered_nurse: 0.3},
         {MainJobRoleLabels.care_worker: 0.8, MainJobRoleLabels.registered_nurse: 0.2})
    ]
    # fmt: on

    # fmt: off
    expected_merge_map_columns_in_order_when_both_map_columns_populated_at_multiple_locations = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4},
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         IndCQC.ascwds_job_role_ratios),
        ("1-002",
         {MainJobRoleLabels.care_worker: 0.7, MainJobRoleLabels.registered_nurse: 0.3},
         {MainJobRoleLabels.care_worker: 0.8, MainJobRoleLabels.registered_nurse: 0.2},
         {MainJobRoleLabels.care_worker: 0.7, MainJobRoleLabels.registered_nurse: 0.3},
         IndCQC.ascwds_job_role_ratios)
    ]
    # fmt: on

    source_missing_rows = [
        ("1-000001", 8.0, None),
        ("1-000002", None, None),
        ("1-000003", 4.0, "already_populated"),
    ]

    expected_source_added_rows = [
        ("1-000001", 8.0, "model_name"),
        ("1-000002", None, None),
        ("1-000003", 4.0, "already_populated"),
    ]

    test_first_selection_rows = [
        ("loc 1", 1, None, 100.0),
        ("loc 1", 2, 2.0, 50.0),
        ("loc 1", 3, 3.0, 25.0),
    ]
    expected_test_first_selection_rows = [
        ("loc 1", 1, None, 100.0, 50.0),
        ("loc 1", 2, 2.0, 50.0, 50.0),
        ("loc 1", 3, 3.0, 25.0, 50.0),
    ]
    test_last_selection_rows = [
        ("loc 1", 1, 1.0, 100.0),
        ("loc 1", 2, 2.0, 50.0),
        ("loc 1", 3, None, 25.0),
    ]
    expected_test_last_selection_rows = [
        ("loc 1", 1, 1.0, 100.0, 50.0),
        ("loc 1", 2, 2.0, 50.0, 50.0),
        ("loc 1", 3, None, 25.0, 50.0),
    ]


@dataclass
class CalculateAscwdsFilledPostsData:
    # fmt: off
    calculate_ascwds_filled_posts_rows = [
        # Both 0: Return None
        ("1-000001", 0, None, None, None,),
        # Both 500: Return 500
        ("1-000002", 500, 500, None, None,),
        # Only know total_staff: Return None
        ("1-000003", 10, None, None, None,),
        # worker_record_count below min permitted: return None
        ("1-000004", 23, 1, None, None,),
        # Only know worker_records: None
        ("1-000005", None, 100, None, None,),
        # None of the rules apply: Return None
        ("1-000006", 900, 600, None, None,),
        # Absolute difference is within absolute bounds: Return Average
        ("1-000007", 12, 11, None, None,),
        # Absolute difference is within percentage bounds: Return Average
        ("1-000008", 500, 475, None, None,),
        # Already populated, shouldn't change it
        ("1-000009", 10, 10, 8.0, "already populated"),
    ]
    # fmt: on

    # fmt: off
    expected_ascwds_filled_posts_rows = [
        # Both 0: Return None
        ("1-000001", 0, None, None, None,),
        # Both 500: Return 500
        ("1-000002", 500, 500, 500.0, ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description,),
        # Only know total_staff: Return None
        ("1-000003", 10, None, None, None,),
        # worker_record_count below min permitted: return None
        ("1-000004", 23, 1, None, None,),
        # Only know worker_records: Return None
        ("1-000005", None, 100, None, None,),
        # None of the rules apply: Return None
        ("1-000006", 900, 600, None, None,),
        # Absolute difference is within absolute bounds: Return Average
        ("1-000007", 12, 11, 11.5, ascwds_filled_posts_difference_within_range_source_description,),
        # Absolute difference is within percentage bounds: Return Average
        ("1-000008", 500, 475, 487.5, ascwds_filled_posts_difference_within_range_source_description,),
        # Already populated, shouldn't change it
        ("1-000009", 10, 10, 10.0, ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description),
    ]
    # fmt: on


@dataclass
class CalculateAscwdsFilledPostsTotalStaffEqualWorkerRecordsData:
    # fmt: off
    calculate_ascwds_filled_posts_rows = [
        # Both 0: Return None
        ("1-000001", 0, None, None, None,),
        # Both 500: Return 500
        ("1-000002", 500, 500, None, None,),
        # Only know total_staff: Return None
        ("1-000003", 10, None, None, None,),
        # worker_record_count below min permitted: return None
        ("1-000004", 23, 1, None, None,),
        # Only know worker_records: None
        ("1-000005", None, 100, None, None,),
        # None of the rules apply: Return None
        ("1-000006", 900, 600, None, None,),
        # Absolute difference is within absolute bounds: Return Average
        ("1-000007", 12, 11, None, None,),
        # Absolute difference is within percentage bounds: Return Average
        ("1-000008", 500, 475, None, None,),
        # Already populated, shouldn't change it
        ("1-000009", 10, 10, 8.0, "already populated"),
    ]
    # fmt: on


@dataclass
class CalculateAscwdsFilledPostsDifferenceInRangeData:
    # fmt: off
    calculate_ascwds_filled_posts_rows = [
        # Both 0: Return None
        ("1-000001", 0, None, None, None,),
        # Both 500: Return 500
        ("1-000002", 500, 500, None, None,),
        # Only know total_staff: Return None
        ("1-000003", 10, None, None, None,),
        # worker_record_count below min permitted: return None
        ("1-000004", 23, 1, None, None,),
        # Only know worker_records: None
        ("1-000005", None, 100, None, None,),
        # None of the rules apply: Return None
        ("1-000006", 900, 600, None, None,),
        # Absolute difference is within absolute bounds: Return Average
        ("1-000007", 12, 11, None, None,),
        # Absolute difference is within percentage bounds: Return Average
        ("1-000008", 500, 475, None, None,),
        # Already populated, shouldn't change it
        ("1-000009", 10, 10, 8.0, "already populated"),
    ]
    # fmt: on

    # fmt: off
    expected_ascwds_filled_posts_rows = [
        # Both 0: Return None
        ("1-000001", 0, None, None, None,),
        # Both 500: Return 500
        ("1-000002", 500, 500, 500.0, ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description,),
        # Only know total_staff: Return None
        ("1-000003", 10, None, None, None,),
        # worker_record_count below min permitted: return None
        ("1-000004", 23, 1, None, None,),
        # Only know worker_records: Return None
        ("1-000005", None, 100, None, None,),
        # None of the rules apply: Return None
        ("1-000006", 900, 600, None, None,),
        # Absolute difference is within absolute bounds: Return Average
        ("1-000007", 12, 11, 11.5, ascwds_filled_posts_difference_within_range_source_description,),
        # Absolute difference is within percentage bounds: Return Average
        ("1-000008", 500, 475, 487.5, ascwds_filled_posts_difference_within_range_source_description,),
        # Already populated, shouldn't change it
        ("1-000009", 10, 10, 10.0, ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description),
    ]
    # fmt: on


@dataclass
class CleanAscwdsFilledPostOutliersData:
    # fmt: off
    unfiltered_ind_cqc_rows = [
        ("01", "prov 1", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 30.0),
        ("02", "prov 1", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 35.0),
        ("03", "prov 1", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, None, 8.0),
    ]
    # fmt: on


@dataclass
class WinsorizeCareHomeFilledPostsPerBedRatioOutliersData:
    # fmt: off
    unfiltered_ind_cqc_rows = [
        ("01", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 2.0, 2.0, 2.0, 0.04, AscwdsFilteringRule.populated),
        ("02", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 4.0, 4.0, 4.0, 0.08, AscwdsFilteringRule.populated),
        ("03", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 6.0, 6.0, 6.0, 0.12, AscwdsFilteringRule.populated),
        ("04", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 8.0, 8.0, 8.0, 0.16, AscwdsFilteringRule.populated),
        ("05", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 10.0, 10.0, 10.0, 0.2, AscwdsFilteringRule.populated),
        ("06", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 15.0, 15.0, 15.0, 0.3, AscwdsFilteringRule.populated),
        ("07", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 20.0, 20.0, 20.0, 0.4, AscwdsFilteringRule.populated),
        ("08", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 25.0, 25.0, 25.0, 0.2, AscwdsFilteringRule.populated),
        ("09", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 30.0, 30.0, 30.0, 0.6, AscwdsFilteringRule.populated),
        ("10", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 35.0, 35.0, 35.0, 0.7, AscwdsFilteringRule.populated),
        ("11", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 37.0, 37.0, 37.0, 0.74, AscwdsFilteringRule.populated),
        ("12", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 37.5, 37.5, 37.5, 0.75, AscwdsFilteringRule.populated),
        ("13", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 38.0, 38.0, 38.0, 0.76, AscwdsFilteringRule.populated),
        ("14", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 39.0, 39.0, 39.0, 0.78, AscwdsFilteringRule.populated),
        ("15", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 40.0, 40.0, 40.0, 0.8, AscwdsFilteringRule.populated),
        ("16", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 41.0, 41.0, 41.0, 0.82, AscwdsFilteringRule.populated),
        ("17", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 42.0, 42.0, 42.0, 0.84, AscwdsFilteringRule.populated),
        ("18", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 43.0, 43.0, 43.0, 0.86, AscwdsFilteringRule.populated),
        ("19", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 44.0, 44.0, 44.0, 0.88, AscwdsFilteringRule.populated),
        ("20", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 45.0, 45.0, 45.0, 0.9, AscwdsFilteringRule.populated),
        ("21", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 46.0, 46.0, 46.0, 0.92, AscwdsFilteringRule.populated),
        ("22", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 47.0, 47.0, 47.0, 0.94, AscwdsFilteringRule.populated),
        ("23", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 48.0, 48.0, 48.0, 0.96, AscwdsFilteringRule.populated),
        ("24", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 49.0, 49.0, 49.0, 0.98, AscwdsFilteringRule.populated),
        ("25", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 50.0, 50.0, 50.0, 1.0, AscwdsFilteringRule.populated),
        ("26", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 51.0, 51.0, 51.0, 1.02, AscwdsFilteringRule.populated),
        ("27", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 52.0, 52.0, 52.0, 1.04, AscwdsFilteringRule.populated),
        ("28", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 53.0, 53.0, 53.0, 1.06, AscwdsFilteringRule.populated),
        ("29", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 54.0, 54.0, 54.0, 1.08, AscwdsFilteringRule.populated),
        ("30", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 55.0, 55.0, 55.0, 1.10, AscwdsFilteringRule.populated),
        ("31", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 56.0, 56.0, 56.0, 1.12, AscwdsFilteringRule.populated),
        ("32", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 57.0, 57.0, 57.0, 1.14, AscwdsFilteringRule.populated),
        ("33", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 58.0, 58.0, 58.0, 1.16, AscwdsFilteringRule.populated),
        ("34", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 59.0, 59.0, 59.0, 1.18, AscwdsFilteringRule.populated),
        ("35", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 60.0, 60.0, 60.0, 1.20, AscwdsFilteringRule.populated),
        ("36", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 61.0, 61.0, 61.0, 1.22, AscwdsFilteringRule.populated),
        ("37", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 62.0, 62.0, 62.0, 1.24, AscwdsFilteringRule.populated),
        ("38", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 63.0, 63.0, 63.0, 1.26, AscwdsFilteringRule.populated),
        ("39", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 250.0, 250.0, 250.0, 5.0, AscwdsFilteringRule.populated),
        ("40", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 500.0, 500.0, 500.0, 10.0, AscwdsFilteringRule.populated),
        ("41", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 6.0, None, None, None, None, AscwdsFilteringRule.missing_data),
        ("42", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, None, None, 42.0, 42.0, 42.0, None, AscwdsFilteringRule.populated),
        ("43", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, 25, 6.0, 43.0, 43.0, 43.0, 0.92, AscwdsFilteringRule.populated),
        ("44", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, None, None, 44.0, 44.0, 44.0, None, AscwdsFilteringRule.populated),
    ]
    # fmt: on

    # fmt: off
    expected_care_home_jobs_per_bed_ratio_filtered_rows = [
        ("01", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 2.0, 2.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("02", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 4.0, 4.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("03", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 6.0, 6.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("04", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 8.0, 8.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("05", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 10.0, 10.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("06", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 15.0, 15.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("07", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 20.0, 20.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("08", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 25.0, 25.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("09", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 30.0, 30.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("10", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 35.0, 35.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("11", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 37.0, 37.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("12", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 37.5, 37.5, 37.5, 0.75, AscwdsFilteringRule.populated),
        ("13", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 38.0, 38.0, 38.0, 0.76, AscwdsFilteringRule.populated),
        ("14", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 39.0, 39.0, 39.0, 0.78, AscwdsFilteringRule.populated),
        ("15", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 40.0, 40.0, 40.0, 0.8, AscwdsFilteringRule.populated),
        ("16", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 41.0, 41.0, 41.0, 0.82, AscwdsFilteringRule.populated),
        ("17", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 42.0, 42.0, 42.0, 0.84, AscwdsFilteringRule.populated),
        ("18", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 43.0, 43.0, 43.0, 0.86, AscwdsFilteringRule.populated),
        ("19", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 44.0, 44.0, 44.0, 0.88, AscwdsFilteringRule.populated),
        ("20", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 45.0, 45.0, 45.0, 0.9, AscwdsFilteringRule.populated),
        ("21", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 46.0, 46.0, 46.0, 0.92, AscwdsFilteringRule.populated),
        ("22", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 47.0, 47.0, 47.0, 0.94, AscwdsFilteringRule.populated),
        ("23", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 48.0, 48.0, 48.0, 0.96, AscwdsFilteringRule.populated),
        ("24", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 49.0, 49.0, 49.0, 0.98, AscwdsFilteringRule.populated),
        ("25", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 50.0, 50.0, 50.0, 1.0, AscwdsFilteringRule.populated),
        ("26", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 51.0, 51.0, 51.0, 1.02, AscwdsFilteringRule.populated),
        ("27", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 52.0, 52.0, 52.0, 1.04, AscwdsFilteringRule.populated),
        ("28", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 53.0, 53.0, 53.0, 1.06, AscwdsFilteringRule.populated),
        ("29", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 54.0, 54.0, 54.0, 1.08, AscwdsFilteringRule.populated),
        ("30", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 55.0, 55.0, 55.0, 1.10, AscwdsFilteringRule.populated),
        ("31", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 56.0, 56.0, 56.0, 1.12, AscwdsFilteringRule.populated),
        ("32", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 57.0, 57.0, 57.0, 1.14, AscwdsFilteringRule.populated),
        ("33", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 58.0, 58.0, 58.0, 1.16, AscwdsFilteringRule.populated),
        ("34", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 59.0, 59.0, 59.0, 1.18, AscwdsFilteringRule.populated),
        ("35", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 60.0, 60.0, 60.0, 1.20, AscwdsFilteringRule.populated),
        ("36", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 61.0, 61.0, 61.0, 1.22, AscwdsFilteringRule.populated),
        ("37", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 62.0, 62.0, 62.0, 1.24, AscwdsFilteringRule.populated),
        ("38", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 63.0, 63.0, 63.0, 1.26, AscwdsFilteringRule.populated),
        ("39", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 250.0, 250.0, 250.0, 5.0, AscwdsFilteringRule.populated),
        ("40", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 500.0, 500.0, 250.0, 5.0, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("41", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 6.0, None, None, None, None, AscwdsFilteringRule.missing_data),
        ("42", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, None, None, 42.0, 42.0, 42.0, None, AscwdsFilteringRule.populated),
        ("43", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, 25, 6.0, 43.0, 43.0, 43.0, 0.92, AscwdsFilteringRule.populated),
        ("44", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, None, None, 44.0, 44.0, 44.0, None, AscwdsFilteringRule.populated),
    ]
    # fmt: on

    filter_df_to_care_homes_with_known_beds_and_filled_posts_rows = [
        ("01", "Y", None, None),
        ("02", "Y", None, 0.0),
        ("03", "Y", None, 1.0),
        ("04", "Y", 0, None),
        ("05", "Y", 0, 0.0),
        ("06", "Y", 0, 1.0),
        ("07", "Y", 1, None),
        ("08", "Y", 1, 0.0),
        ("09", "Y", 1, 1.0),
        ("10", "N", None, None),
        ("11", "N", None, 0.0),
        ("12", "N", None, 1.0),
        ("13", "N", 0, None),
        ("14", "N", 0, 0.0),
        ("15", "N", 0, 1.0),
        ("16", "N", 1, None),
        ("17", "N", 1, 0.0),
        ("18", "N", 1, 1.0),
    ]

    expected_filtered_df_to_care_homes_with_known_beds_and_filled_posts_rows = [
        ("09", "Y", 1, 1.0),
    ]

    calculate_standardised_residuals_rows = [
        ("1", 55.5, 64.0),
        ("2", 25.0, 16.0),
    ]
    expected_calculate_standardised_residuals_rows = [
        ("1", 55.5, 64.0, -1.0625),
        ("2", 25.0, 16.0, 2.25),
    ]

    standardised_residual_percentile_cutoff_rows = [
        ("1", PrimaryServiceType.care_home_with_nursing, 0.54321),
        ("2", PrimaryServiceType.care_home_with_nursing, -3.2545),
        ("3", PrimaryServiceType.care_home_with_nursing, -4.2542),
        ("4", PrimaryServiceType.care_home_with_nursing, 2.41654),
        ("5", PrimaryServiceType.care_home_with_nursing, 25.0),
        ("6", PrimaryServiceType.care_home_only, 1.0),
        ("7", PrimaryServiceType.care_home_only, 2.0),
        ("8", PrimaryServiceType.care_home_only, 3.0),
    ]

    expected_standardised_residual_percentile_cutoff_with_percentiles_rows = [
        ("1", PrimaryServiceType.care_home_with_nursing, 0.54321, -3.454, 6.933),
        ("2", PrimaryServiceType.care_home_with_nursing, -3.2545, -3.454, 6.933),
        ("3", PrimaryServiceType.care_home_with_nursing, -4.2542, -3.454, 6.933),
        ("4", PrimaryServiceType.care_home_with_nursing, 2.41654, -3.454, 6.933),
        ("5", PrimaryServiceType.care_home_with_nursing, 25.0, -3.454, 6.933),
        ("6", PrimaryServiceType.care_home_only, 1.0, 1.4, 2.6),
        ("7", PrimaryServiceType.care_home_only, 2.0, 1.4, 2.6),
        ("8", PrimaryServiceType.care_home_only, 3.0, 1.4, 2.6),
    ]

    duplicate_ratios_within_standardised_residual_cutoff_rows = [
        ("1", 1.0, -2.50, -1.23, 1.23),
        ("2", 2.0, -1.23, -1.23, 1.23),
        ("3", 3.0, 0.00, -1.23, 1.23),
        ("4", 4.0, 1.23, -1.23, 1.23),
        ("5", 5.0, 1.25, -1.23, 1.23),
    ]
    expected_duplicate_ratios_within_standardised_residual_cutoff_rows = [
        ("1", 1.0, -2.50, -1.23, 1.23, None),
        ("2", 2.0, -1.23, -1.23, 1.23, 2.0),
        ("3", 3.0, 0.00, -1.23, 1.23, 3.0),
        ("4", 4.0, 1.23, -1.23, 1.23, 4.0),
        ("5", 5.0, 1.25, -1.23, 1.23, None),
    ]

    min_and_max_permitted_ratios_rows = [
        ("1", 0.55, 1.0),
        ("2", 5.88, 1.0),
        ("3", None, 1.0),
        ("4", 3.21, 2.0),
        ("5", 4.88, 2.0),
        ("6", None, 2.0),
    ]
    expected_min_and_max_permitted_ratios_rows = [
        ("1", 0.55, 1.0, 0.75, 5.88),
        ("2", 5.88, 1.0, 0.75, 5.88),
        ("3", None, 1.0, 0.75, 5.88),
        ("4", 3.21, 2.0, 3.21, 5.0),
        ("5", 4.88, 2.0, 3.21, 5.0),
        ("6", None, 2.0, 3.21, 5.0),
    ]

    winsorize_outliers_rows = [
        ("1", CareHome.care_home, 9.0, 15, 0.6, 1.0, 5.0),
        ("2", CareHome.care_home, 30.0, 15, 2.0, 1.0, 5.0),
        ("3", CareHome.care_home, 90.0, 15, 6.0, 1.0, 5.0),
    ]
    expected_winsorize_outliers_rows = [
        ("1", CareHome.care_home, 15.0, 15, 1.0, 1.0, 5.0),
        ("2", CareHome.care_home, 30.0, 15, 2.0, 1.0, 5.0),
        ("3", CareHome.care_home, 75.0, 15, 5.0, 1.0, 5.0),
    ]

    set_minimum_permitted_ratio_rows = [
        ("1", 0.05),
        ("2", 2.55),
    ]
    expected_set_minimum_permitted_ratio_rows = [
        ("1", 0.75),
        ("2", 2.55),
    ]

    combine_dataframes_care_home_rows = [
        (
            "01",
            date(2023, 1, 1),
            "Y",
            PrimaryServiceType.care_home_only,
            25,
            6.0,
            1.0,
            1.0,
            None,
            0.04,
            AscwdsFilteringRule.populated,
            10.0,
        ),
        (
            "02",
            date(2023, 1, 1),
            "Y",
            PrimaryServiceType.care_home_only,
            25,
            6.0,
            2.0,
            2.0,
            2.0,
            0.08,
            AscwdsFilteringRule.populated,
            20.0,
        ),
    ]

    combine_dataframes_non_care_home_rows = [
        (
            "03",
            date(2023, 1, 1),
            "N",
            PrimaryServiceType.non_residential,
            None,
            None,
            3.0,
            3.0,
            3.0,
            None,
            AscwdsFilteringRule.populated,
        ),
    ]

    expected_combined_dataframes_rows = [
        (
            "01",
            date(2023, 1, 1),
            "Y",
            PrimaryServiceType.care_home_only,
            25,
            6.0,
            1.0,
            1.0,
            None,
            0.04,
            AscwdsFilteringRule.populated,
        ),
        (
            "02",
            date(2023, 1, 1),
            "Y",
            PrimaryServiceType.care_home_only,
            25,
            6.0,
            2.0,
            2.0,
            2.0,
            0.08,
            AscwdsFilteringRule.populated,
        ),
        (
            "03",
            date(2023, 1, 1),
            "N",
            PrimaryServiceType.non_residential,
            None,
            None,
            3.0,
            3.0,
            3.0,
            None,
            AscwdsFilteringRule.populated,
        ),
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


@dataclass
class DiagnosticsOnKnownFilledPostsData:
    estimate_filled_posts_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            10.0,
            10.0,
            PrimaryServiceType.care_home_only,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            None,
            None,
            None,
            10.0,
            "2024",
            "01",
            "01",
            "20240101",
        ),
    ]


@dataclass
class DiagnosticsOnCapacityTrackerData:
    estimate_filled_posts_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            CareHome.care_home,
            PrimaryServiceType.care_home_only,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            None,
            None,
            None,
            10.0,
            10,
            1.0,
            1704067200,
            date(2024, 1, 1),
            11,
            None,
            None,
            "2024",
            "01",
            "01",
            "20240101",
        ),
        (
            "loc 1",
            date(2024, 2, 2),
            CareHome.care_home,
            PrimaryServiceType.care_home_only,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            None,
            None,
            None,
            10.0,
            10,
            1.0,
            1706832000,
            date(2024, 2, 1),
            11,
            None,
            None,
            "2024",
            "01",
            "01",
            "20240101",
        ),
        (
            "loc 2",
            date(2024, 1, 1),
            CareHome.not_care_home,
            PrimaryServiceType.non_residential,
            10.0,
            None,
            None,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            None,
            1704067200,
            None,
            None,
            date(2024, 1, 1),
            10,
            "2024",
            "01",
            "01",
            "20240101",
        ),
    ]

    convert_to_all_posts_using_ratio_rows = [
        ("loc 1", 1.0),
        ("loc 2", 6.0),
        ("loc 3", None),
    ]
    expected_convert_to_all_posts_using_ratio_rows = [
        ("loc 1", 1.0, 1.25),
        ("loc 2", 6.0, 7.5),
        ("loc 3", None, None),
    ]

    calculate_care_worker_ratio_rows = [
        ("loc 1", 8.0, 10.0),
        ("loc 2", 16.0, 20.0),
        ("loc 3", 24.0, 30.0),
        ("loc 4", None, 40.0),
        ("loc 5", 40.0, None),
        ("loc 6", None, None),
    ]
    expected_care_worker_ratio = 0.8


@dataclass
class DiagnosticsUtilsData:
    filter_to_known_values_rows = [
        (
            "loc 1",
            1.0,
            1.0,
        ),
        (
            "loc 2",
            None,
            1.0,
        ),
        (
            "loc 3",
            2.0,
            None,
        ),
    ]

    expected_filter_to_known_values_rows = [
        (
            "loc 1",
            1.0,
            1.0,
        ),
        (
            "loc 3",
            2.0,
            None,
        ),
    ]
    list_of_models = ["model_type_one", "model_type_two"]
    expected_list_of_models = [
        *CatValues.estimate_filled_posts_source_column_values.categorical_values,
        IndCQC.estimate_filled_posts,
    ]
    restructure_dataframe_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            10.0,
            PrimaryServiceType.care_home_only,
            13.0,
            12.0,
            "2024",
            "01",
            "01",
            "20240101",
        ),
    ]
    expected_restructure_dataframe_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            "model_type_one",
            13.0,
            "2024",
            "01",
            "01",
            "20240101",
        ),
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            "model_type_two",
            12.0,
            "2024",
            "01",
            "01",
            "20240101",
        ),
    ]
    # fmt: off
    calculate_distribution_metrics_rows = [
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0),
    ]
    expected_calculate_distribution_mean_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 15.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 15.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 35.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 35.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 55.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 55.0),
    ]
    expected_calculate_distribution_standard_deviation_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 7.0710678118654755),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 7.0710678118654755),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 7.0710678118654755),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 7.0710678118654755),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 7.0710678118654755),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 7.0710678118654755),
    ]
    expected_calculate_distribution_kurtosis_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, -2.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, -2.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, -2.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, -2.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, -2.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, -2.0),
    ]
    expected_calculate_distribution_skewness_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 0.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 0.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 0.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 0.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 0.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 0.0),
    ]
    expected_calculate_distribution_metrics_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 15.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 15.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 35.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 35.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 55.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 55.0, 7.0710678118654755, -2.0, 0.0),
    ]
    # fmt: on

    calculate_residuals_rows = [
        ("loc 1", 10.0, 5.0),
        ("loc 2", 5.0, 10.0),
    ]
    expected_calculate_residual_rows = [
        ("loc 1", 10.0, 5.0, -5.0),
        ("loc 2", 5.0, 10.0, 5.0),
    ]
    expected_calculate_absolute_residual_rows = [
        ("loc 1", 10.0, 5.0, -5.0, 5.0),
        ("loc 2", 5.0, 10.0, 5.0, 5.0),
    ]
    expected_calculate_percentage_residual_rows = [
        ("loc 1", 10.0, 5.0, -1.0),
        ("loc 2", 5.0, 10.0, 0.5),
    ]
    expected_calculate_standardised_residual_rows = [
        ("loc 1", 10.0, 5.0, -5.0, -1.58113883),
        ("loc 2", 5.0, 10.0, 5.0, 2.23606798),
    ]
    expected_calculate_residuals_rows = [
        ("loc 1", 10.0, 5.0, -5.0, 5.0, -1.0, -1.58113883),
        ("loc 2", 5.0, 10.0, 5.0, 5.0, 0.5, 2.23606798),
    ]
    # fmt: off
    calculate_aggregate_residuals_rows = [
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 10.0, 0.1, 0.9),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, -20.0, 20.0, 0.2, 1.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 30.0, 0.3, 1.1),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, -40.0, 40.0, 0.4, 1.2),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 50.0, 0.5, 1.3),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, -60.0, 60.0, 0.6, 1.4),
    ]
    expected_calculate_aggregate_residuals_rows = [
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 10.0, 0.1, 0.9, 15.0, 0.15, 10.0, -20.0, 0.5, 1.0, 1.0, 1.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, -20.0, 20.0, 0.2, 1.0, 15.0, 0.15, 10.0, -20.0, 0.5, 1.0, 1.0, 1.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 30.0, 0.3, 1.1, 35.0, 0.35, 30.0, -40.0, 0.0, 0.5, 0.0, 0.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, -40.0, 40.0, 0.4, 1.2,  35.0, 0.35, 30.0, -40.0, 0.0, 0.5, 0.0, 0.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 50.0, 0.5, 1.3, 55.0, 0.55, 50.0, -60.0, 0.0, 0.0, 0.0, 0.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, -60.0, 60.0, 0.6, 1.4, 55.0, 0.55, 50.0, -60.0, 0.0, 0.0, 0.0, 0.0),
    ]

    create_summary_dataframe_rows = [
        ("loc 1", date(2024, 1, 1), PrimaryServiceType.care_home_only, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, "2024", "01", "01", "20240101", 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        ("loc 2", date(2024, 1, 1), PrimaryServiceType.care_home_only, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, "2024", "01", "01", "20240101", 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        ("loc 3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, "2024", "01", "01", "20240101", 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        ("loc 4", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, "2024", "01", "01", "20240101", 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        ("loc 5", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.imputed_posts_care_home_model, 100.0, "2024", "01", "01", "20240101", 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
        ("loc 6", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.imputed_posts_care_home_model, 100.0, "2024", "01", "01", "20240101", 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
    ]
    expected_create_summary_dataframe_rows = [
        (PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 1.0, 2.0, 3.0, 4.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        (PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 2.0, 3.0, 4.0, 5.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        (PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 3.0, 4.0, 5.0, 6.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
    ]

    # fmt: on


@dataclass
class ASCWDSFilteringUtilsData:
    add_filtering_column_rows = [
        ("loc 1", 10.0),
        ("loc 2", None),
    ]
    expected_add_filtering_column_rows = [
        ("loc 1", 10.0, AscwdsFilteringRule.populated),
        ("loc 2", None, AscwdsFilteringRule.missing_data),
    ]
    update_filtering_rule_populated_to_nulled_rows = [
        (
            "loc 1",
            10.0,
            10.0,
            AscwdsFilteringRule.populated,
        ),
        (
            "loc 2",
            10.0,
            None,
            AscwdsFilteringRule.populated,
        ),
        (
            "loc 3",
            10.0,
            None,
            AscwdsFilteringRule.missing_data,
        ),
    ]
    update_filtering_rule_populated_to_winsorized_rows = [
        (
            "loc 1",
            10.0,
            9.0,
            AscwdsFilteringRule.populated,
        ),
        (
            "loc 2",
            10.0,
            11.0,
            AscwdsFilteringRule.populated,
        ),
        (
            "loc 3",
            10.0,
            10.0,
            AscwdsFilteringRule.populated,
        ),
    ]
    update_filtering_rule_winsorized_to_nulled_rows = [
        (
            "loc 1",
            10.0,
            9.0,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        ),
        (
            "loc 2",
            10.0,
            None,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        ),
    ]
    expected_update_filtering_rule_populated_to_nulled_rows = [
        (
            "loc 1",
            10.0,
            10.0,
            AscwdsFilteringRule.populated,
        ),
        (
            "loc 2",
            10.0,
            None,
            AscwdsFilteringRule.contained_invalid_missing_data_code,
        ),
        (
            "loc 3",
            10.0,
            None,
            AscwdsFilteringRule.missing_data,
        ),
    ]
    expected_update_filtering_rule_populated_to_winsorized_rows = [
        (
            "loc 1",
            10.0,
            9.0,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        ),
        (
            "loc 2",
            10.0,
            11.0,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        ),
        (
            "loc 3",
            10.0,
            10.0,
            AscwdsFilteringRule.populated,
        ),
    ]
    expected_update_filtering_rule_winsorized_to_nulled_rows = [
        (
            "loc 1",
            10.0,
            9.0,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        ),
        (
            "loc 2",
            10.0,
            None,
            AscwdsFilteringRule.contained_invalid_missing_data_code,
        ),
    ]


@dataclass
class NullFilledPostsUsingInvalidMissingDataCodeData:
    null_filled_posts_using_invalid_missing_data_code_rows = [
        ("loc 1", 20.0, 20.0, AscwdsFilteringRule.populated),
        ("loc 2", 999.0, 999.0, AscwdsFilteringRule.populated),
        ("loc 3", None, None, AscwdsFilteringRule.missing_data),
    ]
    expected_null_filled_posts_using_invalid_missing_data_code_rows = [
        ("loc 1", 20.0, 20.0, AscwdsFilteringRule.populated),
        ("loc 2", 999.0, None, AscwdsFilteringRule.contained_invalid_missing_data_code),
        ("loc 3", None, None, AscwdsFilteringRule.missing_data),
    ]


@dataclass
class NullGroupedProvidersData:
    # fmt: off
    null_grouped_providers_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 13.0, 4, 3.25, AscwdsFilteringRule.populated, 1.0),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", None, None,  None, 4, None, AscwdsFilteringRule.missing_data, 1.0),
        ("loc 3", "prov 1", date(2024, 1, 1), "Y", None, None, None, 4, None, AscwdsFilteringRule.missing_data, 1.0),
        ("loc 1", "prov 1", date(2024, 1, 8), "Y", "estab 1", 12.0, 12.0, 4, 3.0, AscwdsFilteringRule.populated, 1.0),
        ("loc 2", "prov 1", date(2024, 1, 8), "Y", None, None, None, 4, None, AscwdsFilteringRule.missing_data, 1.0),
    ]
    # fmt: on

    # fmt: off
    calculate_data_for_grouped_provider_identification_where_provider_has_one_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", None, 4, 12.0),
        ("loc 2", "prov 2", date(2024, 1, 1), "Y", None, None, 5, None),
        ("loc 3", "prov 3", date(2024, 1, 1), "N", "estab 3", 10.0, None, 15.0),
    ]
    expected_calculate_data_for_grouped_provider_identification_where_provider_has_one_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0, 11.0, 1, 1, 1, 4, 1, 11.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", None, 4, 12.0, 11.0, 1, 1, 0, 4, 1, 11.0),
        ("loc 2", "prov 2", date(2024, 1, 1), "Y", None, None, 5, None, None, 1, 0, 0, 5, 0, None),
        ("loc 3", "prov 3", date(2024, 1, 1), "N", "estab 3", 10.0, None, 15.0, 15.0, 1, 1, 1, None, 1, 15.0),
    ]
    # fmt: on

    # fmt: off
    calculate_data_for_grouped_provider_identification_where_provider_has_multiple_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", 13.0, 4, 20.0),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", "estab 2", 14.0, 3, 15.0),
        ("loc 2", "prov 1", date(2024, 2, 1), "Y", None, None, 5, 25.0),
        ("loc 3", "prov 2", date(2024, 1, 1), "Y", None, None, 6, 10.0),
        ("loc 4", "prov 2", date(2024, 1, 1), "N", "estab 3", None, None, None),
        ("loc 5", "prov 3", date(2024, 1, 1), "N", None, None, None, None),
        ("loc 6", "prov 3", date(2024, 1, 1), "N", None, None, None, None),
    ]
    expected_calculate_data_for_grouped_provider_identification_where_provider_has_multiple_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0, 15.0, 2, 2, 2, 7, 2, 35.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", 13.0, 4, 20.0, 15.0, 2, 1, 1, 9, 2, 35.0),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", "estab 2", 14.0, 3, 15.0, 20.0, 2, 2, 2, 7, 2, 35.0),
        ("loc 2", "prov 1", date(2024, 2, 1), "Y", None, None, 5, 25.0, 20.0, 2, 1, 1, 9, 2, 35.0),
        ("loc 3", "prov 2", date(2024, 1, 1), "Y", None, None, 6, 10.0, 10.0, 2, 1, 0, 6, 1, 10.0),
        ("loc 4", "prov 2", date(2024, 1, 1), "N", "estab 3", None, None, None, None, 2, 1, 0, 6, 1, 10.0),
        ("loc 5", "prov 3", date(2024, 1, 1), "N", None, None, None, None, None, 2, 0, 0, None, 0, None),
        ("loc 6", "prov 3", date(2024, 1, 1), "N", None, None, None, None, None, 2, 0, 0, None, 0, None),
    ]
    # fmt: on

    identify_potential_grouped_providers_rows = [
        ("1", 1, 1, 1),
        ("2", 1, 1, 0),
        ("3", 1, 0, 0),
        ("4", 2, 2, 2),
        ("5", 2, 2, 1),
        ("6", 2, 2, 0),
        ("7", 2, 1, 1),
        ("8", 2, 1, 0),
        ("9", 5, 1, 1),
    ]
    expected_identify_potential_grouped_providers_rows = [
        ("1", 1, 1, 1, False),
        ("2", 1, 1, 0, False),
        ("3", 1, 0, 0, False),
        ("4", 2, 2, 2, False),
        ("5", 2, 2, 1, False),
        ("6", 2, 2, 0, False),
        ("7", 2, 1, 1, True),
        ("8", 2, 1, 0, False),
        ("9", 5, 1, 1, True),
    ]

    # fmt: off
    null_care_home_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.care_home, 25.0, 25.0, 2, 2, 12.5, True, AscwdsFilteringRule.populated),
        ("1-002", CareHome.care_home, 60.0, 60.0, 2, 2, 30.0, True, AscwdsFilteringRule.populated),
    ]
    expected_null_care_home_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.care_home, 25.0, None, 2, 2, None, True, AscwdsFilteringRule.care_home_location_was_grouped_provider),
        ("1-002", CareHome.care_home, 60.0, None, 2, 2, None, True, AscwdsFilteringRule.care_home_location_was_grouped_provider),
    ]
    null_care_home_grouped_providers_where_location_does_not_meet_criteria_rows = [
        ("1-001", CareHome.not_care_home, 25.0, 25.0, None, 2, None, True, AscwdsFilteringRule.populated),  # non res location
        ("1-002", CareHome.care_home, 25.0, 25.0, 2, 3, 12.5, False, AscwdsFilteringRule.populated),  # not identified as potential grouped provider
        ("1-003", CareHome.care_home, 24.0, 24.0, 2, 2, 12.0, True, AscwdsFilteringRule.populated),  # below minimum size
        ("1-004", CareHome.care_home, 25.0, 25.0, 20, 22, 1.25, True, AscwdsFilteringRule.populated), # below location and provider threshold
    ]
    # fmt: on

    # fmt: off
    null_non_res_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.not_care_home, True, 50.0, 50.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),
        ("1-002", CareHome.not_care_home, True, None, None, 15.0, 2, 25.0, None),
    ]
    expected_null_non_res_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.not_care_home, True, 50.0, None, 10.0, 2, 25.0, AscwdsFilteringRule.non_res_location_was_grouped_provider),
        ("1-002", CareHome.not_care_home, True, None, None, 15.0, 2, 25.0, None),
    ]
    null_non_res_grouped_providers_when_does_not_meet_criteria_rows = [
        ("1-001", CareHome.care_home, True, 50.0, 50.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),  # care home location
        ("1-002", CareHome.not_care_home, False, 50.0, 50.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),  # not identified as potential grouped provider
        ("1-003", CareHome.not_care_home, True, 49.0, 49.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),  # below minimum size
        ("1-004", CareHome.not_care_home, True, 50.0, 50.0, None, 2, 25.0, AscwdsFilteringRule.populated),  # no PIR data for location
        ("1-005", CareHome.not_care_home, True, 50.0, 50.0, 40.0, 2, 45.0, AscwdsFilteringRule.populated),  # below location threshold and provider sum
        ("1-006", CareHome.not_care_home, True, 50.0, 50.0, 40.0, 1, 40.0, AscwdsFilteringRule.populated),  # below location threshold and provider count
        ("1-008", CareHome.not_care_home, True, 50.0, None, 10.0, 2, 25.0, AscwdsFilteringRule.contained_invalid_missing_data_code),  # already filtered
    ]
    # fmt: on
