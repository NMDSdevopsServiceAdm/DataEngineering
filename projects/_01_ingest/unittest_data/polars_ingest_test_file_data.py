from dataclasses import dataclass
from datetime import date, datetime

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CareHome,
    LocationType,
    RegistrationStatus,
)


@dataclass
class FlattenUtilsData:
    clean_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        (date(2018, 1, 1), date(2023, 7, 1), date(2018, 1, 1)),
        ("20231101", "20240101", "20231101"),
    ]

    expected_clean_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        (date(2018, 1, 1), date(2023, 7, 1), date(2018, 1, 1)),
        ("20231101", "20240101", "20231101"),
        (date(2018, 1, 1), date(2023, 7, 1), date(2018, 1, 1)),
    ]

    time_in_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        (
            datetime(2018, 1, 1, 0, 0, 0),
            datetime(2023, 7, 1, 15, 19, 0),
            datetime(2018, 1, 1),
        ),
        ("20231101", "20240101", "20231101"),
    ]

    expected_time_in_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        (
            datetime(2018, 1, 1, 0, 0, 0),
            datetime(2023, 7, 1, 15, 19, 0),
            datetime(2018, 1, 1),
        ),
        ("20231101", "20240101", "20231101"),
        (
            date(2018, 1, 1),
            date(2023, 7, 1),
            date(2018, 1, 1),
        ),
    ]

    registration_date_after_import_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        (date(2018, 1, 1), date(2023, 7, 1), date(2018, 1, 1)),
        ("20121101", "20240101", "20131101"),
    ]

    expected_registration_date_after_import_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        (date(2018, 1, 1), date(2023, 7, 1), date(2018, 1, 1)),
        ("20121101", "20240101", "20131101"),
        (date(2012, 11, 1), date(2023, 7, 1), date(2013, 11, 1)),
    ]

    registration_date_missing_single_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, date(2023, 7, 1), date(2023, 7, 1)),
        ("20240101", "20240201", "20240301"),
    ]

    expected_registration_date_missing_single_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, date(2023, 7, 1), date(2023, 7, 1)),
        ("20240101", "20240201", "20240301"),
        (date(2023, 7, 1), date(2023, 7, 1), date(2023, 7, 1)),
    ]

    registration_date_missing_multiple_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, date(2023, 8, 1), date(2023, 7, 1)),
        ("20240101", "20240201", "20240301"),
    ]

    expected_registration_date_missing_multiple_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, date(2023, 8, 1), date(2023, 7, 1)),
        ("20240101", "20240201", "20240301"),
        (date(2023, 7, 1), date(2023, 8, 1), date(2023, 7, 1)),
    ]

    registration_date_missing_for_all_loc_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, None, None),
        ("20240201", "20240101", "20240301"),
    ]

    expected_registration_date_missing_for_all_loc_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, None, None),
        ("20240201", "20240101", "20240301"),
        (date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1)),
    ]


@dataclass
class ExtractRegisteredManagerNamesData:
    explode_contacts_information_when_single_contact = [
        ("1-001",),
        (date(2024, 1, 1),),
        (CareHome.care_home,),
        (
            [
                {
                    CQCL.name: "Activity 1",
                    CQCL.code: "A1",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
    ]
    expected_explode_contacts_information_when_single_contact = [
        ("1-001",),
        (date(2024, 1, 1),),
        (
            {
                CQCL.name: "Activity 1",
                CQCL.code: "A1",
                CQCL.contacts: [
                    {
                        CQCL.person_family_name: "Surname",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                ],
            },
        ),
        (
            {
                CQCL.person_family_name: "Surname",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]

    explode_contacts_information_when_multiple_activities = [
        ("1-001", "1-002"),
        (date(2024, 1, 1), date(2024, 1, 1)),
        (CareHome.care_home, CareHome.not_care_home),
        (
            [
                {
                    CQCL.name: "Activity 1a",
                    CQCL.code: "A1a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
                {
                    CQCL.name: "Activity 1b",
                    CQCL.code: "A1b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            [
                {
                    CQCL.name: "Activity 2a",
                    CQCL.code: "A2a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2",
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
                            CQCL.person_family_name: "Surname_2",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
    ]
    expected_explode_contacts_information_when_multiple_activities = [
        ("1-001", "1-001", "1-002", "1-002"),
        (date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1)),
        (
            {
                CQCL.name: "Activity 1a",
                CQCL.code: "A1a",
                CQCL.contacts: [
                    {
                        CQCL.person_family_name: "Surname_1a",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                ],
            },
            {
                CQCL.name: "Activity 1b",
                CQCL.code: "A1b",
                CQCL.contacts: [
                    {
                        CQCL.person_family_name: "Surname_1b",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                ],
            },
            {
                CQCL.name: "Activity 2a",
                CQCL.code: "A2a",
                CQCL.contacts: [
                    {
                        CQCL.person_family_name: "Surname_2",
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
                        CQCL.person_family_name: "Surname_2",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                ],
            },
        ),
        (
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
            {
                CQCL.person_family_name: "Surname_2",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
            {
                CQCL.person_family_name: "Surname_2",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]

    explode_contacts_information_when_multiple_contacts_per_activity = [
        ("1-001", "1-002"),
        (date(2024, 1, 1), date(2024, 1, 1)),
        (CareHome.care_home, CareHome.not_care_home),
        (
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
            [
                {
                    CQCL.name: "Activity 2a",
                    CQCL.code: "A2a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                        {
                            CQCL.person_family_name: "Surname_2",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
    ]
    expected_explode_contacts_information_when_multiple_contacts_per_activity = [
        ("1-001", "1-001", "1-002", "1-002"),
        (date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1)),
        (
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
            {
                CQCL.name: "Activity 2a",
                CQCL.code: "A2a",
                CQCL.contacts: [
                    {
                        CQCL.person_family_name: "Surname_2",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                    {
                        CQCL.person_family_name: "Surname_2",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                ],
            },
            {
                CQCL.name: "Activity 2a",
                CQCL.code: "A2a",
                CQCL.contacts: [
                    {
                        CQCL.person_family_name: "Surname_2",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                    {
                        CQCL.person_family_name: "Surname_2",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                ],
            },
        ),
        (
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
            {
                CQCL.person_family_name: "Surname_2",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
            {
                CQCL.person_family_name: "Surname_2",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]

    explode_contacts_information_when_multiple_activities_and_multple_contacts_per_activity = [
        ("1-001", "1-002"),
        (date(2024, 1, 1), date(2024, 1, 1)),
        (CareHome.care_home, CareHome.not_care_home),
        (
            [
                {
                    CQCL.name: "Activity 1a",
                    CQCL.code: "A1a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
                {
                    CQCL.name: "Activity 1b",
                    CQCL.code: "A1b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            [
                {
                    CQCL.name: "Activity 2a",
                    CQCL.code: "A2a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                        {
                            CQCL.person_family_name: "Surname_2",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
    ]
    expected_explode_contacts_information_when_multiple_activities_and_multple_contacts_per_activity = [
        ("1-001", "1-001", "1-002", "1-002"),
        (date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1)),
        (
            {
                CQCL.name: "Activity 1a",
                CQCL.code: "A1a",
                CQCL.contacts: [
                    {
                        CQCL.person_family_name: "Surname_1a",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                ],
            },
            {
                CQCL.name: "Activity 1b",
                CQCL.code: "A1b",
                CQCL.contacts: [
                    {
                        CQCL.person_family_name: "Surname_1b",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                ],
            },
            {
                CQCL.name: "Activity 2a",
                CQCL.code: "A2a",
                CQCL.contacts: [
                    {
                        CQCL.person_family_name: "Surname_2",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                    {
                        CQCL.person_family_name: "Surname_2",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                ],
            },
            {
                CQCL.name: "Activity 2a",
                CQCL.code: "A2a",
                CQCL.contacts: [
                    {
                        CQCL.person_family_name: "Surname_2",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                    {
                        CQCL.person_family_name: "Surname_2",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                ],
            },
        ),
        (
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
            {
                CQCL.person_family_name: "Surname_2",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
            {
                CQCL.person_family_name: "Surname_2",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]

    explode_contacts_information_when_contains_empty_contacts = [
        ("1-001", "1-002"),
        (date(2024, 1, 1), date(2024, 1, 1)),
        (CareHome.care_home, CareHome.not_care_home),
        (
            [
                {
                    CQCL.name: "Activity 1a",
                    CQCL.code: "A1a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
                {CQCL.name: "Activity 1b", CQCL.code: "A1b", CQCL.contacts: []},
            ],
            [
                {CQCL.name: "Activity 2", CQCL.code: "A2", CQCL.contacts: []},
            ],
        ),
    ]
    expected_explode_contacts_information_when_contains_empty_contacts = [
        ("1-001",),
        (date(2024, 1, 1),),
        (
            {
                CQCL.name: "Activity 1a",
                CQCL.code: "A1a",
                CQCL.contacts: [
                    {
                        CQCL.person_family_name: "Surname",
                        CQCL.person_given_name: "Name",
                        CQCL.person_roles: ["Registered Manager"],
                        CQCL.person_title: "M",
                    },
                ],
            },
        ),
        (
            {
                CQCL.person_family_name: "Surname",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]

    select_and_create_full_name_when_given_and_family_name_both_populated = [
        ("1-001",),
        (date(2024, 1, 1),),
        (CareHome.care_home,),
        (
            {
                CQCL.person_family_name: "Surname",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]
    expected_select_and_create_full_name_when_given_and_family_name_both_populated = [
        ("1-001",),
        (date(2024, 1, 1),),
        ("Name Surname",),
    ]

    select_and_create_full_name_when_given_or_family_name_or_null = [
        ("1-001", "1-002"),
        (date(2024, 1, 1), date(2024, 1, 1)),
        (CareHome.care_home, CareHome.care_home),
        (
            {
                CQCL.person_family_name: None,
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
            {
                CQCL.person_family_name: "Surname",
                CQCL.person_given_name: None,
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]
    expected_select_and_create_full_name_when_given_or_family_name_or_null = [
        ("1-001", "1-002"),
        (date(2024, 1, 1), date(2024, 1, 1)),
        (None, None),
    ]

    select_and_create_full_name_without_contact = [
        ("1-001",),
        (date(2024, 1, 1),),
        (CareHome.care_home,),
        (None,),
    ]
    expected_select_and_create_full_name_without_contact = [
        ("1-001",),
        (date(2024, 1, 1),),
        (None,),
    ]

    add_registered_manager_names_full_lf = [
        ("1-001", "1-001", "1-002", "1-002"),
        (
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 1, 1),
            date(2024, 2, 1),
        ),
        (
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
        ),
    ]

    registered_manager_names_without_duplicates = [
        ("1-001", "1-001", "1-002", "1-002"),
        (
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 1, 1),
            date(2024, 2, 1),
        ),
        (
            "Name Surname_1",
            "Name Surname_2",
            "Name Surname_3",
            "Name Surname_4",
        ),
    ]
    expected_add_registered_manager_names_without_duplicates = [
        ("1-001", "1-001", "1-002", "1-002"),
        (
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 1, 1),
            date(2024, 2, 1),
        ),
        (
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
        ),
        (
            ["Name Surname_1"],
            ["Name Surname_2"],
            ["Name Surname_3"],
            ["Name Surname_4"],
        ),
    ]

    registered_manager_names_with_duplicates = [
        ("1-001", "1-001", "1-001", "1-002", "1-002", "1-002"),
        (
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 2, 1),
            date(2024, 1, 1),
            date(2024, 1, 1),
            date(2024, 2, 1),
        ),
        (
            "Name Surname_1",
            "Name Surname_1",
            "Name Surname_1",
            "Name Surname_2",
            "Name Surname_2",
            "Name Surname_2",
        ),
    ]
    expected_add_registered_manager_names_with_duplicates = [
        ("1-001", "1-001", "1-002", "1-002"),
        (
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 1, 1),
            date(2024, 2, 1),
        ),
        (
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
        ),
        (
            ["Name Surname_1"],
            ["Name Surname_1"],
            ["Name Surname_2"],
            ["Name Surname_2"],
        ),
    ]

    registered_manager_names_with_locations_with_multiple_managers = [
        ("1-001", "1-001", "1-001", "1-002", "1-002", "1-002", "1-002", "1-002"),
        (
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 2, 1),
            date(2024, 1, 1),
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 2, 1),
            date(2024, 2, 1),
        ),
        (
            "Name Surname_1",
            "Name Surname_2",
            "Name Surname_1",
            "Name Surname_3",
            "Name Surname_1",
            "Name Surname_2",
            "Name Surname_3",
            "Name Surname_1",
        ),
    ]
    expected_registered_manager_names_with_locations_with_multiple_managers = [
        ("1-001", "1-001", "1-002", "1-002"),
        (
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 1, 1),
            date(2024, 2, 1),
        ),
        (
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
        ),
        (
            ["Name Surname_1"],
            ["Name Surname_1", "Name Surname_2"],
            ["Name Surname_1", "Name Surname_3"],
            ["Name Surname_1", "Name Surname_2", "Name Surname_3"],
        ),
    ]

    registered_manager_names_with_locations_without_contact_names = [
        ("1-001",),
        (date(2024, 1, 1),),
        ("Name Surname",),
    ]
    expected_registered_manager_names_with_locations_without_contact_names = [
        ("1-001", "1-001", "1-002", "1-002"),
        (
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 1, 1),
            date(2024, 2, 1),
        ),
        (
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
        ),
        (["Name Surname"], None, None, None),
    ]


@dataclass
class PostcodeMatcherTest:
    locations_where_all_match_rows = [
        ("1-001", "1-001", "1-002", "1-002", "1-002", "1-003", "1-004"),
        (
            date(2020, 1, 1),
            date(2025, 1, 1),
            date(2020, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
        ),
        ("name 1", "name 1", "name 2", "name 2", "name 2", "name 3", "name 4"),
        (
            "1 road name",
            "1 road name",
            "2 road name",
            "2 road name",
            "2 road name",
            "3 road name",
            "4 road name",
        ),
        ("AA1 1aa", "AA1 1aa", "AA1 ZAA", "AA1 2AA", "AA1 3AA", "TF7 3QH", "AA1 4ZZ"),
        (
            RegistrationStatus.registered,
            RegistrationStatus.registered,
            RegistrationStatus.registered,
            RegistrationStatus.registered,
            RegistrationStatus.registered,
            RegistrationStatus.registered,
            RegistrationStatus.registered,
        ),
        (
            LocationType.social_care_identifier,
            LocationType.social_care_identifier,
            LocationType.social_care_identifier,
            LocationType.social_care_identifier,
            LocationType.social_care_identifier,
            LocationType.social_care_identifier,
            LocationType.social_care_identifier,
        ),
    ]
    locations_with_unmatched_postcode_rows = [
        ("1-001", "1-001", "1-005"),
        (date(2020, 1, 1), date(2025, 1, 1), date(2025, 1, 1)),
        ("name 1", "name 1", "name 5"),
        ("1 road name", "1 road name", "5 road name"),
        ("AA1 1aa", "AA1 1aa", "AA2 5XX"),
        (
            RegistrationStatus.registered,
            RegistrationStatus.registered,
            RegistrationStatus.registered,
        ),
        (
            LocationType.social_care_identifier,
            LocationType.social_care_identifier,
            LocationType.social_care_identifier,
        ),
    ]
    postcodes_rows = [
        (
            "AA11AA",
            "AA12AA",
            "AA13AA",
            "AA11AA",
            "AA12AA",
            "AA13AA",
            "AA14AA",
            "TF74EH",
        ),
        (
            date(2020, 1, 1),
            date(2020, 1, 1),
            date(2020, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
        ),
        (
            "CSSR 1",
            "CSSR 1",
            "CSSR 1",
            "CSSR 1",
            "CSSR 1",
            "CSSR 1",
            "CSSR 1",
            "CSSR 1",
        ),
        (
            None,
            None,
            None,
            "SubICB 1",
            "SubICB 1",
            "SubICB 1",
            "SubICB 1",
            "SubICB 1",
        ),
        (
            "CCG 1",
            "CCG 1",
            "CCG 1",
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "CSSR 1",
            "CSSR 2",
            "CSSR 3",
            "CSSR 1",
            "CSSR 2",
            "CSSR 3",
            "CSSR 4",
            "CSSR 5",
        ),
        (
            "SubICB 1",
            "SubICB 1",
            "SubICB 1",
            "SubICB 1",
            "SubICB 1",
            "SubICB 1",
            "SubICB 1",
            "SubICB 1",
        ),
    ]

    clean_postcode_column_rows = [
        ("aA11Aa", "AA1 2AA", "aA1 3aA"),
    ]
    expected_clean_postcode_column_when_drop_is_false_rows = [
        ("aA11Aa", "AA1 2AA", "aA1 3aA"),
        ("AA11AA", "AA12AA", "AA13AA"),
    ]
    expected_clean_postcode_column_when_drop_is_true_rows = [
        ("AA11AA", "AA12AA", "AA13AA")
    ]

    join_postcode_data_locations_rows = [
        ("1-001", "1-001", "1-002", "1-002"),
        (date(2020, 1, 1), date(2025, 1, 1), date(2020, 1, 1), date(2025, 1, 1)),
        ("AA11AA", "AA11AA", "AA1ZAA", "AA12AA"),
    ]
    join_postcode_data_postcodes_rows = [
        ("AA11AA", "AA12AA", "AA11AA", "AA12AA"),
        (date(2020, 1, 1), date(2020, 1, 1), date(2025, 1, 1), date(2025, 1, 1)),
        ("CSSR 1", "CSSR 2", "CSSR 1", "CSSR 2"),
    ]
    expected_join_postcode_data_matched_rows = [
        ("1-001", "1-001", "1-002"),
        (date(2020, 1, 1), date(2025, 1, 1), date(2025, 1, 1)),
        ("AA11AA", "AA11AA", "AA12AA"),
        ("CSSR 1", "CSSR 1", "CSSR 2"),
    ]
    expected_join_postcode_data_unmatched_rows = [
        ("1-002",),
        (date(2020, 1, 1),),
        ("AA1ZAA",),
    ]

    first_successful_postcode_unmatched_rows = [
        ("1-001", "1-003", "1-004"),
        (date(2023, 1, 1), date(2025, 1, 1), date(2023, 1, 1)),
        ("AA10AA", "AA13AA", "AA12AA"),
    ]

    first_successful_postcode_matched_rows = [
        ("1-001", "1-001", "1-002", "1-004"),
        (date(2024, 1, 1), date(2025, 1, 1), date(2025, 1, 1), date(2022, 1, 1)),
        ("AA11AB", "AA11AA", "AA12AA", "AA13AA"),
        ("CSSR 2", "CSSR 1", "CSSR 1", "CSSR 1"),
    ]
    expected_get_first_successful_postcode_match_rows = [
        ("1-001", "1-003", "1-004"),
        (date(2023, 1, 1), date(2025, 1, 1), date(2023, 1, 1)),
        ("AA11AB", "AA13AA", "AA13AA"),
    ]

    amend_invalid_postcodes_rows = [
        ("1-001", "1-002", "1-003"),
        ("CH52LY", "AB12CD", None),
    ]
    expected_amend_invalid_postcodes_rows = [
        # 1. amended as per invalid postcode dictionary, 2. not in dictionary, doesn't change, 3. null values should remain as null
        ("1-001", "1-002", "1-003"),
        ("CH16HU", "AB12CD", None),
    ]

    postcode_corrections_dict = {
        "CH52LY": "CH16HU",  # Welsh postcode, replaced with nearest English postcode
        "TF73QH": "TF74EH",  # Incorrectly entered and no other postcodes exist starting 'TF7 3'
    }

    truncate_postcode_rows = [
        ("AA11AA", "AA11AB", "AB1CD", "B1CD"),
        (date(2023, 1, 1), date(2023, 1, 1), date(2023, 1, 1), date(2023, 1, 1)),
    ]
    expected_truncate_postcode_rows = [
        ("AA11AA", "AA11AB", "AB1CD", "B1CD"),
        (date(2023, 1, 1), date(2023, 1, 1), date(2023, 1, 1), date(2023, 1, 1)),
        ("AA11", "AA11", "AB1", "B1"),
    ]

    create_truncated_postcode_df_rows = [
        ("AB12CD", "AB12CE", "AB12CF", "AB12CG", "AB12CG", "AB13CD"),
        (
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 1),
        ),
        ("LA_1", "LA_2", "LA_2", "LA_3", "LA_4", "LA_3"),
        ("CCG_1", "CCG_2", "CCG_2", "CCG_1", "CCG_1", "CCG_3"),
        ("ICB_1", "ICB_2", "ICB_2", "ICB_1", "ICB_1", "ICB_3"),
        ("LA_1", "LA_2", "LA_2", "LA_1", "LA_1", "LA_3"),
        ("ICB_1", "ICB_2", "ICB_2", "ICB_1", "ICB_1", "ICB_3"),
    ]
    expected_create_truncated_postcode_df_rows = [
        (date(2025, 1, 1), "LA_2", "CCG_2", "ICB_2", "LA_2", "ICB_2", "AB12"),
        (date(2025, 1, 1), "LA_3", "CCG_3", "ICB_3", "LA_3", "ICB_3", "AB13"),
    ]

    raise_error_if_unmatched_rows = [
        ("1-001",),
        (date(2025, 1, 1),),
        ("name 1",),
        ("1 road name",),
        ("AB1 2CD",),
    ]

    combine_matched_df1_rows = [
        ("1-001", "1-003"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        ("AA11AA", "AA12AA"),
        ("CSSR 1", "CSSR 1"),
    ]
    combine_matched_df2_rows = [
        ("1-002", "1-004"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        ("ZZ11AA", "ZZ12AA"),
        ("ZZ11", "ZZ12"),
        ("CSSR 2", "CSSR 3"),
    ]
    expected_combine_matched_rows = [
        ("1-001", "1-003", "1-002", "1-004"),
        (date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1)),
        ("AA11AA", "AA12AA", "ZZ11AA", "ZZ12AA"),
        ("CSSR 1", "CSSR 1", "CSSR 2", "CSSR 3"),
        (None, None, "ZZ11", "ZZ12"),
    ]
