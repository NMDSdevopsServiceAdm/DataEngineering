from dataclasses import dataclass
from datetime import date, datetime

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CareHome,
    PrimaryServiceType,
    RegistrationStatus,
    Sector,
    Services,
    Specialisms,
    SpecialistGeneralistOther,
)


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
