from dataclasses import dataclass
from datetime import date

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CareHome,
    LocationType,
    PrimaryServiceType,
    RegistrationStatus,
    RelatedLocation,
    Sector,
    Services,
    Specialisms,
)
from utils.column_values.categorical_column_values import (
    SpecialistGeneralistOther as SpecGenOther,
)


@dataclass
class FlattenUtilsData:
    flatten_struct_fields_empty_struct_row = [
        ("1-001", "20250101", None, None),
    ]
    expected_flatten_struct_fields_empty_struct_row = [
        ("1-001", "20250101", None, None, None, None),
    ]

    field_1 = "field_1"
    field_2 = "field_2"
    flatten_struct_fields_populated_struct_row = [
        (
            "1-001",
            "20250101",
            [
                {field_1: "s1f1a1", field_2: "s1f2a1"},
                {field_1: "s1f1a2", field_2: "s1f2a2"},
            ],
            [{field_1: "s2f1a", field_2: "s2f2a"}],
        ),
        (
            "1-002",
            "20250101",
            [{field_1: "s1f1x", field_2: "s1f2y"}, {field_1: None, field_2: None}],
            [
                {field_1: "s2f1x1", field_2: "s2f2y1"},
                {field_1: "s2f1x2", field_2: "s2f2y2"},
            ],
        ),
    ]
    expected_flatten_struct_fields_populated_struct_row = [
        (
            "1-001",
            "20250101",
            [
                {field_1: "s1f1a1", field_2: "s1f2a1"},
                {field_1: "s1f1a2", field_2: "s1f2a2"},
            ],
            [{field_1: "s2f1a", field_2: "s2f2a"}],
            ["s1f1a1", "s1f1a2"],
            ["s2f2a"],
        ),
        (
            "1-002",
            "20250101",
            [{field_1: "s1f1x", field_2: "s1f2y"}, {field_1: None, field_2: None}],
            [
                {field_1: "s2f1x1", field_2: "s2f2y1"},
                {field_1: "s2f1x2", field_2: "s2f2y2"},
            ],
            ["s1f1x", None],
            ["s2f2y1", "s2f2y2"],
        ),
    ]


@dataclass
class FullFlattenUtilsData:
    load_latest_snapshot = [
        ("1-001", "1-001", "1-002", "1-002"),
        (20250101, 20250201, 20250201, 20250301),
    ]
    load_latest_snapshot_existing_dates = [20250101, 20250201]
    expected_load_latest_snapshot = [
        ("1-001", "1-002"),
        (20250201, 20250201),
    ]

    create_full_snapshot_full_lf = [
        ("1-001", "1-002", "1-003"),
        ("Y", "Y", "Y"),
        (1, 2, 3),
        (20250101, 20250101, 20250101),
    ]
    create_full_snapshot_delta_lf = [
        ("1-002", "1-004"),
        ("Y", "N"),
        (4, None),
        (20250201, 20250201),
    ]
    expected_create_full_snapshot_lf = [
        ("1-001", "1-002", "1-003", "1-004"),
        ("Y", "Y", "Y", "N"),
        (1, 4, 3, None),
        (20250101, 20250201, 20250101, 20250201),
    ]

    apply_partitions = [
        ("1-001", "1-002", "1-003", "1-004"),
        (2025, 2025, 2025, 2025),
        (2, 2, 2, 3),
        (1, 1, 1, 2),
        (20250201, 20250201, 20250201, 20250302),
    ]
    apply_partitions_import_date_int = 20250302
    apply_partitions_import_date_str = "20250302"
    expected_apply_partitions = [
        ("1-001", "1-002", "1-003", "1-004"),
        (2025, 2025, 2025, 2025),
        (3, 3, 3, 3),
        (2, 2, 2, 2),
        (20250302, 20250302, 20250302, 20250302),
    ]


@dataclass
class ExtractRegisteredManagerNamesData:
    explode_contacts_information_when_single_contact = [
        ("1-001",),
        (20240101,),
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
        (20240101,),
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
        (20240101, 20240101),
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
        (20240101, 20240101, 20240101, 20240101),
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
        (20240101, 20240101),
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
        (20240101, 20240101, 20240101, 20240101),
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
        (20240101, 20240101),
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
        (20240101, 20240101, 20240101, 20240101),
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
        (20240101, 20240101),
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
        (20240101,),
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
        (20240101,),
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
        (20240101,),
        ("Name Surname",),
    ]

    select_and_create_full_name_when_given_or_family_name_or_null = [
        ("1-001", "1-002"),
        (20240101, 20240101),
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
        (20240101, 20240101),
        (None, None),
    ]

    select_and_create_full_name_without_contact = [
        ("1-001",),
        (20240101,),
        (CareHome.care_home,),
        (None,),
    ]
    expected_select_and_create_full_name_without_contact = [
        ("1-001",),
        (20240101,),
        (None,),
    ]

    add_registered_manager_names_full_lf = [
        ("1-001", "1-001", "1-002", "1-002"),
        (20240101, 20240201, 20240101, 20240201),
        (
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
        ),
    ]

    registered_manager_names_without_duplicates = [
        ("1-001", "1-001", "1-002", "1-002"),
        (20240101, 20240201, 20240101, 20240201),
        (
            "Name Surname_1",
            "Name Surname_2",
            "Name Surname_3",
            "Name Surname_4",
        ),
    ]
    expected_add_registered_manager_names_without_duplicates = [
        ("1-001", "1-001", "1-002", "1-002"),
        (20240101, 20240201, 20240101, 20240201),
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
        (20240101, 20240201, 20240201, 20240101, 20240101, 20240201),
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
        (20240101, 20240201, 20240101, 20240201),
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
            20240101,
            20240201,
            20240201,
            20240101,
            20240101,
            20240201,
            20240201,
            20240201,
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
        (20240101, 20240201, 20240101, 20240201),
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
        (20240101,),
        ("Name Surname",),
    ]
    expected_registered_manager_names_with_locations_without_contact_names = [
        ("1-001", "1-001", "1-002", "1-002"),
        (20240101, 20240201, 20240101, 20240201),
        (
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
            CareHome.care_home,
        ),
        (["Name Surname"], None, None, None),
    ]


@dataclass
class LocationsCleanUtilsData:
    # fmt: off
    save_latest_full_snapshot_rows  = [
        ("1-001", "1-901", date(2025, 1, 1), RegistrationStatus.registered, None),
        ("1-001", "1-901", date(2025, 1, 1), RegistrationStatus.deregistered, date(2025, 1, 1)),
        ("1-001", "1-901", date(2025, 2, 1), RegistrationStatus.registered, None),
        ("1-001", "1-901", date(2025, 2, 1), RegistrationStatus.deregistered, date(2025, 1, 1)),
    ]
    expected_save_latest_full_snapshot_rows = [
        (date(2025, 2, 1), "1-001", RegistrationStatus.registered, None),
        (date(2025, 2, 1), "1-001", RegistrationStatus.deregistered, date(2025, 1, 1)),
    ]
    # fmt: on

    clean_provider_id_column_rows = [
        ("1-001", "1-001"),
        ("1-123456789", "1-123"),
        ("20240101", "20240201"),
    ]

    long_provider_id_column_rows = [
        ("1-001",),
        ("1-223456789 1-223456789",),
        ("20240101",),
    ]
    expected_long_provider_id_column_rows = [
        ("1-001",),
        (None,),
        ("20240101",),
    ]

    impute_missing_values_single_col_rows = [
        ("1-001", "1-001", "1-001", "1-001"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1), date(2024, 4, 1)),
        (None, None, "Prov ID", None),
        (None, None, None, None),
    ]
    expected_impute_missing_values_single_col_rows = [
        ("1-001", "1-001", "1-001", "1-001"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1), date(2024, 4, 1)),
        ("Prov ID", "Prov ID", "Prov ID", "Prov ID"),
        (None, None, None, None),
    ]

    impute_missing_values_multiple_cols_rows = [
        ("1-001", "1-001", "1-001"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)),
        (None, "Prov ID", None),
        (
            ["Service 1", "Service 2"],
            None,
            None,
        ),
    ]
    expected_impute_missing_values_multiple_cols_rows = [
        ("1-001", "1-001", "1-001"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)),
        ("Prov ID", "Prov ID", "Prov ID"),
        (
            ["Service 1", "Service 2"],
            ["Service 1", "Service 2"],
            ["Service 1", "Service 2"],
        ),
    ]

    impute_missing_values_imputation_partitions_rows = [
        ("1-001", "1-001", "1-002", "1-002"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 1, 1), date(2024, 2, 1)),
        (None, "1-101", "1-102", None),
        (None, None, None, None),
    ]
    expected_impute_missing_values_imputation_partitions_rows = [
        ("1-001", "1-001", "1-002", "1-002"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 1, 1), date(2024, 2, 1)),
        ("1-101", "1-101", "1-102", "1-102"),
        (None, None, None, None),
    ]

    impute_missing_values_out_of_order_dates_rows = [
        ("1-001", "1-001", "1-001", "1-001"),
        (date(2024, 3, 1), date(2024, 2, 1), date(2024, 4, 1), date(2024, 1, 1)),
        ("1-103", "1-102", None, None),
        (None, None, None, None),
    ]
    expected_impute_missing_values_out_of_order_dates_rows = [
        ("1-001", "1-001", "1-001", "1-001"),
        (date(2024, 3, 1), date(2024, 2, 1), date(2024, 4, 1), date(2024, 1, 1)),
        ("1-103", "1-102", "1-103", "1-102"),
        (None, None, None, None),
    ]

    impute_missing_values_fully_null_rows = [
        ("1-001", "1-001"),
        (date(2024, 1, 1), date(2024, 2, 1)),
        (None, None),
        (None, None),
    ]
    expected_impute_missing_values_fully_null_rows = [
        ("1-001", "1-001"),
        (date(2024, 1, 1), date(2024, 2, 1)),
        (None, None),
        (None, None),
    ]

    impute_missing_values_multiple_partitions_and_missing_data_rows = [
        ("1-001", "1-001", "1-002", "1-002"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 2, 1), date(2024, 1, 1)),
        ("1-101", None, None, "1-102"),
        (None, ["Service 1", "Service 2"], None, None),
    ]
    expected_impute_missing_values_multiple_partitions_and_missing_data_rows = [
        ("1-001", "1-001", "1-002", "1-002"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 2, 1), date(2024, 1, 1)),
        ("1-101", "1-101", "1-102", "1-102"),
        (["Service 1", "Service 2"], ["Service 1", "Service 2"], None, None),
    ]

    impute_missing_values_overwrites_empty_lists_rows = [
        ("1-001", "1-001", "1-001", "1-001", "1-001"),
        (
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 3, 1),
            date(2024, 4, 1),
            date(2024, 5, 1),
        ),
        ("1-101", "1-101", "1-101", "1-101", "1-101"),
        ([], ["Service 1"], [], ["Service 2"], None),
    ]
    expected_impute_missing_values_overwrites_empty_lists_rows = [
        ("1-001", "1-001", "1-001", "1-001", "1-001"),
        (
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 3, 1),
            date(2024, 4, 1),
            date(2024, 5, 1),
        ),
        ("1-101", "1-101", "1-101", "1-101", "1-101"),
        (["Service 1"], ["Service 1"], ["Service 1"], ["Service 2"], ["Service 2"]),
    ]

    assign_cqc_sector = [
        ("1-001", "1-002"),
        ("1-0001", "1-0002"),
    ]
    expected_assign_cqc_sector_local_authority = [
        ("1-001", "1-002"),
        ("1-0001", "1-0002"),
        (Sector.local_authority, Sector.local_authority),
    ]
    expected_assign_cqc_sector_independent = [
        ("1-001", "1-002"),
        ("1-0001", "1-0002"),
        (Sector.independent, Sector.independent),
    ]

    # fmt: off
    primary_service_type_rows = [
        ("1-001", "1-0001", [Services.domiciliary_care_service,],),
        ("1-002", "1-0002", [Services.care_home_service_with_nursing,],),
        ("1-003", "1-0003", [Services.care_home_service_without_nursing,],),
        ("1-004", "1-0004", [Services.care_home_service_with_nursing, Services.care_home_service_without_nursing,],),
        ("1-005", "1-0005", [Services.care_home_service_without_nursing, "Fake service",],),
        ("1-006", "1-0006", [Services.care_home_service_with_nursing, Services.domiciliary_care_service,],),
        ("1-007", "1-0007", [Services.care_home_service_without_nursing, Services.care_home_service_with_nursing,],),
        ("1-008", "1-0008", [Services.care_home_service_without_nursing, Services.domiciliary_care_service,],),
        ("1-009", "1-0009", [Services.domiciliary_care_service, Services.care_home_service_without_nursing,],),
        ("1-010", "1-0010", [Services.domiciliary_care_service, Services.care_home_service_with_nursing,],),
    ]
    expected_primary_service_type_rows = [
        ("1-001", "1-0001", [Services.domiciliary_care_service,], PrimaryServiceType.non_residential,),
        ("1-002", "1-0002", [Services.care_home_service_with_nursing,], PrimaryServiceType.care_home_with_nursing,),
        ("1-003", "1-0003", [Services.care_home_service_without_nursing,], PrimaryServiceType.care_home_only,),
        ("1-004", "1-0004", [Services.care_home_service_with_nursing, Services.care_home_service_without_nursing,], PrimaryServiceType.care_home_with_nursing,),
        ("1-005", "1-0005", [Services.care_home_service_without_nursing, "Fake service",], PrimaryServiceType.care_home_only,),
        ("1-006", "1-0006", [Services.care_home_service_with_nursing, Services.domiciliary_care_service,], PrimaryServiceType.care_home_with_nursing,),
        ("1-007", "1-0007", [Services.care_home_service_without_nursing, Services.care_home_service_with_nursing,], PrimaryServiceType.care_home_with_nursing,),
        ("1-008", "1-0008", [Services.care_home_service_without_nursing, Services.domiciliary_care_service,], PrimaryServiceType.care_home_only,),
        ("1-009", "1-0009", [Services.domiciliary_care_service, Services.care_home_service_without_nursing,], PrimaryServiceType.care_home_only,),
        ("1-010", "1-0010", [Services.domiciliary_care_service, Services.care_home_service_with_nursing,], PrimaryServiceType.care_home_with_nursing,),
    ]

    realign_carehome_column_rows = [
        ("1-001", CareHome.care_home, PrimaryServiceType.care_home_only,),
        ("1-002", CareHome.not_care_home, PrimaryServiceType.care_home_only,),
        ("1-003", CareHome.care_home, PrimaryServiceType.care_home_with_nursing,),
        ("1-004", CareHome.not_care_home, PrimaryServiceType.care_home_with_nursing,),
        ("1-005", CareHome.care_home, PrimaryServiceType.non_residential,),
        ("1-006", CareHome.not_care_home, PrimaryServiceType.non_residential,),
    ]
    expected_realign_carehome_column_rows = [
        ("1-001", CareHome.care_home, PrimaryServiceType.care_home_only,),
        ("1-002", CareHome.care_home, PrimaryServiceType.care_home_only,),
        ("1-003", CareHome.care_home, PrimaryServiceType.care_home_with_nursing,),
        ("1-004", CareHome.care_home, PrimaryServiceType.care_home_with_nursing,),
        ("1-005", CareHome.not_care_home, PrimaryServiceType.non_residential,),
        ("1-006", CareHome.not_care_home, PrimaryServiceType.non_residential,),
    ]


    add_related_location_column_rows = [
        ("1-001", "1-002", "1-003", "1-004"),
        (None, [], ["HSCA Predecessor",], ["HSCA Predecessor", "Anything else"],),
    ]
    expected_add_related_location_column_rows = add_related_location_column_rows + [
        (
            RelatedLocation.no_related_location,
            RelatedLocation.no_related_location,
            RelatedLocation.has_related_location,
            RelatedLocation.has_related_location,
        ),
    ]
    # fmt: on

    clean_and_impute_registration_date_when_reg_date_before_or_equal_to_import_date_rows = [
        ("1-001", "1-002"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        (date(2025, 1, 1), date(2025, 1, 2)),
    ]
    expected_clean_and_impute_registration_date_when_reg_date_before_or_equal_to_import_date_rows = (
        clean_and_impute_registration_date_when_reg_date_before_or_equal_to_import_date_rows
        + [
            (date(2025, 1, 1), date(2025, 1, 1)),
        ]
    )
    clean_and_impute_registration_date_when_reg_date_null_rows = [
        ("1-001",),
        (None,),
        (date(2025, 1, 1),),
    ]
    expected_clean_and_impute_registration_date_when_reg_date_null_rows = (
        clean_and_impute_registration_date_when_reg_date_null_rows
        + [
            (date(2025, 1, 1),),
        ]
    )
    clean_and_impute_registration_date_when_reg_date_after_import_date_and_only_one_reg_date_rows = [
        ("1-001",),
        (date(2025, 1, 2),),
        (date(2025, 1, 1),),
    ]
    expected_clean_and_impute_registration_date_when_reg_date_after_import_date_and_only_one_reg_date_rows = (
        clean_and_impute_registration_date_when_reg_date_after_import_date_and_only_one_reg_date_rows
        + [
            (date(2025, 1, 1),),
        ]
    )
    clean_and_impute_registration_date_when_reg_date_after_import_date_and_has_other_acceptable_reg_date_rows = [
        ("1-001", "1-001"),
        (date(2025, 1, 5), date(2025, 1, 1)),
        (date(2025, 1, 4), date(2025, 1, 3)),
    ]
    expected_clean_and_impute_registration_date_when_reg_date_after_import_date_and_has_other_acceptable_reg_date_rows = (
        clean_and_impute_registration_date_when_reg_date_after_import_date_and_has_other_acceptable_reg_date_rows
        + [
            (date(2025, 1, 1), date(2025, 1, 1)),
        ]
    )
    clean_and_impute_registration_date_when_reg_date_after_import_date_and_import_date_out_of_order_rows = [
        ("1-001", "1-001"),
        (date(2025, 1, 5), date(2025, 1, 6)),
        (date(2025, 1, 4), date(2025, 1, 3)),
    ]
    expected_clean_and_impute_registration_date_when_reg_date_after_import_date_and_import_date_out_of_order_rows = (
        clean_and_impute_registration_date_when_reg_date_after_import_date_and_import_date_out_of_order_rows
        + [
            (date(2025, 1, 3), date(2025, 1, 3)),
        ]
    )
    # fmt: off
    clean_and_impute_registration_date_when_given_mixed_scenarios_rows = [
        ("1-001", "1-002", "1-002", "1-003", "1-003"),
        (
            date(2025, 1, 10), # registration date after import date, no other import dates.
            date(2025, 1, 10), # registration date after import date, has other import dates.
            date(2025, 1, 10), # registration date after import date, has other import dates.
            date(2025, 1, 1), # registration date before import date.
            date(2025, 1, 1) # registration date same as import date.
        ),
        (
            date(2025, 1, 2),
            date(2025, 1, 2),
            date(2025, 1, 11),
            date(2025, 1, 2),
            date(2025, 1, 1)
        ),
    ]
    expected_clean_and_impute_registration_date_when_given_mixed_scenarios_rows = (
        clean_and_impute_registration_date_when_given_mixed_scenarios_rows
        + [
            (
                date(2025, 1, 2),
                date(2025, 1, 2),
                date(2025, 1, 2),
                date(2025, 1, 1),
                date(2025, 1, 1)
            )
        ]
    )
    # fmt: on

    test_list_of_specialisms = [
        Specialisms.dementia,
        Specialisms.learning_disabilities,
        Specialisms.mental_health,
    ]
    classify_specialisms_rows = [
        ("1-001", "1-002", "1-003"),
        (
            [Specialisms.dementia],
            [Specialisms.dementia, Specialisms.learning_disabilities],
            [Specialisms.adults_over_65],
        ),
    ]
    expected_classify_specialisms_rows = [
        ("1-001", "1-002", "1-003"),
        (
            [Specialisms.dementia],
            [Specialisms.dementia, Specialisms.learning_disabilities],
            [Specialisms.adults_over_65],
        ),
        (SpecGenOther.specialist, SpecGenOther.generalist, SpecGenOther.other),
        (SpecGenOther.other, SpecGenOther.generalist, SpecGenOther.other),
        (SpecGenOther.other, SpecGenOther.other, SpecGenOther.other),
    ]

    remove_specialist_colleges_rows = [
        ("1-001", "1-002", "1-003", "1-004", "1-005"),
        (
            [Services.care_home_service_with_nursing],
            [
                Services.care_home_service_with_nursing,
                Services.domiciliary_care_service,
            ],
            [
                Services.care_home_service_with_nursing,
                Services.specialist_college_service,
            ],
            None,
            [Services.specialist_college_service],
        ),
    ]
    expected_remove_specialist_colleges_rows = [
        ("1-001", "1-002", "1-003", "1-004"),
        (
            [Services.care_home_service_with_nursing],
            [
                Services.care_home_service_with_nursing,
                Services.domiciliary_care_service,
            ],
            [
                Services.care_home_service_with_nursing,
                Services.specialist_college_service,
            ],
            None,
        ),
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
