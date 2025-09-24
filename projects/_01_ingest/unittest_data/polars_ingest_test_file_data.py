from dataclasses import dataclass
from datetime import date

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    RegistrationStatus,
    CareHome,
    PrimaryServiceType,
    Services,
    Sector,
)


@dataclass
class CQCLocationsData:
    clean_provider_id_column_rows = [
        ("loc_1", "loc_1", "loc_1", "loc_2", "loc_2", "loc_2"),
        ("123456789", "123456789", "123456789", "223456789", "223456789", "223456789"),
        ("20240101", "20240201", "20240301", "20240101", "20240201", "20240301"),
    ]

    missing_provider_id_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, "123456789", None),
        ("20240101", "20240201", "20240301"),
    ]

    expected_fill_missing_provider_id_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        ("123456789", "123456789", "123456789"),
        ("20240101", "20240201", "20240301"),
    ]

    long_provider_id_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("223456789 223456789", "223456789", "223456789"),
        ("20240101", "20240101", "20240101"),
    ]

    expected_long_provider_id_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        (None, "223456789", "223456789"),
        ("20240101", "20240101", "20240101"),
    ]

    clean_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
        ("20231101", "20240101", "20231101"),
    ]

    expected_clean_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
        ("20231101", "20240101", "20231101"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
    ]

    time_in_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01 00:00:00", "2023-07-01 15:19:00", "2018-01-01"),
        ("20231101", "20240101", "20231101"),
    ]

    expected_time_in_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01 00:00:00", "2023-07-01 15:19:00", "2018-01-01"),
        ("20231101", "20240101", "20231101"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
    ]

    registration_date_after_import_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
        ("20121101", "20240101", "20131101"),
    ]

    expected_registration_date_after_import_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
        ("20121101", "20240101", "20131101"),
        (None, "2023-07-01", None),
    ]

    registration_date_missing_single_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, "2023-07-01", "2023-07-01"),
        ("20240101", "20240201", "20240301"),
    ]

    expected_registration_date_missing_single_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, "2023-07-01", "2023-07-01"),
        ("20240101", "20240201", "20240301"),
        ("2023-07-01", "2023-07-01", "2023-07-01"),
    ]

    registration_date_missing_multiple_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, "2023-08-01", "2023-07-01"),
        ("20240101", "20240201", "20240301"),
    ]

    expected_registration_date_missing_multiple_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, "2023-08-01", "2023-07-01"),
        ("20240101", "20240201", "20240301"),
        ("2023-07-01", "2023-08-01", "2023-07-01"),
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
        ("2024-01-01", "2024-01-01", "2024-01-01"),
    ]

    impute_historic_relationships_all_populated = [
        ("loc_1", "loc_1", "loc_2"),
        (date(2023, 1, 1), date(2023, 2, 1), date(2023, 1, 1)),
        (
            RegistrationStatus.registered,
            RegistrationStatus.deregistered,
            RegistrationStatus.registered,
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB-2",
                    CQCL.related_location_name: "Location B-2",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locC",
                    CQCL.related_location_name: "Location C",
                    CQCL.type: "SomeExtraType",
                    CQCL.reason: "SomeExtraReason",
                }
            ],
        ),
    ]

    expected_impute_historic_relationships_all_populated = [
        ("loc_1", "loc_1", "loc_2"),
        (date(2023, 1, 1), date(2023, 2, 1), date(2023, 1, 1)),
        (
            RegistrationStatus.registered,
            RegistrationStatus.deregistered,
            RegistrationStatus.registered,
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB-2",
                    CQCL.related_location_name: "Location B-2",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locC",
                    CQCL.related_location_name: "Location C",
                    CQCL.type: "SomeExtraType",
                    CQCL.reason: "SomeExtraReason",
                }
            ],
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB-2",
                    CQCL.related_location_name: "Location B-2",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locC",
                    CQCL.related_location_name: "Location C",
                    CQCL.type: "SomeExtraType",
                    CQCL.reason: "SomeExtraReason",
                }
            ],
        ),
    ]

    impute_historic_relationships_no_relationships_rows = [
        ("loc_1", "loc_1"),
        (date(2023, 1, 1), date(2023, 2, 1)),
        (RegistrationStatus.registered, RegistrationStatus.registered),
        (None, None),
    ]

    expected_impute_historic_relationships_no_relationships_rows = [
        ("loc_1", "loc_1"),
        (date(2023, 1, 1), date(2023, 2, 1)),
        (RegistrationStatus.registered, RegistrationStatus.registered),
        (None, None),
        (None, None),
    ]

    impute_historic_relationships_deregistered = [
        ("loc_1", "loc_1", "loc_1"),
        (date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)),
        (
            RegistrationStatus.deregistered,
            RegistrationStatus.deregistered,
            RegistrationStatus.deregistered,
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB-2",
                    CQCL.related_location_name: "Location B-2",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            None,
        ),
    ]

    expected_impute_historic_relationships_deregistered = [
        ("loc_1", "loc_1", "loc_1"),
        (date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)),
        (
            RegistrationStatus.deregistered,
            RegistrationStatus.deregistered,
            RegistrationStatus.deregistered,
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB-2",
                    CQCL.related_location_name: "Location B-2",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            None,
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB-2",
                    CQCL.related_location_name: "Location B-2",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
            ],
        ),
    ]

    impute_historic_relationships_registered = [
        ("loc_1", "loc_1", "loc_1"),
        (date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)),
        (
            RegistrationStatus.registered,
            RegistrationStatus.registered,
            RegistrationStatus.registered,
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB-2",
                    CQCL.related_location_name: "Location B-2",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            None,
        ),
    ]

    expected_impute_historic_relationships_registered = [
        ("loc_1", "loc_1", "loc_1"),
        (date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)),
        (
            RegistrationStatus.registered,
            RegistrationStatus.registered,
            RegistrationStatus.registered,
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB-2",
                    CQCL.related_location_name: "Location B-2",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            None,
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB-2",
                    CQCL.related_location_name: "Location B-2",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "PredecessorID",
                    CQCL.related_location_name: "PredecessorName",
                    CQCL.type: "PredecessorType",
                    CQCL.reason: "PredecessorReason",
                },
            ],
        ),
    ]

    expected_impute_historic_relationships_registered_no_predecessor = [
        ("loc_1", "loc_1", "loc_1"),
        (date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)),
        (
            RegistrationStatus.registered,
            RegistrationStatus.registered,
            RegistrationStatus.registered,
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB-2",
                    CQCL.related_location_name: "Location B-2",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            None,
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "SomeType",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB-2",
                    CQCL.related_location_name: "Location B-2",
                    CQCL.type: "SomeOtherType",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
            None,
        ),
    ]

    get_predecessor_relationships_null_first_known = [
        ("loc_1", "loc_2"),
        (date(2024, 2, 1), date(2024, 2, 1)),
        (RegistrationStatus.deregistered, RegistrationStatus.registered),
        (None, None),
    ]

    expected_get_predecessor_relationships_null_first_known = [
        ("loc_1", "loc_2"),
        (date(2024, 2, 1), date(2024, 2, 1)),
        (RegistrationStatus.deregistered, RegistrationStatus.registered),
        (None, None),
        (None, None),
    ]

    get_predecessor_relationships_successor_first_known = [
        ("loc_1", "loc_2"),
        (date(2024, 2, 1), date(2024, 2, 1)),
        (RegistrationStatus.deregistered, RegistrationStatus.registered),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HCSA Successor",
                    CQCL.reason: "SomeReason",
                }
            ],
            [
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "HCSA Successor",
                    CQCL.reason: "SomeOtherReason",
                }
            ],
        ),
    ]

    expected_get_predecessor_relationships_successor_first_known = [
        ("loc_1", "loc_2"),
        (date(2024, 2, 1), date(2024, 2, 1)),
        (RegistrationStatus.deregistered, RegistrationStatus.registered),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HCSA Successor",
                    CQCL.reason: "SomeReason",
                }
            ],
            [
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "HCSA Successor",
                    CQCL.reason: "SomeOtherReason",
                }
            ],
        ),
        (None, None),
    ]

    get_predecessor_relationships_predecessor_first_known = [
        ("loc_1", "loc_2"),
        (date(2024, 2, 1), date(2024, 2, 1)),
        (RegistrationStatus.deregistered, RegistrationStatus.registered),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                }
            ],
            [
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                }
            ],
        ),
    ]

    expected_get_predecessor_relationships_predecessor_first_known = [
        ("loc_1", "loc_2"),
        (date(2024, 2, 1), date(2024, 2, 1)),
        (RegistrationStatus.deregistered, RegistrationStatus.registered),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                }
            ],
            [
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                }
            ],
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                }
            ],
            [
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                }
            ],
        ),
    ]

    get_predecessor_relationships_both_types = [
        ("loc_1", "loc_2"),
        (date(2024, 2, 1), date(2024, 2, 1)),
        (RegistrationStatus.deregistered, RegistrationStatus.registered),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locY",
                    CQCL.related_location_name: "Location Y",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                },
                {
                    CQCL.related_location_id: "locZ",
                    CQCL.related_location_name: "Location Z",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
        ),
    ]

    expected_get_predecessor_relationships_both_types = [
        ("loc_1", "loc_2"),
        (date(2024, 2, 1), date(2024, 2, 1)),
        (RegistrationStatus.deregistered, RegistrationStatus.registered),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locY",
                    CQCL.related_location_name: "Location Y",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                },
                {
                    CQCL.related_location_id: "locZ",
                    CQCL.related_location_name: "Location Z",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                }
            ],
            [
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                }
            ],
        ),
    ]

    get_predecessor_multiple_predecessors = [
        ("loc_1", "loc_2"),
        (date(2024, 2, 1), date(2024, 2, 1)),
        (RegistrationStatus.deregistered, RegistrationStatus.registered),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locW",
                    CQCL.related_location_name: "Location W",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locY",
                    CQCL.related_location_name: "Location Y",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                },
                {
                    CQCL.related_location_id: "locZ",
                    CQCL.related_location_name: "Location Z",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
        ),
    ]

    expected_get_predecessor_multiple_predecessors = [
        ("loc_1", "loc_2"),
        (date(2024, 2, 1), date(2024, 2, 1)),
        (RegistrationStatus.deregistered, RegistrationStatus.registered),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locW",
                    CQCL.related_location_name: "Location W",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locY",
                    CQCL.related_location_name: "Location Y",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                },
                {
                    CQCL.related_location_id: "locZ",
                    CQCL.related_location_name: "Location Z",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
        ),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locW",
                    CQCL.related_location_name: "Location W",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locB",
                    CQCL.related_location_name: "Location B",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                },
                {
                    CQCL.related_location_id: "locZ",
                    CQCL.related_location_name: "Location Z",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeOtherReason",
                },
            ],
        ),
    ]

    impute_struct_existing_values = [
        ("loc_1", "loc_1", "loc_1"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)),
        (
            [{"name": "Name A", "description": "Desc A"}],
            [{"name": "Name B", "description": "Desc B"}],
            [{"name": "Name C", "description": "Desc C"}],
        ),
    ]

    expected_impute_struct_existing_values = [
        ("loc_1", "loc_1", "loc_1"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)),
        (
            [{"name": "Name A", "description": "Desc A"}],
            [{"name": "Name B", "description": "Desc B"}],
            [{"name": "Name C", "description": "Desc C"}],
        ),
        (
            [{"name": "Name A", "description": "Desc A"}],
            [{"name": "Name B", "description": "Desc B"}],
            [{"name": "Name C", "description": "Desc C"}],
        ),
    ]

    impute_struct_from_historic = [
        ("loc_1", "loc_1", "loc_1"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)),
        (
            [{"name": "Name A", "description": "Desc A"}],
            [],
            [{"name": "Name C", "description": "Desc C"}],
        ),
    ]

    expected_impute_struct_from_historic = [
        ("loc_1", "loc_1", "loc_1"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)),
        (
            [{"name": "Name A", "description": "Desc A"}],
            [],
            [{"name": "Name C", "description": "Desc C"}],
        ),
        (
            [{"name": "Name A", "description": "Desc A"}],
            [{"name": "Name A", "description": "Desc A"}],
            [{"name": "Name C", "description": "Desc C"}],
        ),
    ]

    impute_struct_from_future = [
        ("loc_1", "loc_1", "loc_1"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)),
        (
            [],
            [],
            [{"name": "Name C", "description": "Desc C"}],
        ),
    ]

    expected_impute_struct_from_future = [
        ("loc_1", "loc_1", "loc_1"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)),
        (
            [],
            [],
            [{"name": "Name C", "description": "Desc C"}],
        ),
        (
            [{"name": "Name C", "description": "Desc C"}],
            [{"name": "Name C", "description": "Desc C"}],
            [{"name": "Name C", "description": "Desc C"}],
        ),
    ]

    impute_struct_null_values = [
        ("loc_1", "loc_1", "loc_2"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)),
        (
            [],
            [],
            [{"name": "Name C", "description": "Desc C"}],
        ),
    ]

    expected_impute_struct_null_values = [
        ("loc_1", "loc_1", "loc_2"),
        (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)),
        (
            [],
            [],
            [{"name": "Name C", "description": "Desc C"}],
        ),
        (
            None,
            None,
            [{"name": "Name C", "description": "Desc C"}],
        ),
    ]

    allocate_primary_service_care_home_with_nursing = [
        ("loc_1", "loc_2", "loc_3"),
        (
            [
                {
                    "name": "With Nurses 1",
                    "description": "Care home service with nursing",
                }
            ],
            [
                {
                    "name": "With Nurses 2",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Another Service",
                    "description": "Any other description",
                },
            ],
            [
                {
                    "name": "With Nurses 3",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Another Legitimate Service",
                    "description": "Care home service without nursing",
                },
            ],
        ),
    ]

    expected_allocate_primary_service_care_home_with_nursing = [
        ("loc_1", "loc_2", "loc_3"),
        (
            [
                {
                    "name": "With Nurses 1",
                    "description": "Care home service with nursing",
                }
            ],
            [
                {
                    "name": "With Nurses 2",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Another Service",
                    "description": "Any other description",
                },
            ],
            [
                {
                    "name": "With Nurses 3",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Another Legitimate Service",
                    "description": "Care home service without nursing",
                },
            ],
        ),
        (
            PrimaryServiceType.care_home_with_nursing,
            PrimaryServiceType.care_home_with_nursing,
            PrimaryServiceType.care_home_with_nursing,
        ),
    ]

    allocate_primary_service_care_home_only = [
        ("loc_1", "loc_2", "loc_3"),
        (
            [
                {
                    "name": "Without Nursing 1",
                    "description": "Care home service without nursing",
                }
            ],
            [
                {
                    "name": "Without Nursing 2",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Another Service",
                    "description": "Any other description",
                },
            ],
            [
                {
                    "name": "Another Service First",
                    "description": "Another description",
                },
                {
                    "name": "Without Nursing 3",
                    "description": "Care home service without nursing",
                },
            ],
        ),
    ]

    expected_allocate_primary_service_care_home_only = [
        ("loc_1", "loc_2", "loc_3"),
        (
            [
                {
                    "name": "Without Nursing 1",
                    "description": "Care home service without nursing",
                }
            ],
            [
                {
                    "name": "Without Nursing 2",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Another Service",
                    "description": "Any other description",
                },
            ],
            [
                {
                    "name": "Another Service First",
                    "description": "Another description",
                },
                {
                    "name": "Without Nursing 3",
                    "description": "Care home service without nursing",
                },
            ],
        ),
        (
            PrimaryServiceType.care_home_only,
            PrimaryServiceType.care_home_only,
            PrimaryServiceType.care_home_only,
        ),
    ]

    allocate_primary_service_non_residential = [
        ("loc_1", "loc_2"),
        (
            [
                {
                    "name": "Without Nursing Malformed",
                    "description": "Care home service without nursing with small difference",
                }
            ],
            [
                {
                    "name": "Random Service",
                    "description": "Random Service",
                },
                {
                    "name": "Another Service",
                    "description": "Any other description",
                },
            ],
        ),
    ]

    expected_allocate_primary_service_non_residential = [
        ("loc_1", "loc_2"),
        (
            [
                {
                    "name": "Without Nursing Malformed",
                    "description": "Care home service without nursing with small difference",
                }
            ],
            [
                {
                    "name": "Random Service",
                    "description": "Random Service",
                },
                {
                    "name": "Another Service",
                    "description": "Any other description",
                },
            ],
        ),
        (
            PrimaryServiceType.non_residential,
            PrimaryServiceType.non_residential,
        ),
    ]

    allocate_primary_service_all_types = [
        ("loc_1", "loc_2", "loc_3"),
        (
            [
                {
                    "name": "With Nurses 3",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Another Legitimate Service",
                    "description": "Care home service without nursing",
                },
            ],
            [
                {
                    "name": "Without Nursing 2",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Another Service",
                    "description": "Any other description",
                },
            ],
            [
                {
                    "name": "Random Service",
                    "description": "Random Service",
                },
                {
                    "name": "Another Service",
                    "description": "Any other description",
                },
            ],
        ),
    ]

    expected_allocate_primary_service_all_types = [
        ("loc_1", "loc_2", "loc_3"),
        (
            [
                {
                    "name": "With Nurses 3",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Another Legitimate Service",
                    "description": "Care home service without nursing",
                },
            ],
            [
                {
                    "name": "Without Nursing 2",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Another Service",
                    "description": "Any other description",
                },
            ],
            [
                {
                    "name": "Random Service",
                    "description": "Random Service",
                },
                {
                    "name": "Another Service",
                    "description": "Any other description",
                },
            ],
        ),
        (
            PrimaryServiceType.care_home_with_nursing,
            PrimaryServiceType.care_home_only,
            PrimaryServiceType.non_residential,
        ),
    ]

    align_care_home_care_homes_rows = [
        ("loc_1", "loc_2"),
        (PrimaryServiceType.care_home_with_nursing, PrimaryServiceType.care_home_only),
    ]

    expected_align_care_home_care_homes_rows = [
        ("loc_1", "loc_2"),
        (PrimaryServiceType.care_home_with_nursing, PrimaryServiceType.care_home_only),
        ("Y", "Y"),
    ]

    align_care_home_non_care_homes_rows = [
        ("loc_1", "loc_2"),
        (
            PrimaryServiceType.non_residential,
            "FalseValue",
        ),
    ]

    expected_align_care_home_non_care_homes_rows = [
        ("loc_1", "loc_2"),
        (
            PrimaryServiceType.non_residential,
            "FalseValue",
        ),
        ("N", "N"),
    ]

    related_location_flag_with_related_locations = [
        ("loc_1", "loc_2"),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locW",
                    CQCL.related_location_name: "Location W",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locY",
                    CQCL.related_location_name: "Location Y",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
            ],
        ),
    ]

    expected_related_location_flag_with_related_locations = [
        ("loc_1", "loc_2"),
        (
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locW",
                    CQCL.related_location_name: "Location W",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
                {
                    CQCL.related_location_id: "locY",
                    CQCL.related_location_name: "Location Y",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "SomeReason",
                },
            ],
            [
                {
                    CQCL.related_location_id: "locA",
                    CQCL.related_location_name: "Location A",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "SomeReason",
                },
            ],
        ),
        ["Y", "Y"],
    ]

    related_location_flag_with_no_related_locations = [("loc_1", "loc_2"), ([], None)]

    expected_related_location_flag_with_no_related_locations = [
        ("loc_1", "loc_2"),
        ([], None),
        ("N", "N"),
    ]

    remove_specialist_colleges_fact = [
        ("loc_1", "loc_2"),
        ("20240101", "20240101"),
    ]

    remove_specialist_colleges_dim_only_specialist_college = [
        ("loc_1", "loc_2"),
        ("20240101", "20240101"),
        ([Services.specialist_college_service], [Services.specialist_college_service]),
    ]

    expected_remove_specialist_colleges_dim_only_specialist_college = [
        ("loc_1", "loc_2"),
        ("20240101", "20240101"),
    ]

    remove_specialist_colleges_dim_specialist_college_plus_other = [
        ("loc_1", "loc_2", "loc_3"),
        ("20240101", "20240101", "20240101"),
        (
            [Services.specialist_college_service, Services.specialist_college_service],
            [
                Services.specialist_college_service,
                Services.care_home_service_with_nursing,
            ],
            [
                Services.acute_services_with_overnight_beds,
                Services.specialist_college_service,
            ],
        ),
    ]

    remove_specialist_colleges_dim_no_specialist_college = [
        ("loc_1", "loc_2"),
        ("20240101", "20240101"),
        (
            [Services.care_home_service_with_nursing],
            [Services.acute_services_with_overnight_beds, Services.shared_lives],
        ),
    ]

    remove_specialist_colleges_dim_no_services_offered = [
        ("loc_1", "loc_2"),
        ("20240101", "20240101"),
        ([], None),
    ]
    expected_remove_specialist_colleges_remove_none = []

    select_registered_locations_all_registered = [
        ("loc_1", "loc_2"),
        (RegistrationStatus.registered, RegistrationStatus.registered),
    ]

    select_registered_locations_none_registered = [
        ("loc_1", "loc_2"),
        (RegistrationStatus.deregistered, RegistrationStatus.deregistered),
    ]

    select_registered_locations_mixed_status = [
        ("loc_1", "loc_2", "loc_3", "loc_4"),
        (
            RegistrationStatus.registered,
            RegistrationStatus.registered,
            RegistrationStatus.deregistered,
            RegistrationStatus.deregistered,
        ),
    ]

    select_registered_locations_empty_input = []

    select_registered_locations_invalid_status = [
        ("loc_1", "loc_2"),
        ("invalid_status", f"{RegistrationStatus.registered}-"),
    ]

    expected_select_registered_locations_registered = [
        ("loc_1", "loc_2"),
        (RegistrationStatus.registered, RegistrationStatus.registered),
    ]

    expected_select_registered_locations_empty = []

    assign_cqc_sector = [
        ("loc_1", "loc_2"),
        ("prov_1", "prov_2"),
    ]

    expected_assign_cqc_sector_local_authority = [
        ("loc_1", "loc_2"),
        ("prov_1", "prov_2"),
        (Sector.local_authority, Sector.local_authority),
    ]

    expected_assign_cqc_sector_independent = [
        ("loc_1", "loc_2"),
        ("prov_1", "prov_2"),
        (Sector.independent, Sector.independent),
    ]
