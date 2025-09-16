from dataclasses import dataclass
from datetime import date

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import RegistrationStatus


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
