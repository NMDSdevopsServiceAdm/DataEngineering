from dataclasses import dataclass
from datetime import date

from utils.column_values.categorical_column_values import MainJobRoleLabels


@dataclass
class IngestASCWDSData:
    raise_mainjrid_error_col_not_present_rows = [("123", "1-001")]
    raise_mainjrid_error_with_known_value_rows = [("123", "1-001", "1")]
    raise_mainjrid_error_with_unknown_value_rows = [("123", "1-001", "-1")]

    fix_nmdssc_dates_rows = [("100", "07/31/2021", "8", "10/01/2024")]
    expected_fix_nmdssc_dates_rows = [("100", "31/07/2021", "8", "01/10/2024")]

    fix_nmdssc_dates_with_last_logged_in_rows = [
        ("100", "07/31/2021", "8", "10/01/2024")
    ]
    expected_fix_nmdssc_dates_with_last_logged_in_rows = [
        ("100", "31/07/2021", "8", "01/10/2024")
    ]


@dataclass
class ASCWDSWorkerData:
    workplace_rows = [
        ("1-000000001", "101", "20200101"),
        ("1-000000002", "102", "20200101"),
        ("1-000000003", "103", "20200101"),
        ("1-000000004", "104", "20190101"),
    ]

    worker_rows = [
        ("1-000000001", "101", "100", "1", "20200101"),
        ("1-000000002", "102", "101", "1", "20200101"),
        ("1-000000003", "103", "102", "1", "20200101"),
        ("1-000000004", "104", "103", "1", "20190101"),
        ("1-000000005", "104", "104", "2", "19000101"),
        ("1-000000006", "inv", "105", "3", "20200101"),
        ("1-000000007", "999", "106", "1", "20200101"),
    ]

    expected_remove_workers_without_workplaces_rows = [
        ("1-000000001", "101", "100", "1", "20200101"),
        ("1-000000002", "102", "101", "1", "20200101"),
        ("1-000000003", "103", "102", "1", "20200101"),
        ("1-000000004", "104", "103", "1", "20190101"),
    ]

    create_clean_main_job_role_column_rows = [
        ("101", date(2024, 1, 1), "-1"),
        ("101", date(2025, 1, 1), "1"),
        ("102", date(2025, 1, 1), "-1"),
        ("103", date(2024, 1, 1), "3"),
        ("103", date(2025, 1, 1), "4"),
        ("141", date(2025, 1, 1), "41"),
    ]
    expected_create_clean_main_job_role_column_rows = [
        ("101", date(2024, 1, 1), "-1", "1", MainJobRoleLabels.senior_management),
        ("101", date(2025, 1, 1), "1", "1", MainJobRoleLabels.senior_management),
        ("103", date(2024, 1, 1), "3", "3", MainJobRoleLabels.first_line_manager),
        ("103", date(2025, 1, 1), "4", "4", MainJobRoleLabels.registered_manager),
        ("141", date(2025, 1, 1), "41", "40", MainJobRoleLabels.care_coordinator),
    ]

    remap_mainjrid_codes_mapped_codes_rows = [
        ("1000", "41"),
        ("1001", "22"),
    ]
    expected_remap_mainjrid_codes_mapped_codes_rows = [
        ("1000", "40"),
        ("1001", "27"),
    ]
    remap_mainjrid_codes_unmapped_codes_rows = [
        ("1002", "25"),
        ("1003", "40"),
    ]
    expected_remap_mainjrid_codes_unmapped_codes_rows = [
        ("1002", "25"),
        ("1003", "40"),
    ]

    impute_not_known_job_roles_returns_next_known_value_when_before_first_known_value_rows = [
        ("1001", date(2024, 1, 1), "-1"),
        ("1001", date(2024, 3, 1), "8"),
        ("1002", date(2024, 1, 1), "-1"),
        ("1002", date(2024, 6, 1), "7"),
    ]
    expected_impute_not_known_job_roles_returns_next_known_value_when_before_first_known_value_rows = [
        ("1001", date(2024, 1, 1), "8"),
        ("1001", date(2024, 3, 1), "8"),
        ("1002", date(2024, 1, 1), "7"),
        ("1002", date(2024, 6, 1), "7"),
    ]

    impute_not_known_job_roles_returns_previously_known_value_when_after_known_value_rows = [
        ("1001", date(2024, 3, 1), "8"),
        ("1001", date(2024, 4, 1), "-1"),
        ("1002", date(2024, 3, 1), "7"),
        ("1002", date(2024, 8, 1), "-1"),
    ]
    expected_impute_not_known_job_roles_returns_previously_known_value_when_after_known_value_rows = [
        ("1001", date(2024, 3, 1), "8"),
        ("1001", date(2024, 4, 1), "8"),
        ("1002", date(2024, 3, 1), "7"),
        ("1002", date(2024, 8, 1), "7"),
    ]

    impute_not_known_job_roles_returns_previously_known_value_when_in_between_known_values_rows = [
        ("1001", date(2024, 3, 1), "8"),
        ("1001", date(2024, 4, 1), "-1"),
        ("1001", date(2024, 5, 1), "-1"),
        ("1001", date(2024, 6, 1), "7"),
    ]
    expected_impute_not_known_job_roles_returns_previously_known_value_when_in_between_known_values_rows = [
        ("1001", date(2024, 3, 1), "8"),
        ("1001", date(2024, 4, 1), "8"),
        ("1001", date(2024, 5, 1), "8"),
        ("1001", date(2024, 6, 1), "7"),
    ]

    impute_not_known_job_roles_returns_not_known_when_job_role_never_known_rows = [
        ("1001", date(2024, 1, 1), "-1"),
    ]
    expected_impute_not_known_job_roles_returns_not_known_when_job_role_never_known_rows = [
        ("1001", date(2024, 1, 1), "-1"),
    ]


@dataclass
class IngestONSData:
    sample_rows = [
        ("Yorkshire & Humber", "Leeds", "50.10101"),
        ("Yorkshire & Humber", "York", "52.10101"),
        ("Yorkshire & Humber", "Hull", "53.10101"),
    ]

    expected_rows = [
        ("Yorkshire & Humber", "Leeds", "50.10101"),
        ("Yorkshire & Humber", "York", "52.10101"),
        ("Yorkshire & Humber", "Hull", "53.10101"),
    ]


@dataclass
class ValidatePostcodeDirectoryRawData:
    raw_postcode_directory_rows = [
        ("AB1 2CD", "20240101", "cssr", "region", "rui"),
        ("AB2 2CD", "20240101", "cssr", "region", "rui"),
        ("AB1 2CD", "20240201", "cssr", "region", "rui"),
        ("AB2 2CD", "20240201", "cssr", "region", "rui"),
    ]


@dataclass
class CleanONSData:
    ons_sample_rows_full = [
        ("AB10AA", "104", "1", "38000006", "54000005", "1",        "51.23456", "-.12345", "123", "10123", "20123", "1", None, "1000001", "2000001", "14000530", date(2022, 1, 1)),
        ("AB10AB", "104", "1", "38000006", "54000005", "1",        "51.23456", "-.12345", "123", "10123", "20123", "1", None, "1000001", "2000001", "14000530", date(2022, 1, 1)),
        ("AB10AA", "999", "9", "38000265", "54000064", "40000012", "51.23456", "-.12345", "123", "10123", "20123", "9", "6",  "1035762", "2007116", "14001605", date(2023, 1, 1)),
        ("AB10AB", "999", "9", "38000265", "54000064", "40000012", "51.23456", "-.12345", "123", "10123", "20123", "9", "6",  "1035762", "2007116", "14001605", date(2023, 1, 1)),
        ("AB10AC", "999", "9", "38000265", "54000064", "40000012", "51.23456", "-.12345", "123", "10123", "20123", "9", "6",  "1035762", "2007116", "14001605", date(2023, 1, 1)),
    ] # fmt: skip


@dataclass
class ValidatePostcodeDirectoryCleanedData:
    raw_postcode_directory_rows = [
        ("AB1 2CD",),
        ("AB2 2CD",),
        ("AB1 2CD",),
        ("AB2 2CD",),
    ]

    cleaned_postcode_directory_rows = [
        ("AB1 2CD", date(2024, 1, 1), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
        ("AB2 2CD", date(2024, 1, 1), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
        ("AB1 2CD", date(2024, 1, 9), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
        ("AB2 2CD", date(2024, 1, 9), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
    ] # fmt: skip


@dataclass
class ValidateASCWDSWorkplaceRawData:
    raw_ascwds_workplace_rows = [
        ("estab_1", "20240101"),
        ("estab_2", "20240101"),
        ("estab_1", "20240109"),
        ("estab_2", "20240109"),
    ]


@dataclass
class ValidateASCWDSWorkerRawData:
    raw_ascwds_worker_rows = [
        ("estab_1", "20240101", "worker_1", "8"),
        ("estab_2", "20240101", "worker_2", "8"),
        ("estab_1", "20240109", "worker_3", "8"),
        ("estab_2", "20240109", "worker_4", "8"),
    ]


@dataclass
class ValidateASCWDSWorkerCleanedData:
    cleaned_ascwds_worker_rows = [
        ("estab_1", date(2024, 1, 1), "worker_1", "8", "Care Worker"),
        ("estab_2", date(2024, 1, 1), "worker_2", "8", "Care Worker"),
        ("estab_1", date(2024, 1, 9), "worker_3", "8", "Care Worker"),
        ("estab_2", date(2024, 1, 9), "worker_4", "8", "Care Worker"),
    ]


@dataclass
class CleanCQCPIRData:
    sample_rows_full = [
        (
            "1-1000000001",
            "Location 1",
            "Community",
            "2024-01-01",
            1,
            0,
            0,
            None,
            None,
            "Community based adult social care services",
            "ASC North",
            "Wakefield",
            0,
            "Y",
            "Active",
            "20230201",
        ),
        (
            "1-1000000002",
            "Location 2",
            "Residential",
            "2024-01-01",
            86,
            8,
            3,
            None,
            None,
            "Residential social care",
            "ASC London",
            "Islington",
            53,
            None,
            "Active",
            "20230201",
        ),
        (
            "1-1000000003",
            "Location 3",
            "Residential",
            "2024-01-01",
            37,
            5,
            5,
            None,
            None,
            "Residential social care",
            "ASC Central",
            "Nottingham",
            50,
            None,
            "Active",
            "20230201",
        ),
    ]

    add_care_home_column_rows = [
        ("loc 1", "Residential"),
        ("loc 2", "Shared Lives"),
        ("loc 3", None),
        ("loc 4", "Community"),
    ]
    expected_care_home_column_rows = [
        ("loc 1", "Residential", "Y"),
        ("loc 2", "Shared Lives", None),
        ("loc 3", None, None),
        ("loc 4", "Community", "N"),
    ]

    subset_for_latest_submission_date_before_filter = [
        ("1-1199876096", "Y", date(2022, 2, 1), date(2021, 5, 7)),
        ("1-1199876096", "Y", date(2022, 7, 1), date(2022, 5, 20)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 12)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 24)),
        ("1-1199876096", "N", date(2023, 6, 1), date(2023, 5, 24)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 24)),
    ]
    subset_for_latest_submission_date_after_filter_deduplication = [
        ("1-1199876096", "Y", date(2022, 2, 1), date(2021, 5, 7)),
        ("1-1199876096", "Y", date(2022, 7, 1), date(2022, 5, 20)),
        ("1-1199876096", "N", date(2023, 6, 1), date(2023, 5, 24)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 24)),
    ]


@dataclass
class NullPeopleDirectlyEmployedData:
    null_people_directly_employed_outliers_rows = [
        ("1-0001", date(2024, 1, 1), 1),
        ("1-0001", date(2025, 1, 1), 10),
        ("1-0002", date(2024, 1, 1), 100),
        ("1-0002", date(2025, 1, 1), 1000),
    ]

    null_large_single_submission_locations_rows = [
        ("1-0001", date(2024, 1, 1), None),
        ("1-0001", date(2025, 1, 1), 99),
        ("1-0002", date(2024, 1, 1), None),
        ("1-0002", date(2025, 1, 1), 100),
        ("1-0003", date(2024, 1, 1), 99),
        ("1-0003", date(2025, 1, 1), 100),
        ("1-0004", date(2024, 1, 1), 500),
        ("1-0004", date(2025, 1, 1), 600),
    ]
    expected_null_large_single_submission_locations_rows = [
        ("1-0001", date(2024, 1, 1), None),
        ("1-0001", date(2025, 1, 1), 99),
        ("1-0002", date(2024, 1, 1), None),
        ("1-0002", date(2025, 1, 1), None),
        ("1-0003", date(2024, 1, 1), 99),
        ("1-0003", date(2025, 1, 1), 100),
        ("1-0004", date(2024, 1, 1), 500),
        ("1-0004", date(2025, 1, 1), 600),
    ]


@dataclass
class ValidatePIRCleanedData:
    cleaned_cqc_pir_rows = [
        ("1-000000001", date(2024, 1, 1), 10, "Y"),
        ("1-000000002", date(2024, 1, 1), 10, "Y"),
        ("1-000000001", date(2024, 1, 9), 10, "Y"),
        ("1-000000002", date(2024, 1, 9), 10, "Y"),
    ]
