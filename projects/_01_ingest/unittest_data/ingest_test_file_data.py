from dataclasses import dataclass
from datetime import date

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResColumns as CTNR,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CareHome,
    Dormancy,
    LocationType,
    MainJobRoleLabels,
    PrimaryServiceType,
    RegistrationStatus,
    RelatedLocation,
    Sector,
    Services,
)
from utils.raw_data_adjustments import RecordsToRemoveInLocationsData


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
class ASCWDSWorkplaceData:
    workplace_rows = [
        (
            "1-000000001",
            "101",
            14,
            16,
            "20200101",
            "1",
            date(2021, 2, 1),
            0,
            "201",
            "01/02/2021",
            "A101",
        ),
        (
            "1-000000002",
            "102",
            76,
            65,
            "20200101",
            "1",
            date(2021, 4, 1),
            1,
            None,
            "01/02/2021",
            "A102",
        ),
        (
            "1-000000003",
            "103",
            34,
            34,
            "20200101",
            "2",
            date(2021, 3, 1),
            0,
            "203",
            "01/02/2021",
            "A103",
        ),
        (
            "1-000000004",
            "104",
            234,
            265,
            "20190101",
            "2",
            date(2021, 4, 1),
            0,
            None,
            "01/02/2021",
            "A104",
        ),
        (
            "1-000000005",
            "105",
            62,
            65,
            "20190101",
            "3",
            date(2021, 10, 1),
            0,
            None,
            "01/02/2021",
            "A105",
        ),
        (
            "1-000000006",
            "106",
            77,
            77,
            "20190101",
            "3",
            date(2020, 3, 1),
            1,
            None,
            "01/02/2021",
            "A106",
        ),
        (
            "1-000000007",
            "107",
            51,
            42,
            "20190101",
            " 3",
            date(2021, 5, 1),
            0,
            None,
            "01/05/2021",
            "A107",
        ),
        (
            "1-000000008",
            "108",
            36,
            34,
            "20190101",
            "4",
            date(2021, 7, 1),
            0,
            None,
            "01/05/2021",
            "A108",
        ),
        (
            "1-000000009",
            "109",
            34,
            32,
            "20190101",
            "5",
            date(2021, 12, 1),
            0,
            None,
            "01/05/2021",
            "A109",
        ),
        (
            "1-0000000010",
            "110",
            14,
            20,
            "20190101",
            "6",
            date(2021, 3, 1),
            0,
            None,
            "01/05/2021",
            "A1010",
        ),
    ]

    filter_test_account_when_orgid_present_rows = [
        ("1-001", "310"),
        ("1-002", "2452"),
        ("1-003", "308"),
        ("1-004", "1234"),
        ("1-005", "31138"),
    ]
    expected_filter_test_account_when_orgid_present_rows = [
        ("1-004", "1234"),
    ]

    filter_test_account_when_orgid_not_present_rows = [
        ("1-001", "20250101"),
        ("1-002", "20250101"),
        ("1-003", "20250101"),
        ("1-004", "20250101"),
        ("1-005", "20250101"),
    ]

    remove_white_space_from_nmdsid_rows = [
        ("1-001", "A123  "),
        ("1-002", "A1234 "),
        ("1-003", "A12345"),
    ]
    expected_remove_white_space_from_nmdsid_rows = [
        ("1-001", "A123"),
        ("1-002", "A1234"),
        ("1-003", "A12345"),
    ]

    small_location_rows = [
        ("loc-1", "2020-01-01", "1"),
        ("loc-2", "2020-01-01", "2"),
        ("loc-3", "2020-01-01", "3"),
        ("loc-4", "2021-01-01", "4"),
        (None, "2021-01-01", "5"),
        (None, "2021-01-01", "6"),
    ]

    location_rows_with_duplicates = [
        *small_location_rows,
        ("loc-3", "2020-01-01", "7"),
        ("loc-4", "2021-01-01", "8"),
    ]

    location_rows_with_different_import_dates = [
        *small_location_rows,
        ("loc-3", "2021-01-01", "3"),
        ("loc-4", "2022-01-01", "4"),
    ]

    expected_filtered_location_rows = [
        ("loc-1", "2020-01-01", "1"),
        ("loc-2", "2020-01-01", "2"),
        (None, "2021-01-01", "5"),
        (None, "2021-01-01", "6"),
    ]

    mupddate_for_org_rows = [
        ("1", date(2024, 3, 1), "1", date(2024, 1, 10)),
        ("1", date(2024, 3, 1), "2", date(2024, 1, 20)),
        ("1", date(2024, 4, 1), "3", date(2024, 3, 10)),
        ("1", date(2024, 4, 1), "4", date(2024, 3, 15)),
        ("2", date(2024, 4, 1), "5", date(2024, 2, 15)),
        ("2", date(2024, 4, 1), "6", date(2024, 3, 10)),
    ]
    expected_mupddate_for_org_rows = [
        ("1", date(2024, 3, 1), "1", date(2024, 1, 10), date(2024, 1, 20)),
        ("1", date(2024, 3, 1), "2", date(2024, 1, 20), date(2024, 1, 20)),
        ("1", date(2024, 4, 1), "3", date(2024, 3, 10), date(2024, 3, 15)),
        ("1", date(2024, 4, 1), "4", date(2024, 3, 15), date(2024, 3, 15)),
        ("2", date(2024, 4, 1), "5", date(2024, 2, 15), date(2024, 3, 10)),
        ("2", date(2024, 4, 1), "6", date(2024, 3, 10), date(2024, 3, 10)),
    ]

    add_purge_data_col_rows = [
        ("1", "Yes", date(2024, 2, 2), date(2024, 2, 2)),
        ("2", "Yes", date(2024, 2, 2), date(2024, 3, 3)),
        ("3", "No", date(2024, 2, 2), date(2024, 2, 2)),
        ("4", "No", date(2024, 2, 2), date(2024, 3, 3)),
    ]
    expected_add_purge_data_col_rows = [
        ("1", "Yes", date(2024, 2, 2), date(2024, 2, 2), date(2024, 2, 2)),
        ("2", "Yes", date(2024, 2, 2), date(2024, 3, 3), date(2024, 3, 3)),
        ("3", "No", date(2024, 2, 2), date(2024, 2, 2), date(2024, 2, 2)),
        ("4", "No", date(2024, 2, 2), date(2024, 3, 3), date(2024, 2, 2)),
    ]

    add_workplace_last_active_date_col_rows = [
        ("1", date(2024, 3, 3), date(2024, 2, 2)),
        ("2", date(2024, 4, 4), date(2024, 5, 5)),
    ]
    expected_add_workplace_last_active_date_col_rows = [
        ("1", date(2024, 3, 3), date(2024, 2, 2), date(2024, 3, 3)),
        ("2", date(2024, 4, 4), date(2024, 5, 5), date(2024, 5, 5)),
    ]

    date_col_for_purging_rows = [
        ("1", date(2024, 3, 3)),
        ("2", date(2024, 4, 4)),
    ]
    expected_date_col_for_purging_rows = [
        ("1", date(2024, 3, 3), date(2022, 3, 3)),
        ("2", date(2024, 4, 4), date(2022, 4, 4)),
    ]

    workplace_last_active_rows = [
        ("1", date(2024, 4, 4), date(2024, 5, 5)),
        ("2", date(2024, 4, 4), date(2024, 4, 4)),
        ("3", date(2024, 4, 4), date(2024, 3, 3)),
    ]


@dataclass
class ASCWDSWorkerData:
    worker_rows = [
        ("1-000000001", "101", "100", "1", "20200101", "2020", "01", "01"),
        ("1-000000002", "102", "101", "1", "20200101", "2020", "01", "01"),
        ("1-000000003", "103", "102", "1", "20200101", "2020", "01", "01"),
        ("1-000000004", "104", "103", "1", "20190101", "2019", "01", "01"),
        ("1-000000005", "104", "104", "2", "19000101", "1900", "01", "01"),
        ("1-000000006", "invalid", "105", "3", "20200101", "2020", "01", "01"),
        ("1-000000007", "999", "106", "1", "20200101", "2020", "01", "01"),
    ]

    expected_remove_workers_without_workplaces_rows = [
        ("1-000000001", "101", "100", "1", "20200101", "2020", "01", "01"),
        ("1-000000002", "102", "101", "1", "20200101", "2020", "01", "01"),
        ("1-000000003", "103", "102", "1", "20200101", "2020", "01", "01"),
        ("1-000000004", "104", "103", "1", "20190101", "2019", "01", "01"),
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

    replace_care_navigator_with_care_coordinator_values_updated_when_care_navigator_is_present_rows = [
        ("41", "41"),
    ]
    expected_replace_care_navigator_with_care_coordinator_values_updated_when_care_navigator_is_present_rows = [
        ("41", "40"),
    ]
    replace_care_navigator_with_care_coordinator_values_remain_unchanged_when_care_navigator_not_present_rows = [
        ("25", "25"),
        ("40", "40"),
    ]
    expected_replace_care_navigator_with_care_coordinator_values_remain_unchanged_when_care_navigator_not_present_rows = [
        ("25", "25"),
        ("40", "40"),
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

    remove_workers_with_not_known_job_role_rows = [
        ("1001", date(2024, 3, 1), "8"),
        ("1002", date(2024, 3, 1), "-1"),
        ("1002", date(2024, 4, 1), "-1"),
    ]
    expected_remove_workers_with_not_known_job_role_rows = [
        ("1001", date(2024, 3, 1), "8"),
    ]


@dataclass
class CapacityTrackerCareHomeData:
    sample_rows = [
        (
            "Barnsley Metropolitan Borough Council",
            "Woodways",
            "Bespoke Care and Support Ltd",
            "South Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "North East and Yorkshire",
            "NHS South Yorkshire ICB",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "1-10192918971",
            "VNJ4V",
            "0",
            "No",
            "0",
            "0",
            "0",
            "61",
            "0",
            "0",
            "8",
            "0",
            "0",
            "0",
            "0",
            "0",
            "9483",
            "1623",
            "432",
            "444",
            "0",
            "45330.3840277778",
            "45330.3840277778",
        ),
        (
            "Barnsley Metropolitan Borough Council",
            "Woodlands Lodge Care Home",
            "Mr Dhanus Dharry Ramdharry, Mrs Sooba Devi Mootyen, Mr Dhanraz Danny Ramdharry",
            "South Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "North East and Yorkshire",
            "NHS South Yorkshire ICB",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "1-933054479",
            "VLEH4",
            "2",
            "Yes",
            "0",
            "0",
            "0",
            "28",
            "0",
            "0",
            "14",
            "0",
            "0",
            "0",
            "0",
            "0",
            "4658",
            "0",
            "18",
            "0",
            "24",
            "45330.4958333333",
            "45330.4958333333",
        ),
        (
            "Barnsley Metropolitan Borough Council",
            "Water Royd Nursing Home",
            "Maria Mallaband Limited",
            "South Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "North East and Yorkshire",
            "NHS South Yorkshire ICB",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "1-124000082",
            "VLNVC",
            "0",
            "Yes",
            "11",
            "3",
            "0",
            "46",
            "5",
            "0",
            "14",
            "0",
            "0",
            "0",
            "0",
            "0",
            "9334",
            "1",
            "0",
            "37",
            "0",
            "45351.3625",
            "45351.3625",
        ),
    ]

    expected_rows = sample_rows


@dataclass
class CapacityTrackerNonResData:
    sample_rows = [
        (
            "Barnsley Metropolitan Borough Council",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "NHS South Yorkshire ICB",
            "North East and Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "South Yorkshire",
            "Yorkshire and The Humber",
            "AJB Care Ltd",
            "1-1140582998",
            "VN5A8",
            "45330.3854166667",
            "45330.3854166667",
            "57",
            "",
            "",
            "20",
            "0",
            "TRUE",
            "40",
            "16",
            "2",
            "4",
            "Yes",
            "2228",
            "0",
            "0",
            "0",
            "14",
            "0",
            "0",
            "0",
        ),
        (
            "Barnsley Metropolitan Borough Council",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "NHS South Yorkshire ICB",
            "North East and Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "South Yorkshire",
            "Yorkshire and The Humber",
            "Barnsley Disability Services Limited",
            "1-1002692043",
            "VN1N0",
            "45331.4673611111",
            "45331.4673611111",
            "12",
            "",
            "",
            "10",
            "0",
            "FALSE",
            "0",
            "10",
            "1",
            "3",
            "Yes",
            "1428",
            "7",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
        ),
        (
            "Barnsley Metropolitan Borough Council",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "NHS South Yorkshire ICB",
            "North East and Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "South Yorkshire",
            "Yorkshire and The Humber",
            "Barnsley Mencap",
            "1-119187505",
            "VN3L9",
            "45331.4597222222",
            "45331.4597222222",
            "102",
            "",
            "",
            "165",
            "0",
            "FALSE",
            "0",
            "161",
            "28",
            "37",
            "Yes",
            "18015",
            "3113",
            "567",
            "0",
            "171",
            "0",
            "0",
            "0",
        ),
    ]

    expected_rows = sample_rows

    remove_invalid_characters_from_column_names_rows = [
        ("loc 1", "some data", "other data", "another data", "more data"),
    ]
    expected_columns = [
        CTNR.cqc_id,
        "column_with_spaces",
        "column_without_spaces",
        "column_with_brackets",
        "column_with_brackets_and_spaces",
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
    # fmt: off
    ons_sample_rows_full = [
        ("AB10AA", "cssr1", "region1", "subicb1", "icb1", "icb_region1", "ccg1", "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "2022", "01", "01", "20220101"),
        ("AB10AB", "cssr1", "region1", "subicb1", "icb1", "icb_region1", "ccg1", "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "2022", "01", "01", "20220101"),
        ("AB10AA", "cssr2", "region1", "subicb2", "icb2", "icb_region2", None, "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "2023", "01", "01", "20230101"),
        ("AB10AB", "cssr2", "region1", "subicb2", "icb2", "icb_region2", None, "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "2023", "01", "01", "20230101"),
        ("AB10AC", "cssr2", "region1", "subicb2", "icb2", "icb_region2", None, "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "2023", "01", "01", "20230101"),
    ]
    # fmt: on


@dataclass
class ValidatePostcodeDirectoryCleanedData:
    raw_postcode_directory_rows = [
        ("AB1 2CD", "20240101"),
        ("AB2 2CD", "20240101"),
        ("AB1 2CD", "20240201"),
        ("AB2 2CD", "20240201"),
    ]

    # fmt: off
    cleaned_postcode_directory_rows = [
        ("AB1 2CD", date(2024, 1, 1), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
        ("AB2 2CD", date(2024, 1, 1), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
        ("AB1 2CD", date(2024, 1, 9), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
        ("AB2 2CD", date(2024, 1, 9), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
    ]
    # fmt: on

    calculate_expected_size_rows = raw_postcode_directory_rows


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
class ValidateASCWDSWorkplaceCleanedData:
    cleaned_ascwds_workplace_rows = [
        ("estab_1", date(2024, 1, 1), "org_id", "location_id", 10, 10),
        ("estab_2", date(2024, 1, 1), "org_id", "location_id", 10, 10),
        ("estab_1", date(2024, 1, 9), "org_id", "location_id", 10, 10),
        ("estab_2", date(2024, 1, 9), "org_id", "location_id", 10, 10),
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
    remove_unused_pir_types_rows = add_care_home_column_rows
    expected_remove_unused_pir_types_rows = [
        ("loc 1", "Residential"),
        ("loc 4", "Community"),
    ]

    remove_rows_missing_pir_people_directly_employed = [
        ("loc_1", 1),
        ("loc_1", 0),
        ("loc_1", None),
    ]

    expected_remove_rows_missing_pir_people_directly_employed = [
        ("loc_1", 1),
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
class ValidatePIRRawData:
    raw_cqc_pir_rows = [
        ("1-000000001", "20240101", 10),
        ("1-000000002", "20240101", 10),
        ("1-000000001", "20240109", 10),
        ("1-000000002", "20240109", 10),
    ]


@dataclass
class ValidatePIRCleanedData:
    cleaned_cqc_pir_rows = [
        ("1-000000001", date(2024, 1, 1), 10, "Y"),
        ("1-000000002", date(2024, 1, 1), 10, "Y"),
        ("1-000000001", date(2024, 1, 9), 10, "Y"),
        ("1-000000002", date(2024, 1, 9), 10, "Y"),
    ]


@dataclass
class PostcodeMatcherData:
    # fmt: off
    locations_where_all_match_rows = [
        ("1-001", date(2020, 1, 1), "name 1", "1 road name", "AA1 1aa"),
        ("1-001", date(2025, 1, 1), "name 1", "1 road name", "AA1 1aa"),  # lower case but matches ok
        ("1-002", date(2020, 1, 1), "name 2", "2 road name", "AA1 ZAA"),  # wrong now but amended later (match to the first known one, not the second)
        ("1-002", date(2025, 1, 1), "name 2", "2 road name", "AA1 2AA"),
        ("1-002", date(2025, 1, 1), "name 2", "2 road name", "AA1 3AA"),
        # ("1-003", date(2025, 1, 1), "name 3", "3 road name", "29 5HF"),  # known issue (actually need one from the invalid list here)
        ("1-004", date(2025, 1, 1), "name 4", "4 road name", "AA1 4ZZ"),  # match this in truncated
    ]
    locations_with_unmatched_postcode_rows = [
        ("1-001", date(2020, 1, 1), "name 1", "1 road name", "AA1 1aa"),
        ("1-001", date(2025, 1, 1), "name 1", "1 road name", "AA1 1aa"),
        ("1-005", date(2025, 1, 1), "name 5", "5 road name", "AA2 5XX"),  # never known
    ]
    # fmt: on

    # fmt: off
    postcodes_rows = [
        ("AA11AA", date(2020, 1, 1), "CSSR 1", None, "CCG 1", "CSSR 1", "SubICB 1"),
        ("AA12AA", date(2020, 1, 1), "CSSR 1", None, "CCG 1", "CSSR 2", "SubICB 1"),
        ("AA13AA", date(2020, 1, 1), "CSSR 1", None, "CCG 1", "CSSR 3", "SubICB 1"),
        ("AA11AA", date(2025, 1, 1), "CSSR 1", "SubICB 1", None, "CSSR 1", "SubICB 1"),
        ("AA12AA", date(2025, 1, 1), "CSSR 1", "SubICB 1", None, "CSSR 2", "SubICB 1"),
        ("AA13AA", date(2025, 1, 1), "CSSR 1", "SubICB 1", None, "CSSR 3", "SubICB 1"),
        ("AA14AA", date(2025, 1, 1), "CSSR 1", "SubICB 1", None, "CSSR 4", "SubICB 1"),
    ]
    # fmt: on

    clean_postcode_column_rows = [
        ("aA11Aa", "ccsr 1"),
        ("AA1 2AA", "ccsr 1"),
        ("aA1 3aA", "ccsr 1"),
    ]
    expected_clean_postcode_column_rows = [
        ("aA11Aa", "ccsr 1", "AA11AA"),
        ("AA1 2AA", "ccsr 1", "AA12AA"),
        ("aA1 3aA", "ccsr 1", "AA13AA"),
    ]

    join_postcode_data_locations_rows = [
        ("1-001", date(2020, 1, 1), "AA11AA"),
        ("1-001", date(2025, 1, 1), "AA11AA"),
        ("1-002", date(2020, 1, 1), "AA1ZAA"),
        ("1-002", date(2025, 1, 1), "AA12AA"),
    ]
    join_postcode_data_postcodes_rows = [
        ("AA11AA", date(2020, 1, 1), "CSSR 1"),
        ("AA12AA", date(2020, 1, 1), "CSSR 2"),
        ("AA11AA", date(2025, 1, 1), "CSSR 1"),
        ("AA12AA", date(2025, 1, 1), "CSSR 2"),
    ]
    expected_join_postcode_data_matched_rows = [
        ("1-001", date(2020, 1, 1), "AA11AA", "CSSR 1"),
        ("1-001", date(2025, 1, 1), "AA11AA", "CSSR 1"),
        ("1-002", date(2025, 1, 1), "AA12AA", "CSSR 2"),
    ]
    expected_join_postcode_data_unmatched_rows = [
        ("1-002", date(2020, 1, 1), "AA1ZAA"),
    ]

    first_successful_postcode_unmatched_rows = [
        ("1-001", date(2023, 1, 1), "AA10AA"),
        ("1-003", date(2025, 1, 1), "AA13AA"),
    ]
    first_successful_postcode_matched_rows = [
        ("1-001", date(2024, 1, 1), "AA11AB", "CSSR 2"),
        ("1-001", date(2025, 1, 1), "AA11AA", "CSSR 1"),
        ("1-002", date(2025, 1, 1), "AA12AA", "CSSR 1"),
    ]
    expected_get_first_successful_postcode_match_rows = [
        ("1-001", date(2023, 1, 1), "AA11AB"),
        ("1-003", date(2025, 1, 1), "AA13AA"),
    ]

    truncate_postcode_rows = [
        ("AA11AA", date(2023, 1, 1)),
        ("AA11AB", date(2023, 1, 1)),
        ("AB1CD", date(2023, 1, 1)),
        ("B1CD", date(2023, 1, 1)),
    ]
    expected_truncate_postcode_rows = [
        ("AA11AA", date(2023, 1, 1), "AA11"),
        ("AA11AB", date(2023, 1, 1), "AA11"),
        ("AB1CD", date(2023, 1, 1), "AB1"),
        ("B1CD", date(2023, 1, 1), "B1"),
    ]

    # fmt: off
    create_truncated_postcode_df_rows = [
        ("AB12CD", date(2025, 1, 1), "LA_1", "CCG_1", "ICB_1", "LA_1", "ICB_1"),
        ("AB12CE", date(2025, 1, 1), "LA_2", "CCG_2", "ICB_2", "LA_2", "ICB_2"),
        ("AB12CF", date(2025, 1, 1), "LA_2", "CCG_2", "ICB_2", "LA_2", "ICB_2"),
        ("AB12CG", date(2025, 1, 1), "LA_3", "CCG_1", "ICB_1", "LA_1", "ICB_1"),
        ("AB12CG", date(2025, 1, 1), "LA_4", "CCG_1", "ICB_1", "LA_1", "ICB_1"),
        ("AB13CD", date(2025, 1, 1), "LA_3", "CCG_3", "ICB_3", "LA_3", "ICB_3"),
    ]
    expected_create_truncated_postcode_df_rows = [
        (date(2025, 1, 1), "LA_2", "CCG_2", "ICB_2", "LA_2", "ICB_2", "AB12"),
        (date(2025, 1, 1), "LA_3", "CCG_3", "ICB_3", "LA_3", "ICB_3", "AB13"),
    ]
    # fmt: on

    combine_matched_df1_rows = [
        ("1-001", date(2025, 1, 1), "AA11AA", "CSSR 1"),
        ("1-003", date(2025, 1, 1), "AA12AA", "CSSR 1"),
    ]
    combine_matched_df2_rows = [
        ("1-002", date(2025, 1, 1), "ZZ11AA", "ZZ11", "CSSR 2"),
        ("1-004", date(2025, 1, 1), "ZZ12AA", "ZZ12", "CSSR 3"),
    ]
    expected_combine_matched_rows = [
        ("1-001", date(2025, 1, 1), "AA11AA", "CSSR 1", None),
        ("1-002", date(2025, 1, 1), "ZZ11AA", "CSSR 2", "ZZ11"),
        ("1-003", date(2025, 1, 1), "AA12AA", "CSSR 1", None),
        ("1-004", date(2025, 1, 1), "ZZ12AA", "CSSR 3", "ZZ12"),
    ]


@dataclass
class ValidateLocationsAPIRawData:
    raw_cqc_locations_rows = [
        ("1-00001", "20240101", "1-001", "name", LocationType.social_care_identifier),
        ("1-00002", "20240101", "1-001", "name", LocationType.social_care_identifier),
        ("1-00001", "20240201", "1-001", "name", LocationType.social_care_identifier),
        ("1-00002", "20240201", "1-001", "name", LocationType.social_care_identifier),
        ("1-00002", "20240201", "1-001", "name", LocationType.social_care_identifier),
    ]


@dataclass
class ValidateProvidersAPIRawData:
    # fmt: off
    raw_cqc_providers_rows = [
        ("1-000000001", "20240101", "name"),
        ("1-000000002", "20240101", "name"),
        ("1-000000001", "20240201", "name"),
        ("1-000000002", "20240201", "name"),
    ]


@dataclass
class CQCLocationsData:
    sample_rows = [
        (
            "location1",
            "provider1",
            "Location",
            "Social Care Org",
            "name of location",
            "Registered",
            "2020-01-01",
            None,
            "N",
            20,
            "www.website.org",
            "1 The Street",
            "Leeds",
            "West Yorkshire",
            "Yorkshire",
            "LS1 2AB",
            "50.123455",
            "-5.6789",
            "Y",
            "Adult social care",
            "01234567891",
            "Trafford",
            [
                {
                    "name": "Personal care",
                    "code": "RA1",
                    "contacts": [
                        {
                            "personfamilyname": "Doe",
                            "persongivenname": "John",
                            "personroles": ["Registered Manager"],
                            "persontitle": "Mr",
                        }
                    ],
                }
            ],
            [{"name": "Homecare agencies", "description": "Domiciliary care service"}],
            [{"name": "Services for everyone"}],
            {
                CQCL.overall: {
                    CQCL.organisation_id: None,
                    CQCL.rating: "Overall rating Excellent",
                    CQCL.report_date: "report_date",
                    CQCL.report_link_id: None,
                    CQCL.use_of_resources: {
                        CQCL.organisation_id: None,
                        CQCL.summary: None,
                        CQCL.use_of_resources_rating: None,
                        CQCL.combined_quality_summary: None,
                        CQCL.combined_quality_rating: None,
                        CQCL.report_date: None,
                        CQCL.report_link_id: None,
                    },
                    CQCL.key_question_ratings: [
                        {
                            CQCL.name: "Safe",
                            CQCL.rating: "Safe rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Well-led",
                            CQCL.rating: "Well-led rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Caring",
                            CQCL.rating: "Caring rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Responsive",
                            CQCL.rating: "Responsive rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Effective",
                            CQCL.rating: "Effective rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                    ],
                },
                CQCL.service_ratings: [
                    {
                        CQCL.name: None,
                        CQCL.rating: None,
                        CQCL.report_date: None,
                        CQCL.organisation_id: None,
                        CQCL.report_link_id: None,
                        CQCL.key_question_ratings: [
                            {
                                CQCL.name: None,
                                CQCL.rating: None,
                            },
                        ],
                    },
                ],
            },
            [
                {
                    CQCL.report_date: "report_date",
                    CQCL.report_link_id: None,
                    CQCL.organisation_id: None,
                    CQCL.service_ratings: [
                        {
                            CQCL.name: None,
                            CQCL.rating: None,
                            CQCL.key_question_ratings: [
                                {
                                    CQCL.name: None,
                                    CQCL.rating: None,
                                },
                            ],
                        },
                    ],
                    CQCL.overall: {
                        CQCL.rating: "Overall rating Excellent",
                        CQCL.use_of_resources: {
                            CQCL.combined_quality_rating: None,
                            CQCL.combined_quality_summary: None,
                            CQCL.use_of_resources_rating: None,
                            CQCL.use_of_resources_summary: None,
                        },
                        CQCL.key_question_ratings: [
                            {CQCL.name: "Safe", CQCL.rating: "Safe rating Good"},
                            {
                                CQCL.name: "Well-led",
                                CQCL.rating: "Well-led rating Good",
                            },
                            {CQCL.name: "Caring", CQCL.rating: "Caring rating Good"},
                            {
                                CQCL.name: "Responsive",
                                CQCL.rating: "Responsive rating Good",
                            },
                            {
                                CQCL.name: "Effective",
                                CQCL.rating: "Effective rating Good",
                            },
                        ],
                    },
                },
            ],
            [
                {
                    CQCL.related_location_id: "1",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                }
            ],
            "2020-01-01",
        ),
    ]

    impute_historic_relationships_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.registered, None),
        ("1-002", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.registered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        ("1-003", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
        ("1-004", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        ("1-005", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-005",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        ("1-006", date(2024, 1, 1), RegistrationStatus.deregistered, None),
        (
            "1-006",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        ("1-007", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-007",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 3, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    expected_impute_historic_relationships_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.registered, None, None),
        (
            "1-002",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.registered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        ("1-003", date(2024, 1, 1), RegistrationStatus.registered, None, None),
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
        (
            "1-004",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-005",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-005",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-006",
            date(2024, 1, 1),
            RegistrationStatus.deregistered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-006",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-007",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 3, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    impute_historic_relationships_when_type_is_none_returns_none_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.registered, None),
    ]
    expected_impute_historic_relationships_when_type_is_none_returns_none_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.registered, None, None),
    ]
    impute_historic_relationships_when_type_is_predecessor_returns_predecessor_rows = [
        ("1-002", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.registered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    expected_impute_historic_relationships_when_type_is_predecessor_returns_predecessor_rows = [
        (
            "1-002",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.registered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    impute_historic_relationships_when_type_is_successor_returns_none_when_registered_rows = [
        ("1-003", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
    ]
    expected_impute_historic_relationships_when_type_is_successor_returns_none_when_registered_rows = [
        ("1-003", date(2024, 1, 1), RegistrationStatus.registered, None, None),
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
    ]
    impute_historic_relationships_when_type_is_successor_returns_successor_when_deregistered_rows = [
        ("1-004", date(2024, 1, 1), RegistrationStatus.deregistered, None),
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
    ]
    expected_impute_historic_relationships_when_type_is_successor_returns_successor_when_deregistered_rows = [
        (
            "1-004",
            date(2024, 1, 1),
            RegistrationStatus.deregistered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
    ]
    impute_historic_relationships_when_type_has_both_types_only_returns_predecessors_when_registered_rows = [
        ("1-005", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-005",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]
    expected_impute_historic_relationships_when_type_has_both_types_only_returns_predecessors_when_registered_rows = [
        (
            "1-005",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-005",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]
    impute_historic_relationships_when_type_has_both_types_returns_original_values_when_deregistered_rows = [
        ("1-006", date(2024, 1, 1), RegistrationStatus.deregistered, None),
        (
            "1-006",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]
    expected_impute_historic_relationships_when_type_has_both_types_returns_original_values_when_deregistered_rows = [
        (
            "1-006",
            date(2024, 1, 1),
            RegistrationStatus.deregistered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-006",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]
    impute_historic_relationships_where_different_relationships_over_time_returns_first_found_rows = [
        ("1-007", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-007",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 3, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    expected_impute_historic_relationships_where_different_relationships_over_time_returns_first_found_rows = [
        (
            "1-007",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 3, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]

    get_relationships_where_type_is_none_returns_none_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.deregistered, None),
    ]
    expected_get_relationships_where_type_is_none_returns_none_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.deregistered, None, None),
    ]
    get_relationships_where_type_is_successor_returns_none_rows = [
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
    ]
    expected_get_relationships_where_type_is_successor_returns_none_rows = [
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
            None,
        ),
    ]
    get_relationships_where_type_is_predecessor_returns_predecessor_rows = [
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    expected_get_relationships_where_type_is_predecessor_returns_predecessor_rows = [
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    get_relationships_where_type_has_both_types_only_returns_predecessor_rows = [
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0042",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0043",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]
    expected_get_relationships_where_type_has_both_types_only_returns_predecessor_rows = [
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0042",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0043",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
            [
                {
                    CQCL.related_location_id: "1-0042",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0043",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]

    # fmt: off
    impute_missing_struct_column_rows = [
        ("1-001", date(2024, 1, 1), []),
        ("1-001", date(2024, 2, 1), None),
        ("1-001", date(2024, 3, 1), [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 4, 1), []),
        ("1-001", date(2024, 5, 1), None),
        ("1-001", date(2024, 6, 1), [{"name": "Name B", "description": "Desc B"}, {"name": "Name C", "description": "Desc C"}]),
        ("1-001", date(2024, 7, 1), None),
        ("1-001", date(2024, 8, 1), []),
        ("1-002", date(2024, 1, 1), []),
        ("1-002", date(2024, 2, 1), None),
    ]
    expected_impute_missing_struct_column_rows = [
        ("1-001", date(2024, 1, 1), [], [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 2, 1), None, [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 3, 1), [{"name": "Name A", "description": "Desc A"}], [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 4, 1), [], [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 5, 1), None, [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 6, 1), [{"name": "Name B", "description": "Desc B"}, {"name": "Name C", "description": "Desc C"}], [{"name": "Name B", "description": "Desc B"}, {"name": "Name C", "description": "Desc C"}]),
        ("1-001", date(2024, 7, 1), None, [{"name": "Name B", "description": "Desc B"}, {"name": "Name C", "description": "Desc C"}]),
        ("1-001", date(2024, 8, 1), [], [{"name": "Name B", "description": "Desc B"}, {"name": "Name C", "description": "Desc C"}]),
        ("1-002", date(2024, 1, 1), [], None),
        ("1-002", date(2024, 2, 1), None, None),
    ]
    # fmt: on

    remove_locations_that_never_had_regulated_activities_rows = [
        (
            "loc 1",
            [
                {
                    "name": "Personal care",
                    "code": "RA1",
                    "contacts": [
                        {
                            "personfamilyname": "Doe",
                            "persongivenname": "John",
                            "personroles": ["Registered Manager"],
                            "persontitle": "Mr",
                        }
                    ],
                }
            ],
        ),
        ("loc 2", None),
    ]
    expected_remove_locations_that_never_had_regulated_activities_rows = [
        (
            "loc 1",
            [
                {
                    "name": "Personal care",
                    "code": "RA1",
                    "contacts": [
                        {
                            "personfamilyname": "Doe",
                            "persongivenname": "John",
                            "personroles": ["Registered Manager"],
                            "persontitle": "Mr",
                        }
                    ],
                }
            ],
        ),
    ]

    extract_from_struct_rows = [
        (
            "1-001",
            [
                {
                    CQCL.name: "Homecare agencies",
                    CQCL.description: Services.domiciliary_care_service,
                }
            ],
        ),
        (
            "1-002",
            [
                {
                    CQCL.name: "With nursing",
                    CQCL.description: Services.care_home_service_with_nursing,
                },
                {
                    CQCL.name: "Without nursing",
                    CQCL.description: Services.care_home_service_without_nursing,
                },
            ],
        ),
        (
            "1-003",
            None,
        ),
    ]
    expected_extract_from_struct_rows = [
        (
            "1-001",
            [
                {
                    CQCL.name: "Homecare agencies",
                    CQCL.description: Services.domiciliary_care_service,
                },
            ],
            [Services.domiciliary_care_service],
        ),
        (
            "1-002",
            [
                {
                    CQCL.name: "With nursing",
                    CQCL.description: Services.care_home_service_with_nursing,
                },
                {
                    CQCL.name: "Without nursing",
                    CQCL.description: Services.care_home_service_without_nursing,
                },
            ],
            [
                Services.care_home_service_with_nursing,
                Services.care_home_service_without_nursing,
            ],
        ),
        (
            "1-003",
            None,
            None,
        ),
    ]

    primary_service_type_rows = [
        (
            "location1",
            "provider1",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                }
            ],
        ),
        (
            "location2",
            "provider2",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                }
            ],
        ),
        (
            "location3",
            "provider3",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                }
            ],
        ),
        (
            "location4",
            "provider4",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
            ],
        ),
        (
            "location5",
            "provider5",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Fake",
                    "description": "Fake service",
                },
            ],
        ),
        (
            "location6",
            "provider6",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
            ],
        ),
        (
            "location7",
            "provider7",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
            ],
        ),
        (
            "location8",
            "provider8",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
            ],
        ),
        (
            "location9",
            "provider9",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
            ],
        ),
        (
            "location10",
            "provider10",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
            ],
        ),
    ]
    expected_primary_service_type_rows = [
        (
            "location1",
            "provider1",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                }
            ],
            PrimaryServiceType.non_residential,
        ),
        (
            "location2",
            "provider2",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                }
            ],
            PrimaryServiceType.care_home_with_nursing,
        ),
        (
            "location3",
            "provider3",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                }
            ],
            PrimaryServiceType.care_home_only,
        ),
        (
            "location4",
            "provider4",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
            ],
            PrimaryServiceType.care_home_with_nursing,
        ),
        (
            "location5",
            "provider5",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Fake",
                    "description": "Fake service",
                },
            ],
            PrimaryServiceType.care_home_only,
        ),
        (
            "location6",
            "provider6",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
            ],
            PrimaryServiceType.care_home_with_nursing,
        ),
        (
            "location7",
            "provider7",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
            ],
            PrimaryServiceType.care_home_with_nursing,
        ),
        (
            "location8",
            "provider8",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
            ],
            PrimaryServiceType.care_home_only,
        ),
        (
            "location9",
            "provider9",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
            ],
            PrimaryServiceType.care_home_only,
        ),
        (
            "location10",
            "provider10",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
            ],
            PrimaryServiceType.care_home_with_nursing,
        ),
    ]

    realign_carehome_column_rows = [
        ("1", CareHome.care_home, PrimaryServiceType.care_home_only),
        ("2", CareHome.not_care_home, PrimaryServiceType.care_home_only),
        ("3", CareHome.care_home, PrimaryServiceType.care_home_with_nursing),
        ("4", CareHome.not_care_home, PrimaryServiceType.care_home_with_nursing),
        ("5", CareHome.care_home, PrimaryServiceType.non_residential),
        ("6", CareHome.not_care_home, PrimaryServiceType.non_residential),
    ]
    expected_realign_carehome_column_rows = [
        ("1", CareHome.care_home, PrimaryServiceType.care_home_only),
        ("2", CareHome.care_home, PrimaryServiceType.care_home_only),
        ("3", CareHome.care_home, PrimaryServiceType.care_home_with_nursing),
        ("4", CareHome.care_home, PrimaryServiceType.care_home_with_nursing),
        ("5", CareHome.not_care_home, PrimaryServiceType.non_residential),
        ("6", CareHome.not_care_home, PrimaryServiceType.non_residential),
    ]

    small_location_rows = [
        (
            "loc-1",
            "prov-1",
            "20200101",
        ),
        (
            "loc-2",
            "prov-1",
            "20200101",
        ),
        (
            "loc-3",
            "prov-2",
            "20200101",
        ),
        (
            "loc-4",
            "prov-2",
            "20210101",
        ),
    ]

    join_provider_rows = [
        (
            "prov-1",
            "Apple Tree Care Homes",
            "Local authority",
            date(2020, 1, 1),
        ),
        (
            "prov-2",
            "Sunshine Domestic Care",
            "Independent",
            date(2020, 1, 1),
        ),
        (
            "prov-3",
            "Sunny Days Domestic Care",
            "Independent",
            date(2020, 1, 1),
        ),
    ]

    expected_joined_rows = [
        (
            "loc-1",
            "prov-1",
            "Apple Tree Care Homes",
            "Local authority",
            date(2020, 1, 1),
            date(2020, 1, 1),
        ),
        (
            "loc-2",
            "prov-1",
            "Apple Tree Care Homes",
            "Local authority",
            date(2020, 1, 1),
            date(2020, 1, 1),
        ),
        (
            "loc-3",
            "prov-2",
            "Sunshine Domestic Care",
            "Independent",
            date(2020, 1, 1),
            date(2020, 1, 1),
        ),
        (
            "loc-4",
            "prov-2",
            "Sunshine Domestic Care",
            "Independent",
            date(2021, 1, 1),
            date(2020, 1, 1),
        ),
    ]

    test_invalid_postcode_data = [
        ("loc-1", "B69 E3G"),
        ("loc-2", "UB4 0EJ."),
        ("loc-3", "PO20 3BD"),
        ("loc-4", "PR! 9HL"),
        ("loc-5", None),
    ]

    expected_invalid_postcode_data = [
        ("loc-1", "B69 3EG"),
        ("loc-2", "UB4 0EJ"),
        ("loc-3", "PO20 3BD"),
        ("loc-4", "PR1 9HL"),
        ("loc-5", None),
    ]

    registration_status_with_missing_data_rows = [
        (
            "loc-1",
            "Registered",
        ),
        (
            "loc-2",
            "Deregistered",
        ),
        (
            "loc-3",
            "new value",
        ),
    ]

    registration_status_rows = [
        (
            "loc-1",
            "Registered",
        ),
        (
            "loc-2",
            "Deregistered",
        ),
    ]

    expected_registered_rows = [
        (
            "loc-1",
            "Registered",
        ),
    ]

    social_care_org_rows = [
        (
            "loc-1",
            "Any none ASC org",
        ),
        (
            "loc-2",
            "Social Care Org",
        ),
        (
            "loc-3",
            None,
        ),
    ]

    expected_social_care_org_rows = [
        (
            "loc-2",
            "Social Care Org",
        ),
    ]

    ons_postcode_directory_rows = [
        (
            "LS12AB",
            date(2021, 1, 1),
            "Leeds",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "Leeds",
            "Yorkshire & Humber",
        ),
        (
            "B693EG",
            date(2021, 1, 1),
            "York",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "York",
            "Yorkshire & Humber",
        ),
        (
            "PR19HL",
            date(2019, 1, 1),
            "Hull",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "East Riding of Yorkshire",
            "Yorkshire & Humber",
        ),
    ]

    locations_for_ons_join_rows = [
        ("loc-1", "prov-1", date(2020, 1, 1), "PR1 9AB", "Registered"),
        ("loc-2", "prov-1", date(2018, 1, 1), "B69 3EG", "Deregistered"),
        (
            "loc-3",
            "prov-2",
            date(2020, 1, 1),
            "PR1 9HL",
            "Deregistered",
        ),
        ("loc-4", "prov-2", date(2021, 1, 1), "LS1 2AB", "Registered"),
    ]

    expected_ons_join_with_null_rows = [
        (
            date(2019, 1, 1),
            "PR19AB",
            date(2020, 1, 1),
            "loc-1",
            "prov-1",
            None,
            None,
            None,
            None,
            None,
            "Registered",
        ),
        (
            None,
            "B693EG",
            date(2018, 1, 1),
            "loc-2",
            "prov-1",
            None,
            None,
            None,
            None,
            None,
            "Deregistered",
        ),
        (
            date(2019, 1, 1),
            "PR19HL",
            date(2020, 1, 1),
            "loc-3",
            "prov-2",
            "Hull",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "East Riding of Yorkshire",
            "Yorkshire & Humber",
            "Deregistered",
        ),
        (
            date(2021, 1, 1),
            "LS12AB",
            date(2021, 1, 1),
            "loc-4",
            "prov-2",
            "Leeds",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "Leeds",
            "Yorkshire & Humber",
            "Registered",
        ),
    ]

    expected_split_registered_no_nulls_rows = [
        (
            date(2019, 1, 1),
            "PR19AB",
            date(2020, 1, 1),
            "loc-1",
            "prov-1",
            "Somerset",
            "Oxen Lane",
            date(2021, 1, 1),
            "Somerset",
            "English Region",
            "Registered",
        ),
        (
            date(2021, 1, 1),
            "LS12AB",
            date(2021, 1, 1),
            "loc-4",
            "prov-2",
            "Leeds",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "Leeds",
            "Yorkshire & Humber",
            "Registered",
        ),
    ]

    # fmt: off
    remove_time_from_date_column_rows = [
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2018-01-01 00:00:00", "20231201", "2018-01-01 00:00:00"),
        ("loc_1", None, "20231101", None),
    ]
    expected_remove_time_from_date_column_rows = [
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2018-01-01 00:00:00", "20231201", "2018-01-01"),
        ("loc_1", None, "20231101", None),
    ]
    remove_late_registration_dates_rows = [
        ("loc_1", "20240101", "2024-01-01"),
        ("loc_2", "20240101", "2024-01-02"),
        ("loc_3", "20240101", "2023-12-31"),
        ("loc_4", "20240201", "2024-02-02"),
        ("loc_4", "20240301", "2024-02-02"),
    ]
    expected_remove_late_registration_dates_rows = [
        ("loc_1", "20240101", "2024-01-01"),
        ("loc_2", "20240101", None),
        ("loc_3", "20240101", "2023-12-31"),
        ("loc_4", "20240201", None),
        ("loc_4", "20240301", None),
    ]
    clean_registration_date_column_rows = [
        ("loc_1", "2018-01-01", "20240101"),
        ("loc_1", "2018-01-01 00:00:00", "20231201"),
        ("loc_1", None, "20231101"),
        ("loc_2", None, "20240101"),
        ("loc_2", None, "20231201"),
        ("loc_2", None, "20231101"),
    ]
    expected_clean_registration_date_column_rows = [
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2018-01-01 00:00:00", "20231201", "2018-01-01"),
        ("loc_1", None, "20231101", "2018-01-01"),
        ("loc_2", None, "20240101", "2023-11-01"),
        ("loc_2", None, "20231201", "2023-11-01"),
        ("loc_2", None, "20231101", "2023-11-01"),
    ]
    impute_missing_registration_dates_rows=expected_remove_time_from_date_column_rows
    expected_impute_missing_registration_dates_rows=[
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2018-01-01 00:00:00", "20231201", "2018-01-01"),
        ("loc_1", None, "20231101", "2018-01-01"),
    ]
    impute_missing_registration_dates_different_rows=[
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2017-01-01 00:00:00", "20231201", "2017-01-01"),
        ("loc_1", None, "20231101", None),
    ]
    expected_impute_missing_registration_dates_different_rows=[
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2017-01-01 00:00:00", "20231201", "2017-01-01"),
        ("loc_1", None, "20231101", "2017-01-01"),
    ]
    impute_missing_registration_dates_missing_rows=[
        ("loc_2", None, "20240101", None),
        ("loc_2", None, "20231201", None),
        ("loc_2", None, "20231101", None),
    ]
    expected_impute_missing_registration_dates_missing_rows=[
        ("loc_2", None, "20240101", "2023-11-01"),
        ("loc_2", None, "20231201", "2023-11-01"),
        ("loc_2", None, "20231101", "2023-11-01"),
    ]
    # fmt: on

    calculate_time_registered_same_day_rows = [
        ("1-0001", date(2025, 1, 1), date(2025, 1, 1)),
    ]
    expected_calculate_time_registered_same_day_rows = [
        ("1-0001", date(2025, 1, 1), date(2025, 1, 1), 1),
    ]

    calculate_time_registered_exact_months_apart_rows = [
        ("1-0001", date(2024, 2, 1), date(2024, 1, 1)),
        ("1-0002", date(2020, 1, 1), date(2019, 1, 1)),
    ]
    expected_calculate_time_registered_exact_months_apart_rows = [
        ("1-0001", date(2024, 2, 1), date(2024, 1, 1), 2),
        ("1-0002", date(2020, 1, 1), date(2019, 1, 1), 13),
    ]

    calculate_time_registered_one_day_less_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 1), date(2024, 12, 2)),
        ("1-0002", date(2025, 6, 8), date(2025, 1, 9)),
    ]
    expected_calculate_time_registered_one_day_less_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 1), date(2024, 12, 2), 1),
        ("1-0002", date(2025, 6, 8), date(2025, 1, 9), 5),
    ]

    calculate_time_registered_one_day_more_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 2), date(2024, 12, 1)),
        ("1-0002", date(2025, 6, 1), date(2025, 1, 31)),
    ]
    expected_calculate_time_registered_one_day_more_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 2), date(2024, 12, 1), 2),
        ("1-0002", date(2025, 6, 1), date(2025, 1, 31), 5),
    ]

    clean_provider_id_column_rows = [
        ("loc_1", None, "20240101"),
        ("loc_1", "123456789", "20240201"),
        ("loc_1", None, "20240201"),
        ("loc_2", "223456789 223456789", "20240101"),
        ("loc_2", "223456789", "20240201"),
        ("loc_2", None, "20240301"),
    ]
    expected_clean_provider_id_column_rows = [
        ("loc_1", "123456789", "20240101"),
        ("loc_1", "123456789", "20240201"),
        ("loc_1", "123456789", "20240201"),
        ("loc_2", "223456789", "20240101"),
        ("loc_2", "223456789", "20240201"),
        ("loc_2", "223456789", "20240301"),
    ]
    long_provider_id_column_rows = [
        ("loc_2", "223456789 223456789", "20240101"),
        ("loc_2", "223456789", "20240201"),
        ("loc_2", None, "20240301"),
    ]
    expected_long_provider_id_column_rows = [
        ("loc_2", None, "20240101"),
        ("loc_2", "223456789", "20240201"),
        ("loc_2", None, "20240301"),
    ]
    fill_missing_provider_id_column_rows = [
        ("loc_1", None, "20240101"),
        ("loc_1", "123456789", "20240201"),
        ("loc_1", None, "20240201"),
    ]

    expected_fill_missing_provider_id_column_rows = [
        ("loc_1", "123456789", "20240101"),
        ("loc_1", "123456789", "20240201"),
        ("loc_1", "123456789", "20240201"),
    ]

    impute_missing_data_from_provider_dataset_single_value_rows = [
        (
            "prov_1",
            None,
            date(2024, 1, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 2, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 3, 1),
        ),
    ]

    expected_impute_missing_data_from_provider_dataset_rows = [
        (
            "prov_1",
            Sector.independent,
            date(2024, 1, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 2, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 3, 1),
        ),
    ]

    impute_missing_data_from_provider_dataset_multiple_values_rows = [
        (
            "prov_1",
            None,
            date(2024, 1, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 3, 1),
        ),
        (
            "prov_1",
            Sector.local_authority,
            date(2024, 2, 1),
        ),
    ]

    expected_impute_missing_data_from_provider_dataset_multiple_values_rows = [
        (
            "prov_1",
            Sector.local_authority,
            date(2024, 1, 1),
        ),
        (
            "prov_1",
            Sector.local_authority,
            date(2024, 2, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 3, 1),
        ),
    ]
    test_only_service_specialist_colleges_rows = [
        (
            "loc 1",
            [Services.specialist_college_service],
        ),
        (
            "loc 4",
            [Services.care_home_service_with_nursing],
        ),
    ]
    test_multiple_services_specialist_colleges_rows = [
        (
            "loc 2",
            [
                Services.specialist_college_service,
                Services.acute_services_with_overnight_beds,
            ],
        ),
        (
            "loc 3",
            [
                Services.acute_services_with_overnight_beds,
                Services.specialist_college_service,
            ],
        ),
    ]
    test_without_specialist_colleges_rows = [
        (
            "loc 4",
            [Services.care_home_service_with_nursing],
        ),
    ]
    test_empty_array_specialist_colleges_rows = [
        (
            "loc 5",
            [],
        ),
    ]
    test_null_row_specialist_colleges_rows = [
        (
            "loc 6",
            None,
        ),
    ]
    expected_only_service_specialist_colleges_rows = [
        (
            "loc 4",
            [Services.care_home_service_with_nursing],
        ),
    ]
    expected_multiple_services_specialist_colleges_rows = (
        test_multiple_services_specialist_colleges_rows
    )
    expected_without_specialist_colleges_rows = test_without_specialist_colleges_rows
    expected_empty_array_specialist_colleges_rows = (
        test_empty_array_specialist_colleges_rows
    )
    expected_null_row_specialist_colleges_rows = test_null_row_specialist_colleges_rows

    add_related_location_column_rows = [
        ("loc 1", None),
        ("loc 2", []),
        (
            "loc 3",
            [
                {
                    CQCL.related_location_id: "1",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                }
            ],
        ),
        (
            "loc 4",
            [
                {
                    CQCL.related_location_id: "1",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                },
                {
                    CQCL.related_location_id: "2",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                },
            ],
        ),
    ]
    expected_add_related_location_column_rows = [
        ("loc 1", None, RelatedLocation.no_related_location),
        ("loc 2", [], RelatedLocation.no_related_location),
        (
            "loc 3",
            [
                {
                    CQCL.related_location_id: "1",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                }
            ],
            RelatedLocation.has_related_location,
        ),
        (
            "loc 4",
            [
                {
                    CQCL.related_location_id: "1",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                },
                {
                    CQCL.related_location_id: "2",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                },
            ],
            RelatedLocation.has_related_location,
        ),
    ]

    calculate_time_since_dormant_rows = [
        ("1-001", date(2025, 1, 1), None),
        ("1-001", date(2025, 2, 1), Dormancy.not_dormant),
        ("1-001", date(2025, 3, 1), Dormancy.dormant),
        ("1-001", date(2025, 4, 1), Dormancy.dormant),
        ("1-001", date(2025, 5, 1), Dormancy.not_dormant),
        ("1-001", date(2025, 6, 1), Dormancy.dormant),
        ("1-001", date(2025, 7, 1), Dormancy.not_dormant),
        ("1-001", date(2025, 8, 1), Dormancy.not_dormant),
        ("1-001", date(2025, 9, 1), None),
        ("1-002", date(2025, 10, 1), Dormancy.not_dormant),
    ]
    expected_calculate_time_since_dormant_rows = [
        ("1-001", date(2025, 1, 1), None, None),
        ("1-001", date(2025, 2, 1), Dormancy.not_dormant, None),
        ("1-001", date(2025, 3, 1), Dormancy.dormant, 1),
        ("1-001", date(2025, 4, 1), Dormancy.dormant, 1),
        ("1-001", date(2025, 5, 1), Dormancy.not_dormant, 2),
        ("1-001", date(2025, 6, 1), Dormancy.dormant, 1),
        ("1-001", date(2025, 7, 1), Dormancy.not_dormant, 2),
        ("1-001", date(2025, 8, 1), Dormancy.not_dormant, 3),
        ("1-001", date(2025, 9, 1), None, 4),
        ("1-002", date(2025, 10, 1), Dormancy.not_dormant, None),
    ]


@dataclass
class CQCProviderData:
    sample_rows_full = [
        (
            "1-10000000001",
            ["1-12000000001"],
            "Provider",
            "Organisation",
            "Independent Healthcare Org",
            "10000000001",
            "Care Solutions Direct Limited",
            "Registered",
            "2022-01-14",
            None,
            "Threefield House",
            "Southampton",
            None,
            "South East",
            "AA10 3LP",
            50.93761444091797,
            -1.452439546585083,
            "0238206106",
            "10000001",
            "Adult social care",
            "Southampton, Itchen",
            "Southampton",
            "20230405",
        ),
        (
            "1-10000000002",
            ["1-12000000002"],
            "Provider",
            "Partnership",
            "Social Care Org",
            "10000000002",
            "Care Solutions Direct Limited",
            "Registered",
            "2022-01-14",
            None,
            "Threefield House",
            "Southampton",
            "Some County",
            "South East",
            "AA10 3LP",
            50.93761444091797,
            -1.452439546585083,
            "0238206106",
            "10000002",
            "Adult social care",
            "Southampton, Itchen",
            "Southampton",
            "20230405",
        ),
        (
            "1-10000000003",
            ["1-12000000003"],
            "Provider",
            "Individual",
            "Social Care Org",
            "10000000003",
            "Care Solutions Direct Limited",
            "Deregistered",
            "2022-01-14",
            "2022-03-07",
            "Threefield House",
            "Southampton",
            None,
            "South East",
            "SO14 3LP",
            50.93761444091797,
            -1.452439546585083,
            "0238206106",
            "10000003",
            "Adult social care",
            "Southampton, Itchen",
            "Southampton",
            "20230405",
        ),
    ]

    sector_rows = [
        "1-10000000002",
        "1-10000000003",
        "1-10000000004",
        "1-10000000005",
    ]

    rows_without_cqc_sector = [
        ("1-10000000001", "data"),
        ("1-10000000002", None),
        ("1-10000000003", "data"),
    ]

    expected_rows_with_cqc_sector = [
        ("1-10000000001", "data", Sector.independent),
        ("1-10000000002", None, Sector.local_authority),
        ("1-10000000003", "data", Sector.local_authority),
    ]


@dataclass
class ValidateLocationsAPICleanedData:
    # fmt: off
    raw_cqc_locations_rows = [
        ("1-000000001", "1-001", "20240101", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000002", "1-001", "20240101", LocationType.social_care_identifier, RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000001", "1-001", "20240201", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000002", "1-001", "20240201", "not social care org", RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
    ]
    # fmt: on

    # fmt: off
    cleaned_cqc_locations_rows = [
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
    ]
    # fmt: on


    # fmt: off
    calculate_expected_size_rows = [
        ("loc_1", "prov_1", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_2", "prov_1", "non social care org", RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_3", "prov_1", None, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_4", "prov_1", LocationType.social_care_identifier, RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_5", "prov_1", "non social care org", RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_6", "prov_1", None, RegistrationStatus.deregistered,  [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_7", "prov_1", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_8", "prov_1", "non social care org", RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_9", "prov_1", None, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_10", "prov_1", LocationType.social_care_identifier, RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_11", "prov_1", "non social care org", RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_12", "prov_1", None, RegistrationStatus.deregistered,  [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_13", "prov_1", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], None),
        ("loc_14", None, LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        (RecordsToRemoveInLocationsData.dental_practice, "prov_1", LocationType.social_care_identifier, RegistrationStatus.registered, None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        (RecordsToRemoveInLocationsData.temp_registration, "prov_1", LocationType.social_care_identifier, RegistrationStatus.registered, None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
    ]
    # fmt: on

    identify_if_location_has_a_known_value_when_array_type_rows = [
        ("loc_1", []),
        ("loc_1", [{CQCL.name: "name", CQCL.code: "A1"}]),
        ("loc_1", None),
        ("loc_2", []),
        ("loc_3", None),
    ]
    expected_identify_if_location_has_a_known_value_when_array_type_rows = [
        ("loc_1", [], True),
        ("loc_1", [{CQCL.name: "name", CQCL.code: "A1"}], True),
        ("loc_1", None, True),
        ("loc_2", [], False),
        ("loc_3", None, False),
    ]

    identify_if_location_has_a_known_value_when_not_array_type_rows = [
        ("loc_1", None),
        ("loc_1", "prov_1"),
        ("loc_1", None),
        ("loc_2", None),
    ]
    expected_identify_if_location_has_a_known_value_when_not_array_type_rows = [
        ("loc_1", None, True),
        ("loc_1", "prov_1", True),
        ("loc_1", None, True),
        ("loc_2", None, False),
    ]


@dataclass
class ValidateProvidersAPICleanedData:
    raw_cqc_providers_rows = [
        ("1-000000001", "20240101"),
        ("1-000000002", "20240101"),
        ("1-000000001", "20240201"),
        ("1-000000002", "20240201"),
    ]
    cleaned_cqc_providers_rows = [
        ("1-000000001", date(2024, 1, 1), "name", Sector.independent),
        ("1-000000002", date(2024, 1, 1), "name", Sector.independent),
        ("1-000000001", date(2024, 1, 9), "name", Sector.independent),
        ("1-000000002", date(2024, 1, 9), "name", Sector.independent),
    ]

    calculate_expected_size_rows = raw_cqc_providers_rows
