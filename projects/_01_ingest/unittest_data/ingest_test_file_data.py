from dataclasses import dataclass
from datetime import date

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResColumns as CTNR,
)
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
        ("1-001", date(2020, 1, 1), "AA1 1aa"),
        ("1-001", date(2025, 1, 1), "AA1 1aa"),  # lower case but matches ok
        ("1-002", date(2020, 1, 1), "AA1 ZAA"),  # wrong now but amended later (match to the first known one, not the second)
        ("1-002", date(2025, 1, 1), "AA1 2AA"),
        ("1-002", date(2025, 1, 1), "AA1 3AA"),
        ("1-003", date(2025, 1, 1), "29 5HF"),  # known issue (actually need one from the invalid list here)
        ("1-004", date(2025, 1, 1), "AA1 4ZZ"),  # match this in truncated
    ]
    locations_with_unmatched_postcode_rows = [
        ("1-001", date(2020, 1, 1), "AA1 1aa"),
        ("1-001", date(2025, 1, 1), "AA1 1aa"),
        ("1-005", date(2025, 1, 1), "AA2 5XX"),  # never known
    ]
    # fmt: on

    postcodes_rows = [
        ("AA11AA", date(2020, 1, 1), "CSSR 1"),
        ("AA12AA", date(2020, 1, 1), "CSSR 2"),
        ("AA13AA", date(2020, 1, 1), "CSSR 3"),
        ("AA11AA", date(2025, 1, 1), "CSSR 1"),
        ("AA12AA", date(2025, 1, 1), "CSSR 2"),
        ("AA13AA", date(2025, 1, 1), "CSSR 3"),
    ]

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
