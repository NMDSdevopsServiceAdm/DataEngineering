from dataclasses import dataclass
from datetime import date

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResColumns as CTNR,
)


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
class ONSData:
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
