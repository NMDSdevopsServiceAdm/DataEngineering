from dataclasses import dataclass
from datetime import date


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
