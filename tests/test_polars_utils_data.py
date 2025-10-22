from dataclasses import dataclass
from datetime import date


@dataclass
class CleaningUtilsData:
    align_dates_primary_rows = [
        (date(2019, 1, 1), date(2021, 1, 15), date(2020, 1, 8), date(2021, 1, 15)),
        ("1-001", "1-001", "1-001", "1-002"),
    ]

    align_dates_secondary_rows = [
        (
            date(2020, 1, 1),
            date(2021, 1, 1),
            date(2020, 1, 5),
            date(2020, 1, 1),
            date(2021, 1, 5),
            date(2022, 1, 1),
        ),
        ("123", "123", "123", "456", "789", "789"),
    ]
    expected_align_dates_primary_with_secondary_rows = [
        (date(2019, 1, 1), date(2020, 1, 8), date(2021, 1, 15), date(2021, 1, 15)),
        ("1-001", "1-001", "1-001", "1-002"),
        (None, date(2020, 1, 5), date(2021, 1, 5), date(2021, 1, 5)),
    ]

    align_dates_primary_single_row = [(date(2025, 5, 1),), ("1-001",)]

    align_dates_secondary_exact_match_rows = [(date(2025, 5, 1),), ("123",)]
    expected_align_dates_secondary_exact_match_rows = [
        (date(2025, 5, 1),),
        ("1-001",),
        (date(2025, 5, 1),),
    ]

    align_dates_secondary_closest_historical_rows = [
        (date(2024, 4, 4), date(2023, 3, 3), date(2025, 5, 5)),
        ("123", "123", "123"),
    ]
    expected_align_dates_secondary_closest_historical_rows = [
        (date(2025, 5, 1),),
        ("1-001",),
        (date(2024, 4, 4),),
    ]

    align_dates_secondary_future_rows = [(date(2025, 5, 5),), ("123",)]
    expected_align_dates_secondary_future_rows = [
        (date(2025, 5, 1),),
        ("1-001",),
        (None,),
    ]

    worker_rows = [
        ("1", "2", "3", "4", "5", "6"),
        ("1", "1", "2", "2", None, "2"),
        ("100", "101", "102", "103", "103", None),
    ]
    gender = {
        "1": "male",
        "2": "female",
    }
    nationality = {
        "100": "British",
        "101": "French",
        "102": "Spanish",
        "103": "Portuguese",
    }
    expected_rows_with_new_columns = [
        ("1", "2", "3", "4", "5", "6"),
        ("1", "1", "2", "2", None, "2"),
        ("100", "101", "102", "103", "103", None),
        ("male", "male", "female", "female", None, "female"),
        ("British", "French", "Spanish", "Portuguese", "Portuguese", None),
    ]
    expected_rows_without_new_columns = [
        ("1", "2", "3", "4", "5", "6"),
        ("male", "male", "female", "female", None, "female"),
        ("British", "French", "Spanish", "Portuguese", "Portuguese", None),
    ]
    expected_rows_with_1_new_column_and_1_custom_column_name = [
        ("1", "2", "3", "4", "5", "6"),
        ("1", "1", "2", "2", None, "2"),
        ("100", "101", "102", "103", "103", None),
        ("male", "male", "female", "female", None, "female"),
    ]
    expected_rows_with_2_new_columns_and_2_custom_column_names = [
        ("1", "2", "3", "4", "5", "6"),
        ("1", "1", "2", "2", None, "2"),
        ("100", "101", "102", "103", "103", None),
        ("male", "male", "female", "female", None, "female"),
        ("British", "French", "Spanish", "Portuguese", "Portuguese", None),
    ]
