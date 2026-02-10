import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path


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

    column_to_date_string_with_hyphens_rows = [
        ("2023-01-02", "2022-05-04", "2019-12-07", "1908-12-05"),
    ]
    column_to_date_string_without_hyphens_rows = [
        ("20230102", "20220504", "20191207", "19081205"),
    ]
    column_to_date_integer_without_hyphens_rows = [
        (20230102, 20220504, 20191207, 19081205),
    ]
    expected_column_to_date_rows = [
        (date(2023, 1, 2), date(2022, 5, 4), date(2019, 12, 7), date(1908, 12, 5)),
    ]

    column_to_date_with_new_col_rows = [
        ("20230102", "20220504", "20191207", "19081205"),
    ]
    expected_column_to_date_with_new_col_rows = [
        ("20230102", "20220504", "20191207", "19081205"),
        (date(2023, 1, 2), date(2022, 5, 4), date(2019, 12, 7), date(1908, 12, 5)),
    ]

    reduce_dataset_to_earliest_file_per_month_rows = [
        ("loc 1", "20220101", "2022", "01", "01"),
        ("loc 2", "20220105", "2022", "01", "05"),
        ("loc 3", "20220205", "2022", "02", "05"),
        ("loc 4", "20220207", "2022", "02", "07"),
        ("loc 5", "20220301", "2022", "03", "01"),
        ("loc 6", "20220402", "2022", "04", "02"),
    ]
    expected_reduce_dataset_to_earliest_file_per_month_rows = [
        ("loc 1", "20220101", "2022", "01", "01"),
        ("loc 3", "20220205", "2022", "02", "05"),
        ("loc 5", "20220301", "2022", "03", "01"),
        ("loc 6", "20220402", "2022", "04", "02"),
    ]


@dataclass
class RawDataAdjustmentsData:
    CONFIG = Path(__file__).parent.parent / "polars_utils" / "exclusions.json"
    EXCLUSIONS = json.loads(CONFIG.read_text())

    invalid_locations_list = EXCLUSIONS["locationId"].values()
    invalid_locations_list_tuples = [(i, "other") for i in invalid_locations_list]

    locations_data_with_multiple_rows_to_remove = (
        [("loc_1", "other")]
        + invalid_locations_list_tuples
        + invalid_locations_list_tuples
    )

    locations_data_with_only_rows_to_remove = (
        invalid_locations_list_tuples + invalid_locations_list_tuples
    )

    locations_data_without_rows_to_remove = [
        ("loc_1", "other"),
    ]

    expected_locations_data = locations_data_without_rows_to_remove
