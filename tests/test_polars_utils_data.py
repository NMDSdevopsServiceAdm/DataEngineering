import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from utils.column_values.categorical_column_values import (
    CareHome,
)


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

    filled_posts_per_bed_ratio_rows = [
        ("1-000000001", 5.0, 100, CareHome.care_home),
        ("1-000000002", 2.0, 1, CareHome.care_home),
        ("1-000000003", None, 100, CareHome.care_home),
        ("1-000000004", 0.0, 1, CareHome.care_home),
        ("1-000000005", 5.0, None, CareHome.care_home),
        ("1-000000006", 2.0, 0, CareHome.care_home),
        ("1-000000007", None, 0, CareHome.care_home),
        ("1-000000008", 0.0, None, CareHome.care_home),
        ("1-000000009", None, None, CareHome.care_home),
        ("1-000000010", 0.0, 0, CareHome.care_home),
        ("1-000000011", 4.0, 10, CareHome.not_care_home),
    ]
    expected_filled_posts_per_bed_ratio_rows = [
        ("1-000000001", 5.0, 100, CareHome.care_home, 0.05),
        ("1-000000002", 2.0, 1, CareHome.care_home, 2.0),
        ("1-000000003", None, 100, CareHome.care_home, None),
        ("1-000000004", 0.0, 1, CareHome.care_home, 0.0),
        ("1-000000005", 5.0, None, CareHome.care_home, None),
        ("1-000000006", 2.0, 0, CareHome.care_home, None),
        ("1-000000007", None, 0, CareHome.care_home, None),
        ("1-000000008", 0.0, None, CareHome.care_home, None),
        ("1-000000009", None, None, CareHome.care_home, None),
        ("1-000000010", 0.0, 0, CareHome.care_home, None),
        ("1-000000011", 4.0, 10, CareHome.not_care_home, None),
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


@dataclass
class CalculateWindowedColumnData:
    calculate_new_column_rows = [
        ("1-001", 10.0, 2.5),
        ("1-002", 2.0, 4.0),
        ("1-003", None, 2.5),
        ("1-004", 10.0, None),
        ("1-005", None, None),
    ]
    expected_calculate_new_column_plus_rows = [
        ("1-001", 10.0, 2.5, 12.5),
        ("1-002", 2.0, 4.0, 6.0),
        ("1-003", None, 2.5, None),
        ("1-004", 10.0, None, None),
        ("1-005", None, None, None),
    ]
    expected_calculate_new_column_minus_rows = [
        ("1-001", 10.0, 2.5, 7.5),
        ("1-002", 2.0, 4.0, -2.0),
        ("1-003", None, 2.5, None),
        ("1-004", 10.0, None, None),
        ("1-005", None, None, None),
    ]
    expected_calculate_new_column_multipy_rows = [
        ("1-001", 10.0, 2.5, 25.0),
        ("1-002", 2.0, 4.0, 8.0),
        ("1-003", None, 2.5, None),
        ("1-004", 10.0, None, None),
        ("1-005", None, None, None),
    ]
    expected_calculate_new_column_divide_rows = [
        ("1-001", 10.0, 2.5, 4.0),
        ("1-002", 2.0, 4.0, 0.5),
        ("1-003", None, 2.5, None),
        ("1-004", 10.0, None, None),
        ("1-005", None, None, None),
    ]
    expected_calculate_new_column_average_rows = [
        ("1-001", 10.0, 2.5, 6.25),
        ("1-002", 2.0, 4.0, 3.0),
        ("1-003", None, 2.5, None),
        ("1-004", 10.0, None, None),
        ("1-005", None, None, None),
    ]
    expected_calculate_new_column_absolute_difference_rows = [
        ("1-001", 10.0, 2.5, 7.5),
        ("1-002", 2.0, 4.0, 2.0),
        ("1-003", None, 2.5, None),
        ("1-004", 10.0, None, None),
        ("1-005", None, None, None),
    ]
    calculate_new_column_with_when_clause_rows = [
        ("1-001", 10.0, 2.5),
        ("1-002", 20.0, 2.5),
    ]
    expected_calculate_new_column_with_when_clause_rows = [
        ("1-001", 10.0, 2.5, 12.5),
        ("1-002", 20.0, 2.5, None),
    ]

    calculate_windowed_column_rows = [
        ("1-001", date(2025, 1, 1), "Y", 1.0),
        ("1-002", date(2025, 2, 1), "Y", 3.0),
        ("1-003", date(2025, 3, 1), "Y", 2.0),
        ("1-004", date(2025, 1, 1), "N", 5.0),
        ("1-005", date(2025, 2, 1), "N", None),
        ("1-006", date(2025, 3, 1), "N", 15.0),
        ("1-007", date(2025, 1, 1), "N", 10.0),
    ]
    expected_calculate_windowed_column_avg_rows = [
        ("1-001", date(2025, 1, 1), "Y", 1.0, 2.0),
        ("1-002", date(2025, 2, 1), "Y", 3.0, 2.0),
        ("1-003", date(2025, 3, 1), "Y", 2.0, 2.0),
        ("1-004", date(2025, 1, 1), "N", 5.0, 10.0),
        ("1-005", date(2025, 2, 1), "N", None, 10.0),
        ("1-006", date(2025, 3, 1), "N", 15.0, 10.0),
        ("1-007", date(2025, 1, 1), "N", 10.0, 10.0),
    ]
    expected_calculate_windowed_column_count_rows = [
        ("1-001", date(2025, 1, 1), "Y", 1.0, 3),
        ("1-002", date(2025, 2, 1), "Y", 3.0, 3),
        ("1-003", date(2025, 3, 1), "Y", 2.0, 3),
        ("1-004", date(2025, 1, 1), "N", 5.0, 3),
        ("1-005", date(2025, 2, 1), "N", None, 3),
        ("1-006", date(2025, 3, 1), "N", 15.0, 3),
        ("1-007", date(2025, 1, 1), "N", 10.0, 3),
    ]
    expected_calculate_windowed_column_max_rows = [
        ("1-001", date(2025, 1, 1), "Y", 1.0, 3.0),
        ("1-002", date(2025, 2, 1), "Y", 3.0, 3.0),
        ("1-003", date(2025, 3, 1), "Y", 2.0, 3.0),
        ("1-004", date(2025, 1, 1), "N", 5.0, 15.0),
        ("1-005", date(2025, 2, 1), "N", None, 15.0),
        ("1-006", date(2025, 3, 1), "N", 15.0, 15.0),
        ("1-007", date(2025, 1, 1), "N", 10.0, 15.0),
    ]
    expected_calculate_windowed_column_min_rows = [
        ("1-001", date(2025, 1, 1), "Y", 1.0, 1.0),
        ("1-002", date(2025, 2, 1), "Y", 3.0, 1.0),
        ("1-003", date(2025, 3, 1), "Y", 2.0, 1.0),
        ("1-004", date(2025, 1, 1), "N", 5.0, 5.0),
        ("1-005", date(2025, 2, 1), "N", None, 5.0),
        ("1-006", date(2025, 3, 1), "N", 15.0, 5.0),
        ("1-007", date(2025, 1, 1), "N", 10.0, 5.0),
    ]
    expected_calculate_windowed_column_sum_rows = [
        ("1-001", date(2025, 1, 1), "Y", 1.0, 6.0),
        ("1-002", date(2025, 2, 1), "Y", 3.0, 6.0),
        ("1-003", date(2025, 3, 1), "Y", 2.0, 6.0),
        ("1-004", date(2025, 1, 1), "N", 5.0, 30.0),
        ("1-005", date(2025, 2, 1), "N", None, 30.0),
        ("1-006", date(2025, 3, 1), "N", 15.0, 30.0),
        ("1-007", date(2025, 1, 1), "N", 10.0, 30.0),
    ]
