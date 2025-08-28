from datetime import date


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
# fmt: off
expected_calculate_windowed_column_avg_rows = [
    ("1-001", date(2025, 1, 1), "Y", 1.0, 1.0),
    ("1-002", date(2025, 2, 1), "Y", 3.0, 2.0),
    ("1-003", date(2025, 3, 1), "Y", 2.0, 2.0),
    ("1-004", date(2025, 1, 1), "N", 5.0, 7.5),
    ("1-005", date(2025, 2, 1), "N", None, 7.5),
    ("1-006", date(2025, 3, 1), "N", 15.0, 10.0),
    ("1-007", date(2025, 1, 1), "N", 10.0, 7.5),
]
expected_calculate_windowed_column_count_rows = [
    ("1-001", date(2025, 1, 1), "Y", 1.0, 1),
    ("1-002", date(2025, 2, 1), "Y", 3.0, 2),
    ("1-003", date(2025, 3, 1), "Y", 2.0, 3),
    ("1-004", date(2025, 1, 1), "N", 5.0, 2),
    ("1-005", date(2025, 2, 1), "N", None, 2),
    ("1-006", date(2025, 3, 1), "N", 15.0, 3),
    ("1-007", date(2025, 1, 1), "N", 10.0, 2),
]
expected_calculate_windowed_column_max_rows = [
    ("1-001", date(2025, 1, 1), "Y", 1.0, 1.0),
    ("1-002", date(2025, 2, 1), "Y", 3.0, 3.0),
    ("1-003", date(2025, 3, 1), "Y", 2.0, 3.0),
    ("1-004", date(2025, 1, 1), "N", 5.0, 10.0),
    ("1-005", date(2025, 2, 1), "N", None, 10.0),
    ("1-006", date(2025, 3, 1), "N", 15.0, 15.0),
    ("1-007", date(2025, 1, 1), "N", 10.0, 10.0),
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
    ("1-001", date(2025, 1, 1), "Y", 1.0, 1.0),
    ("1-002", date(2025, 2, 1), "Y", 3.0, 4.0),
    ("1-003", date(2025, 3, 1), "Y", 2.0, 6.0),
    ("1-004", date(2025, 1, 1), "N", 5.0, 15.0),
    ("1-005", date(2025, 2, 1), "N", None, 15.0),
    ("1-006", date(2025, 3, 1), "N", 15.0, 30.0),
    ("1-007", date(2025, 1, 1), "N", 10.0, 15.0),
]
