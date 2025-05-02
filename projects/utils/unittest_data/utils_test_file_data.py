calculate_rows = [
    ("1-001", 10.0, 2.5),
    ("1-002", 2.0, 4.0),
    ("1-003", None, 2.5),
    ("1-004", 10.0, None),
    ("1-005", None, None),
]
expected_calculate_plus_rows = [
    ("1-001", 10.0, 2.5, 12.5),
    ("1-002", 2.0, 4.0, 6.0),
    ("1-003", None, 2.5, None),
    ("1-004", 10.0, None, None),
    ("1-005", None, None, None),
]
expected_calculate_minus_rows = [
    ("1-001", 10.0, 2.5, 7.5),
    ("1-002", 2.0, 4.0, -2.0),
    ("1-003", None, 2.5, None),
    ("1-004", 10.0, None, None),
    ("1-005", None, None, None),
]
expected_calculate_multipy_rows = [
    ("1-001", 10.0, 2.5, 25.0),
    ("1-002", 2.0, 4.0, 8.0),
    ("1-003", None, 2.5, None),
    ("1-004", 10.0, None, None),
    ("1-005", None, None, None),
]
expected_calculate_divide_rows = [
    ("1-001", 10.0, 2.5, 4.0),
    ("1-002", 2.0, 4.0, 0.5),
    ("1-003", None, 2.5, None),
    ("1-004", 10.0, None, None),
    ("1-005", None, None, None),
]
expected_calculate_average_rows = [
    ("1-001", 10.0, 2.5, 6.25),
    ("1-002", 2.0, 4.0, 3.0),
    ("1-003", None, 2.5, None),
    ("1-004", 10.0, None, None),
    ("1-005", None, None, None),
]
expected_calculate_absolute_difference_rows = [
    ("1-001", 10.0, 2.5, 6.25),
    ("1-002", 2.0, 4.0, 2.0),
    ("1-003", None, 2.5, None),
    ("1-004", 10.0, None, None),
    ("1-005", None, None, None),
]
calculate_with_when_clause_rows = [
    ("1-001", 10.0, 2.5),
    ("1-002", 20.0, 2.5),
]
expected_calculate_with_when_clause_rows = [
    ("1-001", 10.0, 2.5, 12.5),
    ("1-002", 20.0, 2.5, None),
]
