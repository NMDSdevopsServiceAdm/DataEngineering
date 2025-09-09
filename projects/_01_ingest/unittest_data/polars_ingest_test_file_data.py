from dataclasses import dataclass


@dataclass
class CQCLocationsData:
    clean_provider_id_column_rows = [
        ("loc_1", "loc_1", "loc_1", "loc_2", "loc_2", "loc_2"),
        ("123456789", "123456789", "123456789", "223456789", "223456789", "223456789"),
        ("20240101", "20240201", "20240301", "20240101", "20240201", "20240301"),
    ]

    missing_provider_id_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, "123456789", None),
        ("20240101", "20240201", "20240301"),
    ]

    expected_fill_missing_provider_id_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        ("123456789", "123456789", "123456789"),
        ("20240101", "20240201", "20240301"),
    ]

    long_provider_id_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("223456789 223456789", "223456789", "223456789"),
        ("20240101", "20240101", "20240101"),
    ]

    expected_long_provider_id_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        (None, "223456789", "223456789"),
        ("20240101", "20240101", "20240101"),
    ]

    clean_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
        ("20231101", "20240101", "20231101"),
    ]

    expected_clean_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
        ("20231101", "20240101", "20231101"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
    ]

    time_in_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01 00:00:00", "2023-07-01 15:19:00", "2018-01-01"),
        ("20231101", "20240101", "20231101"),
    ]

    expected_time_in_registration_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01 00:00:00", "2023-07-01 15:19:00", "2018-01-01"),
        ("20231101", "20240101", "20231101"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
    ]

    registration_date_after_import_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
        ("20121101", "20240101", "20131101"),
    ]

    expected_registration_date_after_import_date_column_rows = [
        ("loc_1", "loc_2", "loc_3"),
        ("2018-01-01", "2023-07-01", "2018-01-01"),
        ("20121101", "20240101", "20131101"),
        (None, "2023-07-01", None),
    ]

    registration_date_missing_single_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, "2023-07-01", "2023-07-01"),
        ("20240101", "20240201", "20240301"),
    ]

    expected_registration_date_missing_single_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, "2023-07-01", "2023-07-01"),
        ("20240101", "20240201", "20240301"),
        ("2023-07-01", "2023-07-01", "2023-07-01"),
    ]

    registration_date_missing_multiple_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, "2023-08-01", "2023-07-01"),
        ("20240101", "20240201", "20240301"),
    ]

    expected_registration_date_missing_multiple_reg_date_for_loc_column_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, "2023-08-01", "2023-07-01"),
        ("20240101", "20240201", "20240301"),
        ("2023-07-01", "2023-08-01", "2023-07-01"),
    ]

    registration_date_missing_for_all_loc_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, None, None),
        ("20240201", "20240101", "20240301"),
    ]

    expected_registration_date_missing_for_all_loc_rows = [
        ("loc_1", "loc_1", "loc_1"),
        (None, None, None),
        ("20240201", "20240101", "20240301"),
        ("2024-01-01", "2024-01-01", "2024-01-01"),
    ]
