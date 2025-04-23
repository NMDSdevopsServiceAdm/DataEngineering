from dataclasses import dataclass


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
