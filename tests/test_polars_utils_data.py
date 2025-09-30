from dataclasses import dataclass
from datetime import date


@dataclass
class CleaningUtilsData:
    align_dates_primary_rows = [
        (date(2020, 1, 1), date(2020, 1, 8), date(2021, 1, 1), date(2021, 1, 1)),
        ("loc 1", "loc 1", "loc 1", "loc 2"),
    ]
    align_dates_secondary_rows = [
        (
            date(2018, 1, 1),
            date(2019, 1, 1),
            date(2020, 1, 1),
            date(2020, 2, 1),
            date(2021, 1, 8),
            date(2020, 2, 1),
        ),
        ("loc 1", "loc 1", "loc 1", "loc 1", "loc 1", "loc 2"),
    ]
    expected_aligned_dates_rows = [
        (date(2020, 1, 1), date(2020, 1, 8), date(2021, 1, 1)),
        (date(2020, 1, 1), date(2020, 1, 1), date(2020, 2, 1)),
    ]
    align_later_dates_secondary_rows = [
        (date(2020, 2, 1), date(2021, 1, 8), date(2020, 2, 1)),
        ("loc 1", "loc 1", "loc 2"),
    ]
    expected_later_aligned_dates_rows = [
        (date(2021, 1, 1),),
        (date(2020, 2, 1),),
    ]
    expected_cross_join_rows = [
        (
            date(2020, 1, 1),
            date(2020, 1, 8),
            date(2021, 1, 1),
            date(2020, 1, 1),
            date(2020, 1, 8),
            date(2021, 1, 1),
            date(2020, 1, 1),
            date(2020, 1, 8),
            date(2021, 1, 1),
            date(2020, 1, 1),
            date(2020, 1, 8),
            date(2021, 1, 1),
            date(2020, 1, 1),
            date(2020, 1, 8),
            date(2021, 1, 1),
        ),
        (
            date(2019, 1, 1),
            date(2019, 1, 1),
            date(2019, 1, 1),
            date(2020, 1, 1),
            date(2020, 1, 1),
            date(2020, 1, 1),
            date(2020, 2, 1),
            date(2020, 2, 1),
            date(2020, 2, 1),
            date(2021, 1, 8),
            date(2021, 1, 8),
            date(2021, 1, 8),
            date(2018, 1, 1),
            date(2018, 1, 1),
            date(2018, 1, 1),
        ),
    ]
    expected_merged_rows = [
        (date(2020, 1, 1), date(2020, 1, 8), date(2021, 1, 1), date(2021, 1, 1)),
        (date(2020, 1, 1), date(2020, 1, 1), date(2020, 2, 1), date(2020, 2, 1)),
        ("loc 1", "loc 1", "loc 1", "loc 2"),
    ]
    expected_later_merged_rows = [
        (date(2020, 1, 1), date(2020, 1, 8), date(2021, 1, 1), date(2021, 1, 1)),
        (None, None, date(2020, 2, 1), date(2020, 2, 1)),
        ("loc 1", "loc 1", "loc 1", "loc 2"),
    ]
