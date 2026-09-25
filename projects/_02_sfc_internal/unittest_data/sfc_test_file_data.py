from dataclasses import dataclass
from datetime import date

from utils.column_values.categorical_column_values import (
    CQCCurrentOrHistoricValues,
    ParentsOrSinglesAndSubs,
)


@dataclass
class ReconciliationUtilsData:
    parents_or_singles_and_subs_rows = [
        ("1", "Yes", "Parent has ownership"),
        ("2", "Yes", "Workplace has ownership"),
        ("3", "No", "Workplace has ownership"),
        ("4", "No", "Parent has ownership"),
    ]
    # fmt: off
    expected_parents_or_singles_and_subs_rows = [
        ("1", "Yes", "Parent has ownership", ParentsOrSinglesAndSubs.parents),
        ("2", "Yes", "Workplace has ownership", ParentsOrSinglesAndSubs.parents),
        ("3", "No", "Workplace has ownership", ParentsOrSinglesAndSubs.singles_and_subs),
        ("4", "No", "Parent has ownership", ParentsOrSinglesAndSubs.parents),
    ]
    # fmt: on


@dataclass
class MergeCoverageData:

    clean_cqc_location_for_merge_rows = [
        (date(2024, 1, 1), "1-000000001", "Name 1", "AB1 2CD", "Independent", "Y", 10),
        (date(2024, 1, 1), "1-000000002", "Name 2", "EF3 4GH", "Independent", "N", None),
        (date(2024, 1, 1), "1-000000003", "Name 3", "IJ5 6KL", "Independent", "N", None),
        (date(2024, 2, 1), "1-000000001", "Name 1", "AB1 2CD", "Independent", "Y", 10),
        (date(2024, 2, 1), "1-000000002", "Name 2", "EF3 4GH", "Independent", "N", None),
        (date(2024, 2, 1), "1-000000003", "Name 3", "IJ5 6KL", "Independent", "N", None),
        (date(2024, 3, 1), "1-000000001", "Name 1", "AB1 2CD", "Independent", "Y", 10),
        (date(2024, 3, 1), "1-000000002", "Name 2", "EF3 4GH", "Independent", "N", None),
        (date(2024, 3, 1), "1-000000003", "Name 3", "IJ5 6KL", "Independent", "N", None),
    ] # fmt: skip

    clean_ascwds_workplace_for_merge_rows = [
        (date(2024, 1, 1), "1-000000001", date(2024, 1, 1), "1", 1, date(2024, 1, 1), date(2024, 1, 1)),
        (date(2024, 1, 1), "1-000000003", date(2024, 1, 1), "3", 2, date(2024, 1, 1), date(2024, 1, 1)),
        (date(2024, 1, 5), "1-000000001", date(2024, 1, 1), "1", 3, date(2024, 1, 1), date(2024, 1, 1)),
        (date(2024, 1, 9), "1-000000001", date(2024, 1, 1), "1", 4, date(2024, 1, 1), date(2024, 1, 1)),
        (date(2024, 1, 9), "1-000000003", date(2024, 1, 1), "3", 5, date(2024, 1, 1), date(2024, 1, 1)),
        (date(2024, 3, 1), "1-000000003", date(2024, 1, 1), "4", 6, date(2024, 1, 1), date(2024, 1, 1)),
    ]# fmt: skip

    expected_cqc_and_ascwds_merged_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Name 1", "AB1 2CD", "Independent", "Y", 10, date(2024, 1, 1), "1", 1, date(2024, 1, 1), date(2024, 1, 1)),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Name 2", "EF3 4GH", "Independent", "N", None, None, None, None, None, None),
        ("1-000000003", date(2024, 1, 1), date(2024, 1, 1), "Name 3", "IJ5 6KL", "Independent", "N", None, date(2024, 1, 1), "3", 2, date(2024, 1, 1), date(2024, 1, 1)),
        ("1-000000001", date(2024, 1, 9), date(2024, 2, 1), "Name 1", "AB1 2CD", "Independent", "Y", 10, date(2024, 1, 1), "1", 4, date(2024, 1, 1), date(2024, 1, 1)),
        ("1-000000002", date(2024, 1, 9), date(2024, 2, 1), "Name 2", "EF3 4GH", "Independent", "N", None, None, None, None, None, None),
        ("1-000000003", date(2024, 1, 9), date(2024, 2, 1), "Name 3", "IJ5 6KL", "Independent", "N", None, date(2024, 1, 1), "3", 5, date(2024, 1, 1), date(2024, 1, 1)),
        ("1-000000001", date(2024, 3, 1), date(2024, 3, 1), "Name 1", "AB1 2CD", "Independent", "Y", 10, None, None, None, None, None),
        ("1-000000002", date(2024, 3, 1), date(2024, 3, 1), "Name 2", "EF3 4GH", "Independent", "N", None, None, None, None, None, None),
        ("1-000000003", date(2024, 3, 1), date(2024, 3, 1), "Name 3", "IJ5 6KL", "Independent", "N", None, date(2024, 1, 1), "4", 6, date(2024, 1, 1), date(2024, 1, 1)),
    ] # fmt: skip

    sample_in_ascwds_rows = [
        ("1", False),
        ("2", True),
        ("3", None),
        (None, True),
        (None, False),
        (None, None),
    ]

    expected_in_ascwds_rows = [
        ("1", False, 1),
        ("2", True, 0),
        ("3", None, 0),
        (None, True, 0),
        (None, False, 0),
        (None, None, 0),
    ]

    sample_cqc_locations_rows = [("1-000000001",), ("1-000000002",)]

    sample_cqc_ratings_for_merge_rows = [
        ("1-000000001", "2024-01-01", "Good", 0, CQCCurrentOrHistoricValues.historic),
        ("1-000000001", "2024-01-02", "Good", 1, CQCCurrentOrHistoricValues.current),
        ("1-000000001", None, "Good", None, None),
        ("1-000000002", "2024-01-01", None, 1, CQCCurrentOrHistoricValues.current),
        ("1-000000002", "2024-01-01", None, 1, CQCCurrentOrHistoricValues.historic),
        (
            "1-000000002",
            "2024-01-01",
            None,
            1,
            CQCCurrentOrHistoricValues.historic,
        ),  # CQC ratings data will contain duplicates so this needs to be handled correctly
    ]

    # fmt: off
    expected_cqc_locations_and_latest_cqc_rating_rows = [
        ("1-000000001", "2024-01-02", "Good",),
        ("1-000000002", "2024-01-01", None,),
    ]
    # fmt: on

    sample_cqc_providers_for_merge_rows = [
        ("provider_1", "Provider Name A", date(2025, 9, 1)),
        ("provider_1", "Provider Name B", date(2025, 9, 8)),
        ("provider_2", "Provider Name C", date(2025, 9, 8)),
    ]
    sample_merged_coverage_rows = [
        ("loc_1", "provider_1"),
        ("loc_2", "provider_1"),
        ("loc_3", "provider_1"),
        ("loc_4", "provider_2"),
        ("loc_5", "provider_2"),
        ("loc_6", "provider_2"),
    ]
    expected_merged_covergae_and_provider_name_joined_rows = [
        ("loc_1", "provider_1", "Provider Name B"),
        ("loc_2", "provider_1", "Provider Name B"),
        ("loc_3", "provider_1", "Provider Name B"),
        ("loc_4", "provider_2", "Provider Name C"),
        ("loc_5", "provider_2", "Provider Name C"),
        ("loc_6", "provider_2", "Provider Name C"),
    ]


@dataclass
class ValidateMergedCoverageData:
    cqc_locations_rows = [
        (date(2024, 1, 1), "1-001", "Name", "AB1 2CD", "Y", 10),
        (date(2024, 1, 1), "1-002", "Name", "EF3 4GH", "N", None),
        (date(2024, 2, 1), "1-001", "Name", "AB1 2CD", "Y", 10),
        (date(2024, 2, 1), "1-002", "Name", "EF3 4GH", "N", None),
    ]

    merged_coverage_rows = [
        ("1-001", date(2024, 1, 1), date(2024, 1, 1), "Name", "AB1 2CD", "Y"),
        ("1-002", date(2024, 1, 1), date(2024, 1, 1), "Name", "EF3 4GH", "N"),
        ("1-001", date(2024, 1, 9), date(2024, 1, 1), "Name", "AB1 2CD", "Y"),
        ("1-002", date(2024, 1, 9), date(2024, 1, 1), "Name", "EF3 4GH", "N"),
    ]
    calculate_expected_size_rows = [
        ("loc 1", date(2024, 1, 1), "name", "AB1 2CD", "Y"),
        ("loc 1", date(2024, 1, 8), "name", "AB1 2CD", "Y"),
        ("loc 2", date(2024, 1, 1), "name", "AB1 2CD", "Y"),
    ]


@dataclass
class LmEngagementUtilsData:
    # fmt: off
    add_columns_for_locality_manager_dashboard_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, 2024), # in ascwds on both dates
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, 2024),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, 2024), # joins ascwds
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, 2024),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, 2024), # leaves ascwds
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, 2024),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, 2024), # multiple locations in one cssr
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, 2024),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, 2024),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, 2024),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, 2024),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, 2024),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, 2024),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, 2024),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, 2024),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, 2024),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, 2024),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, 2024),
    ]
    expected_add_columns_for_locality_manager_dashboard_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, 2024, 1.0, None, 1, 1, 1),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, 2024, 1.0, 0.0, 0, 0, 1),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, 2024, 0.0, None, 0, 0, 0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, 2024, 1.0, 1.0, 1, 1, 1),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, 2024, 1.0, None, 1, 1, 1),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, 2024, 0.0, -1.0, -1, 0, 1),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None, 0, 0, 0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75, 0.75, 3, 3, 3),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25, 1, 1, 4),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None, 0, 0, 0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75, 0.75, 3, 3, 3),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25, 1, 1, 4),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None, 0, 0, 0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75, 0.75, 3, 3, 3),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25, 1, 1, 4),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None, 0, 0, 0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, 2024, 0.75, 0.75, 3, 3, 3),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25, 1, 1, 4),
    ]

    expected_calculate_la_coverage_monthly_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, 2024, 1.0),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, 2024, 1.0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, 2024, 0.0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, 2024, 1.0),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, 2024, 1.0),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, 2024, 0.0),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, 2024, 0.75),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0),
    ]

    calculate_coverage_monthly_change_rows = expected_calculate_la_coverage_monthly_rows

    expected_calculate_coverage_monthly_change_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, 2024, 1.0, None),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, 2024, 1.0, 0.0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, 2024, 0.0, None),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, 2024, 1.0, 1.0),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, 2024, 1.0, None),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, 2024, 0.0, -1.0),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75, 0.75),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75, 0.75),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75, 0.75),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, 2024, 0.75, 0.75),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25),
    ]
    calculate_locations_monthly_change_rows = expected_calculate_coverage_monthly_change_rows

    expected_calculate_locations_monthly_change_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, 2024, 1.0, None, 0, 1),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, 2024, 1.0, 0.0, 1, 0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, 2024, 0.0, None, 0, 0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, 2024, 1.0, 1.0, 0, 1),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, 2024, 1.0, None, 0, 1),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, 2024, 0.0, -1.0, 1, -1),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None, 0, 0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75, 0.75, 0, 3),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25, 1, 1),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None, 0, 0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75, 0.75, 0, 3),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25, 1, 1),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None, 0, 0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, 2024, 0.75, 0.75, 0, 3),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25, 1, 1),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, 2024, 0.0, None, 0, 0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, 2024, 0.75, 0.75, 0, 3),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, 2024, 1.0, 0.25, 0, 1),
    ]

    calculate_new_registrations_rows = expected_calculate_locations_monthly_change_rows

    expected_calculate_new_registrations_rows = expected_add_columns_for_locality_manager_dashboard_rows
    # fmt: on
