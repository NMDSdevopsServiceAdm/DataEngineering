from dataclasses import dataclass
from datetime import date

from utils.column_values.categorical_column_values import (
    InAscwds,
    ParentsOrSinglesAndSubs,
    RegistrationStatus,
)


@dataclass
class ValidateMergeCoverageData:

    cqc_locations_rows = [
        (
            date(2024, 1, 1),
            "1-001",
            "1-001",
            "Name",
            "AB1 2CD",
            "Y",
            10,
            "2024",
            "01",
            "01",
        ),
        (
            date(2024, 1, 1),
            "1-002",
            "1-001",
            "Name",
            "EF3 4GH",
            "N",
            None,
            "2024",
            "01",
            "01",
        ),
        (
            date(2024, 2, 1),
            "1-001",
            "1-001",
            "Name",
            "AB1 2CD",
            "Y",
            10,
            "2024",
            "02",
            "01",
        ),
        (
            date(2024, 2, 1),
            "1-002",
            "1-001",
            "Name",
            "EF3 4GH",
            "N",
            None,
            "2024",
            "02",
            "01",
        ),
    ]

    merged_coverage_rows = [
        (
            "1-001",
            "Sunrise Care Home",
            date(2024, 1, 1),
            "Y",
            "1-001",
            "Health",
            date(2023, 12, 1),
            "Residential",
            date(2024, 1, 2),
            "AB1 2CD",
            "Good",
            "Region 1",
            "Urban",
            1,
            0.85,
            0.0,
            5,
            12,
            date(2024, 1, 1),
            "NMDS001",
            "Y",
            "Outstanding",
            "Sunrise Care Ltd",
            "Singles",
            0.0,
            date(2024, 1, 3),
            "2024",
            "01",
            "01",
        ),
        (
            "1-002",
            "Sunset Care Home",
            date(2024, 1, 1),
            "N",
            "1-001",
            "Social",
            date(2023, 12, 5),
            "Nursing",
            date(2024, 1, 3),
            "EF3 4GH",
            "Requires Improvement",
            "Region 2",
            "Rural",
            0,
            0.6,
            0.1,
            2,
            8,
            date(2024, 1, 2),
            "NMDS002",
            "N",
            "Good",
            "Sunset Care Ltd",
            "Parents",
            0.1,
            date(2024, 1, 4),
            "2024",
            "01",
            "01",
        ),
        (
            "1-003",
            "Maple Care Home",
            date(2024, 2, 1),
            "Y",
            "1-001",
            "Health",
            date(2024, 1, 1),
            "Residential",
            date(2024, 2, 2),
            "GH5 6JK",
            "Outstanding",
            "Region 3",
            "Urban",
            1,
            0.9,
            -0.05,
            6,
            15,
            date(2024, 2, 1),
            "NMDS003",
            "N",
            "Outstanding",
            "Maple Care Ltd",
            "Singles",
            -0.05,
            date(2024, 2, 3),
            "2024",
            "02",
            "01",
        ),
        (
            "1-004",
            "Oak Care Home",
            date(2024, 2, 1),
            "N",
            "1-001",
            "Social",
            date(2024, 1, 5),
            "Nursing",
            date(2024, 2, 3),
            "IJ7 8LM",
            "Requires Improvement",
            "Region 4",
            "Rural",
            0,
            0.7,
            0.05,
            3,
            9,
            date(2024, 2, 2),
            "NMDS004",
            "Y",
            "Good",
            "Oak Care Ltd",
            "Parents",
            0.05,
            date(2024, 2, 4),
            "2024",
            "02",
            "01",
        ),
    ]

    calculate_expected_size_rows = [
        ("loc 1", date(2024, 1, 1), "name", "AB1 2CD", "Y", "2024", "01", "01"),
        ("loc 1", date(2024, 1, 8), "name", "AB1 2CD", "Y", "2024", "01", "08"),
        ("loc 2", date(2024, 1, 1), "name", "AB1 2CD", "Y", "2024", "01", "01"),
    ]
    expected_row_count = 1


@dataclass
class MergeCoverageData:
    cqc_location_rows = [(date(2024, 4, 1),)]

    ascwds_workplace_rows = [("1-001",)]

    cqc_ratings_rows = [("Good",)]

    cqc_providers_rows = [("1",)]

    # (is_parent, parent_permission)
    deduped_merged_coverage_rows = [
        ("Yes", "Workplace has ownership"),  # is_parent - parent
        ("No", "Parent has ownership"),  # not parent, but parent has ownership - parent
        (
            "No",
            "Workplace has ownership",
        ),  # not parent, no ownership - singles_and_subs
    ]

    expected_parents_or_singles_and_subs = [
        ParentsOrSinglesAndSubs.parents,
        ParentsOrSinglesAndSubs.parents,
        ParentsOrSinglesAndSubs.singles_and_subs,
    ]

    # (establishment_id, workplace_last_active_date, purge_date)
    add_removed_by_purge_date_filter_flag_rows = [
        ("1-001", date(2024, 1, 1), date(2024, 6, 1)),  # active before purge - removed
        ("1-002", date(2024, 6, 1), date(2024, 1, 1)),  # active after purge - kept
    ]
    expected_removed_by_purge_date_filter_flags = [True, False]

    # (ascwds_workplace_import_date, location_id, master_update_date, establishment_id)
    deduplicate_ascwds_workplace_data_rows = [
        (date(2024, 4, 1), "1-001", date(2024, 3, 1), "100"),  # older update - dropped
        (date(2024, 4, 1), "1-001", date(2024, 3, 15), "101"),  # latest update - kept
        (date(2024, 4, 1), "1-002", date(2024, 3, 1), "102"),  # only row - kept
        (date(2024, 4, 1), "1-003", date(2024, 3, 1), "200"),  # update date tied with
        (
            date(2024, 4, 1),
            "1-003",
            date(2024, 3, 1),
            "199",
        ),  # row below - lower id wins
    ]
    expected_deduplicate_ascwds_workplace_data_establishment_ids = [
        "101",
        "102",
        "199",
    ]

    # cqc_location: (cqc_location_import_date, location_id)
    join_ascwds_data_cqc_location_rows = [(date(2024, 4, 1), "1-001")]
    # ascwds_workplace: (ascwds_workplace_import_date, location_id, establishment_id)
    join_ascwds_data_ascwds_workplace_rows = [
        (date(2024, 1, 1), "1-001", "100"),
        (date(2024, 3, 1), "1-001", "101"),  # closest import date on/before 2024-04-01
        (
            date(2024, 5, 1),
            "1-001",
            "102",
        ),  # after the CQC import date - not aligned to
    ]
    expected_join_ascwds_data_aligned_import_date = date(2024, 3, 1)
    expected_join_ascwds_data_establishment_id = "101"

    # (establishment_id, removed_by_purge_date_filter)
    add_flag_for_in_ascwds_rows = [
        ("100", False),  # has establishment, not removed - in ASC-WDS
        ("101", True),  # has establishment, removed - not in ASC-WDS
        (None, False),  # no establishment - not in ASC-WDS
    ]
    expected_in_ascwds_flags = [
        InAscwds.is_in_ascwds,
        InAscwds.not_in_ascwds,
        InAscwds.not_in_ascwds,
    ]

    # (cqc_location_import_date, name, postal_code, care_home, in_ascwds,
    #  imputed_registration_date, location_id)
    deduplicate_merged_coverage_data_rows = [
        (date(2024, 4, 1), "Name A", "AB1 2CD", "Y", 0, date(2024, 1, 1), "1-002"),
        (date(2024, 4, 1), "Name A", "AB1 2CD", "Y", 1, date(2024, 1, 1), "1-001"),
        (date(2024, 4, 1), "Name B", "EF3 4GH", "N", 1, date(2024, 2, 1), "1-003"),
        (date(2024, 4, 1), "Name B", "EF3 4GH", "N", 1, date(2024, 2, 1), "1-004"),
    ]
    expected_deduplicate_merged_coverage_data_location_ids = ["1-001", "1-003"]

    # coverage: (location_id,)
    join_latest_cqc_rating_coverage_rows = [("1-001",), ("1-002",)]
    # ratings: (location_id, overall_rating, latest_rating_flag, current_or_historic)
    join_latest_cqc_rating_ratings_rows = [
        ("1-001", "Good", 1, "Current"),  # latest current rating - kept
        ("1-001", "Requires improvement", 0, "Historic"),  # not latest - filtered out
        ("1-002", "Outstanding", 1, "Historic"),  # latest but historic - filtered out
    ]
    expected_join_latest_cqc_rating_overall_ratings = ["Good", None]

    # coverage: (location_id, provider_id)
    join_provider_name_coverage_rows = [("1-001", "P1"), ("1-002", "P2")]
    # providers: (provider_id, name, cqc_provider_import_date)
    join_provider_name_providers_rows = [
        ("P1", "Provider One Old Name", date(2024, 1, 1)),
        ("P1", "Provider One New Name", date(2024, 4, 1)),  # latest import date - kept
        ("P2", "Provider Two", date(2024, 2, 1)),
    ]
    expected_join_provider_names = ["Provider One New Name", "Provider Two"]

    # (cqc_location_import_date, location_id, is_parent, parent_permission)
    merged_coverage_with_two_import_dates_rows = [
        (
            date(2024, 3, 1),
            "1-001",
            "No",
            "Workplace has ownership",
        ),  # older month - excluded from reduced output
        (
            date(2024, 4, 1),
            "1-002",
            "No",
            "Workplace has ownership",
        ),  # latest month - kept in reduced output
    ]


@dataclass
class LmEngagementData:
    # (location_id, cqc_location_import_date, current_cssr, in_ascwds)
    orchestrator_input_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1),
        # loc 1's cssr 1 rows duplicated into 2025 (still continuously in
        # ASC-WDS) to check new_registrations_ytd resets per year rather than
        # carrying 2024's cumulative total forward.
        ("loc 1", date(2025, 1, 1), "cssr 1", 1),
        ("loc 1", date(2025, 2, 1), "cssr 1", 1),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1),
        # loc 7's rows are deliberately out of date order, to check the window
        # functions sort internally rather than relying on input row order.
        ("loc 7", date(2024, 2, 1), "cssr 4", 0),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1),
    ]

    # orchestrator_input_rows plus a precomputed _year column.
    base_rows = [(*row, row[1].year) for row in orchestrator_input_rows]

    # (location_id, cqc_location_import_date, current_cssr, in_ascwds, _year,
    #  la_monthly_coverage)
    expected_la_coverage_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, 2024, 1.0),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, 2024, 1.0),
        ("loc 1", date(2025, 1, 1), "cssr 1", 1, 2025, 1.0),
        ("loc 1", date(2025, 2, 1), "cssr 1", 1, 2025, 1.0),
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

    # ... plus coverage_monthly_change
    expected_coverage_change_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, 2024, 1.0, None),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, 2024, 1.0, 0.0),
        ("loc 1", date(2025, 1, 1), "cssr 1", 1, 2025, 1.0, 0.0),
        ("loc 1", date(2025, 2, 1), "cssr 1", 1, 2025, 1.0, 0.0),
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

    # ... plus in_ascwds_last_month, locations_monthly_change
    expected_locations_change_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, 2024, 1.0, None, 0, 1),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, 2024, 1.0, 0.0, 1, 0),
        ("loc 1", date(2025, 1, 1), "cssr 1", 1, 2025, 1.0, 0.0, 1, 0),
        ("loc 1", date(2025, 2, 1), "cssr 1", 1, 2025, 1.0, 0.0, 1, 0),
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

    # final shape: ... locations_monthly_change, new_registrations_monthly,
    # new_registrations_ytd (in_ascwds_last_month dropped)
    expected_final_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, 2024, 1.0, None, 1, 1, 1),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, 2024, 1.0, 0.0, 0, 0, 1),
        ("loc 1", date(2025, 1, 1), "cssr 1", 1, 2025, 1.0, 0.0, 0, 0, 0),
        ("loc 1", date(2025, 2, 1), "cssr 1", 1, 2025, 1.0, 0.0, 0, 0, 0),
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


@dataclass
class ReconciliationData:
    # fmt: off
    # (import_date, establishment_id, nmds_id, is_parent, organisation_id, parent_permission,
    #  establishment_type, registration_type, location_id, main_service_id, establishment_name, region_id)
    ascwds_workplace_rows = [
        (date(2024, 4, 1), "100", "10", "No", "100", "Workplace has ownership", "Private sector", "CQC regulated", "1-001", "Care home services with nursing - CHN", "Est Name 00", "1"),  # single, CQC regulated, has location - included
        (date(2024, 4, 1), "101", "11", "No", "101", "Workplace has ownership", "Private sector", "Not regulated", None, "Care home services with nursing - CHN", "Est Name 01", "2"),  # not CQC regulated - excluded
        (date(2024, 4, 1), "102", "12", "No", "102", "Workplace has ownership", "Private sector", "CQC regulated", None, "Head office services", "Est Name 02", "3"),  # head office, no location - excluded
        (date(2024, 4, 1), "103", "13", "No", "103", "Workplace has ownership", "Private sector", "CQC regulated", None, "Domiciliary care services (Adults) - DCC", "Est Name 03", "4"),  # not head office, no location - included
        (date(2024, 4, 1), "104", "14", "No", "104", "Workplace has ownership", "Private sector", "CQC regulated", None, "Domiciliary care services (Adults) - DCC", "Est Name 04", "-1"),  # not head office, no location - included; region_id "-1" ("not known") sentinel
        (date(2024, 4, 1), "201", "20", "Yes", "201", "Workplace has ownership", "Private sector", "CQC regulated", "1-002", "Care home services with nursing - CHN", "Parent 01", "5"),  # parent account - included in parent lookup
        (date(2024, 4, 1), "202", "21", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", "1-003", "Care home services with nursing - CHN", "Est Name 22", "6"),  # sub of a parent (by ownership) - classed as a parent-type row
        (date(2023, 1, 1), "999", "99", "No", "999", "Workplace has ownership", "Private sector", "CQC regulated", "1-999", "Care home services with nursing - CHN", "Old Import", "1"),  # older import date - excluded by max-import-date filter
    ]
    # fmt: on

    # ascwds_workplace_rows plus the purge-date columns main() drops before further processing.
    # fmt: off
    main_ascwds_workplace_rows = [
        (date(2024, 4, 1), "100", "10", "No", "100", "Workplace has ownership", "Private sector", "CQC regulated", "1-001", "Care home services with nursing - CHN", "Est Name 00", "1", date(2024, 4, 1), date(2022, 4, 1)),  # not purged
        (date(2024, 4, 1), "999", "98", "No", "999", "Workplace has ownership", "Private sector", "CQC regulated", "1-998", "Care home services with nursing - CHN", "Purged Est", "1", date(2020, 1, 1), date(2022, 4, 1)),  # purged - excluded before further processing
    ]
    # fmt: on

    main_cqc_location_rows = [
        ("1-001", date(2024, 4, 1), RegistrationStatus.deregistered, date(2024, 3, 15)),
    ]

    main_expected_single_and_subs_nmds_ids = ["10"]


@dataclass
class FlattenCQCRatings:
    def _current_ratings(location_id, key_question_ratings):
        return (
            location_id,
            "Registered",
            {
                "overall": {
                    "reportDate": "2024-01-01",
                    "rating": "Good",
                    "keyQuestionRatings": key_question_ratings,
                }
            },
        )

    def _kq(name, rating="Good"):
        return {"name": name, "rating": rating}

    current_ratings_rows = [
        _current_ratings(
            "1-001",
            [
                _kq("Safe"),
                _kq("Well-led"),
                _kq("Caring", "Outstanding"),
                _kq("Responsive", "Inspected but not rated"),
                _kq("Effective", "Requires improvement"),
            ],
        ),
    ]

    expected_prepare_current_ratings_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Outstanding",
            "Inspected but not rated",
            "Requires improvement",
            "Current",
        ),
    ]

    current_ratings_short_key_question_list_rows = [
        _current_ratings("1-002", [_kq("Safe"), _kq("Well-led")]),
    ]

    expected_prepare_current_ratings_short_key_question_list_rows = [
        (
            "1-002",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            None,
            None,
            None,
            "Current",
        ),
    ]

    def _historic_entry(report_date, key_question_ratings):
        return {
            "reportDate": report_date,
            "overall": {"rating": "Good", "keyQuestionRatings": key_question_ratings},
        }

    historic_ratings_rows = [
        (
            "1-001",
            "Registered",
            [
                _historic_entry(
                    "2023-01-01",
                    [
                        _kq("Safe"),
                        _kq("Well-led"),
                        _kq("Caring"),
                        _kq("Responsive"),
                        _kq("Effective"),
                    ],
                )
            ],
        ),
    ]

    expected_prepare_historic_ratings_rows = [
        (
            "1-001",
            "Registered",
            "2023-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Historic",
        ),
    ]

    historic_ratings_duplicate_date_rows = [
        (
            "1-001",
            "Registered",
            [
                _historic_entry("2023-01-01", [_kq("Safe"), _kq("Well-led")]),
                _historic_entry(
                    "2023-01-01", [_kq("Safe", "Inadequate"), _kq("Well-led")]
                ),
            ],
        ),
    ]

    expected_prepare_historic_ratings_duplicate_date_rows = [
        (
            "1-001",
            "Registered",
            "2023-01-01",
            "Good",
            "Good",
            "Good",
            None,
            None,
            None,
            "Historic",
        ),
        (
            "1-001",
            "Registered",
            "2023-01-01",
            "Good",
            "Inadequate",
            "Good",
            None,
            None,
            None,
            "Historic",
        ),
    ]
