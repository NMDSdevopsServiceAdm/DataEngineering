from dataclasses import dataclass
from datetime import date

from utils.column_values.categorical_column_values import (
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
