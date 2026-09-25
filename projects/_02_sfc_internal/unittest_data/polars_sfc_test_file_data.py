from dataclasses import dataclass
from datetime import date

from utils.column_values.categorical_column_values import RegistrationStatus


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
    cqc_locations_rows = [
        (
            "1-001",
            "Registered",
            "Social Care Org",
            {
                "overall": {
                    "reportDate": "2024-01-01",
                    "rating": "Good",
                    "keyQuestionRatings": [
                        {"name": "Safe", "rating": "Good"},
                        {"name": "Well-led", "rating": "Good"},
                        {"name": "Caring", "rating": "Good"},
                        {"name": "Responsive", "rating": "Good"},
                        {"name": "Effective", "rating": "Good"},
                    ],
                }
            },
            [],
            [],
        ),
    ]

    ascwds_workplace_rows = [
        ("20240101", "2024", "01", "01", "estab-1", "1-001"),
    ]

    current_ratings_rows = [
        (
            "1-001",
            "Registered",
            {
                "overall": {
                    "reportDate": "2024-01-01",
                    "rating": "Good",
                    "keyQuestionRatings": [
                        {"name": "Safe", "rating": "Good"},
                        {"name": "Well-led", "rating": "Good"},
                        {"name": "Caring", "rating": "Outstanding"},
                        {"name": "Responsive", "rating": "Inspected but not rated"},
                        {"name": "Effective", "rating": "Requires improvement"},
                    ],
                }
            },
        ),
    ]
    expected_flatten_current_ratings_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Outstanding",
            None,
            "Requires improvement",
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
            None,
            "Requires improvement",
            "Current",
        ),
    ]

    historic_ratings_rows = [
        (
            "1-001",
            "Registered",
            [
                {
                    "reportDate": "2023-01-01",
                    "overall": {
                        "rating": "Good",
                        "keyQuestionRatings": [
                            {"name": "Safe", "rating": "Good"},
                            {"name": "Well-led", "rating": "Good"},
                            {"name": "Caring", "rating": "Good"},
                            {"name": "Responsive", "rating": "Good"},
                            {"name": "Effective", "rating": "Good"},
                        ],
                    },
                },
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

    def _asg_rating(assessment_plan_id, key_question_ratings):
        return {
            "assessmentPlanId": assessment_plan_id,
            "title": "Care Home Assessment",
            "assessmentDate": "2024-01-02",
            "assessmentPlanStatus": "Assessed",
            "name": "Care Homes",
            "rating": "Good",
            "status": "Current",
            "keyQuestionRatings": key_question_ratings,
        }

    prepare_assessment_ratings_rows = [
        (
            "1-001",
            "Registered",
            [
                {
                    "assessmentPlanPublishedDateTime": "2024-01-01 00:00:00",
                    "ratings": {
                        "overall": [],
                        "asgRatings": [
                            _asg_rating(
                                "AP1",
                                [
                                    {
                                        "name": "Safe",
                                        "rating": "Good",
                                        "status": "Assessed",
                                    },
                                    {
                                        "name": "Well-led",
                                        "rating": "Good",
                                        "status": "Assessed",
                                    },
                                    {
                                        "name": "Caring",
                                        "rating": "Good",
                                        "status": "Assessed",
                                    },
                                    {
                                        "name": "Responsive",
                                        "rating": "Good",
                                        "status": "Assessed",
                                    },
                                    {
                                        "name": "Effective",
                                        "rating": "Good",
                                        "status": "Assessed",
                                    },
                                ],
                            ),
                        ],
                    },
                }
            ],
        ),
    ]
    expected_prepare_assessment_ratings_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01 00:00:00",
            "AP1",
            "Care Home Assessment",
            "2024-01-02",
            "Assessed",
            "SAF",
            "Care Homes",
            "Current",
            "Good",
            "assessment.ratings.asg_ratings",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
    ]

    # Two `keyQuestionRatings` entries share the same key question ("Safe") within a
    # single asg_ratings entry - i.e. duplicate the full pivot grain. The first entry
    # in raw array order ("Good") must always win, regardless of engine/internal order.
    prepare_assessment_ratings_tiebreaker_rows = [
        (
            "1-001",
            "Registered",
            [
                {
                    "assessmentPlanPublishedDateTime": "2024-02-01 00:00:00",
                    "ratings": {
                        "overall": [],
                        "asgRatings": [
                            _asg_rating(
                                "AP2",
                                [
                                    {
                                        "name": "Safe",
                                        "rating": "Good",
                                        "status": "Assessed",
                                    },
                                    {
                                        "name": "Safe",
                                        "rating": "Requires improvement",
                                        "status": "Assessed",
                                    },
                                    {
                                        "name": "Well-led",
                                        "rating": "Good",
                                        "status": "Assessed",
                                    },
                                    {
                                        "name": "Caring",
                                        "rating": "Good",
                                        "status": "Assessed",
                                    },
                                    {
                                        "name": "Responsive",
                                        "rating": "Good",
                                        "status": "Assessed",
                                    },
                                    {
                                        "name": "Effective",
                                        "rating": "Good",
                                        "status": "Assessed",
                                    },
                                ],
                            ),
                        ],
                    },
                }
            ],
        ),
    ]
    expected_prepare_assessment_ratings_tiebreaker_rows = [
        (
            "1-001",
            "Registered",
            "2024-02-01 00:00:00",
            "AP2",
            "Care Home Assessment",
            "2024-01-02",
            "Assessed",
            "SAF",
            "Care Homes",
            "Current",
            "Good",
            "assessment.ratings.asg_ratings",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
    ]

    raise_error_overall_populated_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01 00:00:00",
            None,
            None,
            None,
            None,
            "SAF",
            None,
            "Current",
            "Good",
            "assessment.ratings.overall",
            None,
            None,
            None,
            None,
            None,
        ),
    ]
    raise_error_overall_empty_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01 00:00:00",
            None,
            None,
            None,
            None,
            "SAF",
            None,
            "Current",
            None,
            "assessment.ratings.overall",
            None,
            None,
            None,
            None,
            None,
        ),
    ]

    assessment_ratings_for_merging_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01 00:00:00",
            "AP1",
            "Care Home Assessment",
            "2024-01-02",
            "Assessed",
            "SAF",
            "Care Homes",
            "Current",
            "Good",
            "assessment.ratings.asg_ratings",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
    ]
    standard_ratings_for_merging_rows = [
        (
            "1-002",
            "Registered",
            date(2023, 6, 1),
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Historic",
        ),
    ]
    expected_merge_cqc_ratings_rows = [
        (
            "1-002",
            "Registered",
            date(2023, 6, 1),
            None,
            None,
            None,
            None,
            None,
            None,
            "Pre SAF",
            "Historic",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "1-001",
            "Registered",
            date(2024, 1, 1),
            "AP1",
            "Care Home Assessment",
            "2024-01-02",
            "Assessed",
            "Care Homes",
            "assessment.ratings.asg_ratings",
            "SAF",
            "Current",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
    ]

    recode_unknown_to_null_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "Good",
            "No published rating",
            "Insufficient evidence to rate",
            "Good",
            "",
            "Good",
        ),
    ]
    expected_recode_unknown_to_null_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "Good",
            None,
            None,
            "Good",
            None,
            "Good",
        ),
    ]

    remove_blank_rows_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        ("1-002", "Registered", "2024-01-01", None, None, None, None, None, None),
    ]
    expected_remove_blank_rows_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
    ]

    add_current_or_historic_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
    ]
    expected_add_current_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Current",
        ),
    ]
    expected_add_historic_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Historic",
        ),
    ]

    add_rating_sequence_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Current",
            "2024-01-02",
        ),
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
            "2023-01-02",
        ),
    ]

    add_latest_rating_flag_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Current",
            "2024-01-02",
            1,
        ),
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
            "2023-01-02",
            2,
        ),
    ]

    add_numerical_ratings_rows = [
        (
            "Outstanding",
            "Good",
            "Requires improvement",
            "Inadequate",
            "Outstanding",
            "Good",
        ),
        (None, None, None, None, None, None),
    ]
    expected_add_numerical_ratings_rows = [
        (
            "Outstanding",
            "Good",
            "Requires improvement",
            "Inadequate",
            "Outstanding",
            "Good",
            4,
            3,
            2,
            1,
            4,
            3,
            13,
        ),
        (None, None, None, None, None, None, 0, 0, 0, 0, 0, 0, 0),
    ]

    create_standard_ratings_dataset_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "AP1",
            "Care Home Assessment",
            "2024-01-02",
            "Assessed",
            "Care Homes",
            "assessment.ratings.asg_ratings",
            "SAF",
            1,
            "Current",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            4,
            4,
            4,
            4,
            4,
            20,
        ),
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "AP1",
            "Care Home Assessment",
            "2024-01-02",
            "Assessed",
            "Care Homes",
            "assessment.ratings.asg_ratings",
            "SAF",
            1,
            "Current",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            4,
            4,
            4,
            4,
            4,
            20,
        ),
    ]

    location_id_hash_rows = [
        ("1-001",),
        ("12345678901",),
    ]

    select_ratings_for_benchmarks_rows = [
        ("1-001", "Registered", "Current", 4),
        ("1-002", "Registered", "Historic", 4),
        ("1-003", "Deregistered", "Current", 4),
    ]
    expected_select_ratings_for_benchmarks_rows = [
        ("1-001", "Registered", "Current", 4),
    ]

    add_good_or_outstanding_flag_rows = [
        ("1-001", 3),
        ("1-001", 4),
        ("1-002", 2),
        ("1-002", 4),
    ]

    ratings_join_establishment_ids_rows = [
        ("1-001",),
        ("1-002",),
    ]
    ascwds_join_establishment_ids_rows = [
        ("estab-1", "1-001"),
    ]
    expected_join_establishment_ids_rows = [
        ("1-001", "estab-1"),
        ("1-002", None),
    ]

    create_benchmark_ratings_dataset_rows = [
        ("1-001", "estab-1", "Care Homes", "SAF", 1, "Good", "2024-01-01"),
        ("1-002", None, "Care Homes", "SAF", 1, "Good", "2024-01-01"),
        ("1-003", "estab-3", "Care Homes", "SAF", 1, None, "2024-01-01"),
    ]
    expected_create_benchmark_ratings_dataset_rows = [
        ("1-001", "estab-1", "Care Homes", "SAF", 1, "Good", "2024-01-01"),
    ]
