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
    def _current_ratings(location_id, key_question_ratings):
        return (
            location_id,
            "Registered",
            {
                "overall": {
                    "reportDate": "2024-01-01",
                    "rating": "overall_rating",
                    "keyQuestionRatings": key_question_ratings,
                }
            },
        )

    def _kq(name, rating=None):
        return {
            "name": name,
            "rating": rating or f"{name.lower().replace('-', '_')}_rating",
        }

    current_ratings_rows = [
        _current_ratings(
            "1-001",
            [
                _kq("Safe"),
                _kq("Well-led"),
                _kq("Caring"),
                _kq("Responsive"),
                _kq("Effective"),
            ],
        ),
    ]

    expected_prepare_current_ratings_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
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
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            None,
            None,
            None,
            "Current",
        ),
    ]

    def _historic_entry(report_date, key_question_ratings):
        return {
            "reportDate": report_date,
            "overall": {
                "rating": "overall_rating",
                "keyQuestionRatings": key_question_ratings,
            },
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
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
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
                    "2023-01-01", [_kq("Safe", "safe_rating_second"), _kq("Well-led")]
                ),
            ],
        ),
    ]

    expected_prepare_historic_ratings_duplicate_date_rows = [
        (
            "1-001",
            "Registered",
            "2023-01-01",
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            None,
            None,
            None,
            "Historic",
        ),
        (
            "1-001",
            "Registered",
            "2023-01-01",
            "overall_rating",
            "safe_rating_second",
            "well_led_rating",
            None,
            None,
            None,
            "Historic",
        ),
    ]

    def _asg_kq(name, rating=None):
        return {
            "name": name,
            "rating": rating or f"{name.lower().replace('-', '_')}_rating",
            "status": "key_question_status",
        }

    def _overall_entry(key_question_ratings, status="overall_status"):
        return {
            "rating": "overall_rating",
            "status": status,
            "keyQuestionRatings": key_question_ratings,
        }

    def _asg_rating(assessment_plan_id, key_question_ratings):
        return {
            "assessmentPlanId": assessment_plan_id,
            "title": "title",
            "assessmentDate": "assessment_date",
            "assessmentPlanStatus": "assessment_plan_status",
            "name": "name",
            "rating": "rating",
            "status": "status",
            "keyQuestionRatings": key_question_ratings,
        }

    def _assessment_entry(published_datetime, asg_ratings, overall=None):
        return {
            "assessmentPlanPublishedDateTime": published_datetime,
            "ratings": {"overall": overall or [], "asgRatings": asg_ratings},
        }

    def _asg_entry(
        published_datetime, assessment_plan_id, key_question_ratings, overall=None
    ):
        # Class-body helpers are not visible inside functions, so this cannot call
        # `_assessment_entry` or `_asg_rating`.
        return {
            "assessmentPlanPublishedDateTime": published_datetime,
            "ratings": {
                "overall": overall or [],
                "asgRatings": [
                    {
                        "assessmentPlanId": assessment_plan_id,
                        "title": "title",
                        "assessmentDate": "assessment_date",
                        "assessmentPlanStatus": "assessment_plan_status",
                        "name": "name",
                        "rating": "rating",
                        "status": "status",
                        "keyQuestionRatings": key_question_ratings,
                    }
                ],
            },
        }

    prepare_assessment_ratings_rows = [
        (
            "1-001",
            "Registered",
            [
                _asg_entry(
                    "2024-01-01 00:00:00",
                    "AP1",
                    [
                        _asg_kq("Safe"),
                        _asg_kq("Well-led"),
                        _asg_kq("Caring"),
                        _asg_kq("Responsive"),
                        _asg_kq("Effective"),
                    ],
                )
            ],
        ),
    ]

    expected_prepare_assessment_ratings_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01 00:00:00",
            "AP1",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "SAF",
            "name",
            "status",
            "rating",
            "assessment.ratings.asg_ratings",
            "safe_rating",
            "effective_rating",
            "caring_rating",
            "responsive_rating",
            "well_led_rating",
        ),
    ]

    prepare_assessment_ratings_overall_rows = [
        (
            "1-001",
            "Registered",
            [
                {
                    "assessmentPlanPublishedDateTime": "2024-01-01 00:00:00",
                    "ratings": {
                        "overall": [_overall_entry([_asg_kq("Safe")])],
                        "asgRatings": [],
                    },
                }
            ],
        ),
    ]

    expected_prepare_assessment_ratings_overall_rows = [
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
            "overall_status",
            "overall_rating",
            "assessment.ratings.overall",
            "safe_rating",
            None,
            None,
            None,
            None,
        ),
    ]

    # 1-001 has two plans, and AP1 also has an overall entry. 1-002 reuses plan id AP1.
    prepare_assessment_ratings_multiple_groups_rows = [
        (
            "1-001",
            "Registered",
            [
                _asg_entry(
                    "2024-01-01 00:00:00",
                    "AP1",
                    [_asg_kq("Safe"), _asg_kq("Caring")],
                    overall=[_overall_entry([_asg_kq("Safe", "overall_safe_rating")])],
                ),
                _asg_entry(
                    "2024-06-01 00:00:00",
                    "AP2",
                    [_asg_kq("Safe", "safe_rating_ap2")],
                ),
            ],
        ),
        (
            "1-002",
            "Registered",
            [
                _asg_entry(
                    "2024-01-01 00:00:00",
                    "AP1",
                    [_asg_kq("Safe", "safe_rating_location_2")],
                )
            ],
        ),
    ]

    expected_prepare_assessment_ratings_multiple_groups_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01 00:00:00",
            "AP1",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "SAF",
            "name",
            "status",
            "rating",
            "assessment.ratings.asg_ratings",
            "safe_rating",
            None,
            "caring_rating",
            None,
            None,
        ),
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
            "overall_status",
            "overall_rating",
            "assessment.ratings.overall",
            "overall_safe_rating",
            None,
            None,
            None,
            None,
        ),
        (
            "1-001",
            "Registered",
            "2024-06-01 00:00:00",
            "AP2",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "SAF",
            "name",
            "status",
            "rating",
            "assessment.ratings.asg_ratings",
            "safe_rating_ap2",
            None,
            None,
            None,
            None,
        ),
        (
            "1-002",
            "Registered",
            "2024-01-01 00:00:00",
            "AP1",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "SAF",
            "name",
            "status",
            "rating",
            "assessment.ratings.asg_ratings",
            "safe_rating_location_2",
            None,
            None,
            None,
            None,
        ),
    ]

    # One assessment holding two overall entries (Current and Historic) and two ASG plans.
    prepare_assessment_ratings_multiple_entries_rows = [
        (
            "1-001",
            "Registered",
            [
                _assessment_entry(
                    "2024-01-01 00:00:00",
                    [
                        _asg_rating("AP1", [_asg_kq("Safe", "safe_rating_ap1")]),
                        _asg_rating("AP2", [_asg_kq("Safe", "safe_rating_ap2")]),
                    ],
                    overall=[
                        _overall_entry(
                            [_asg_kq("Safe", "overall_safe_rating_current")],
                            status="Current",
                        ),
                        _overall_entry(
                            [_asg_kq("Safe", "overall_safe_rating_historic")],
                            status="Historic",
                        ),
                    ],
                )
            ],
        ),
    ]

    expected_prepare_assessment_ratings_multiple_entries_rows = [
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
            "overall_rating",
            "assessment.ratings.overall",
            "overall_safe_rating_current",
            None,
            None,
            None,
            None,
        ),
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
            "Historic",
            "overall_rating",
            "assessment.ratings.overall",
            "overall_safe_rating_historic",
            None,
            None,
            None,
            None,
        ),
        (
            "1-001",
            "Registered",
            "2024-01-01 00:00:00",
            "AP1",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "SAF",
            "name",
            "status",
            "rating",
            "assessment.ratings.asg_ratings",
            "safe_rating_ap1",
            None,
            None,
            None,
            None,
        ),
        (
            "1-001",
            "Registered",
            "2024-01-01 00:00:00",
            "AP2",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "SAF",
            "name",
            "status",
            "rating",
            "assessment.ratings.asg_ratings",
            "safe_rating_ap2",
            None,
            None,
            None,
            None,
        ),
    ]

    prepare_assessment_ratings_tiebreaker_rows = [
        (
            "1-001",
            "Registered",
            [
                _asg_entry(
                    "2024-02-01 00:00:00",
                    "AP2",
                    [
                        _asg_kq("Safe"),
                        _asg_kq("Safe", "safe_rating_second"),
                        _asg_kq("Well-led"),
                        _asg_kq("Caring"),
                        _asg_kq("Responsive"),
                        _asg_kq("Effective"),
                    ],
                )
            ],
        ),
    ]

    expected_prepare_assessment_ratings_tiebreaker_rows = [
        (
            "1-001",
            "Registered",
            "2024-02-01 00:00:00",
            "AP2",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "SAF",
            "name",
            "status",
            "rating",
            "assessment.ratings.asg_ratings",
            "safe_rating",
            "effective_rating",
            "caring_rating",
            "responsive_rating",
            "well_led_rating",
        ),
    ]

    prepare_assessment_ratings_null_key_question_ratings_rows = [
        *prepare_assessment_ratings_rows,
        (
            "1-002",
            "Registered",
            [
                {
                    "assessmentPlanPublishedDateTime": "2024-01-01 00:00:00",
                    "ratings": {
                        "overall": [
                            {
                                "rating": "overall_rating",
                                "status": "overall_status",
                                "keyQuestionRatings": None,
                            }
                        ],
                        "asgRatings": [],
                    },
                }
            ],
        ),
    ]

    expected_prepare_assessment_ratings_null_key_question_ratings_rows = (
        expected_prepare_assessment_ratings_rows
    )

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
            "title",
            "assessment_date",
            "assessment_plan_status",
            "SAF",
            "name",
            "status",
            "rating",
            "assessment.ratings.asg_ratings",
            "safe_rating",
            "effective_rating",
            "caring_rating",
            "responsive_rating",
            "well_led_rating",
        ),
    ]

    assessment_ratings_unparseable_date_rows = [
        assessment_ratings_for_merging_rows[0][:2]
        + ("2024-01-01T00:00:00.123Z",)
        + assessment_ratings_for_merging_rows[0][3:],
    ]

    standard_ratings_for_merging_rows = [
        (
            "1-002",
            "Registered",
            date(2023, 6, 1),
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
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
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
        ),
        (
            "1-001",
            "Registered",
            date(2024, 1, 1),
            "AP1",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "name",
            "assessment.ratings.asg_ratings",
            "SAF",
            "status",
            "rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
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

    # "No published rating" and "" outside the rating columns must be left alone.
    recode_unknown_to_null_non_rating_columns_rows = [
        (
            "1-002",
            "No published rating",
            "",
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
        ),
    ]

    remove_blank_rows_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "overall_rating",
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "1-002",
            "Registered",
            "2024-01-01",
            None,
            "safe_rating",
            None,
            None,
            None,
            None,
        ),
        (
            "1-003",
            "Registered",
            "2024-01-01",
            None,
            None,
            "well_led_rating",
            None,
            None,
            None,
        ),
        (
            "1-004",
            "Registered",
            "2024-01-01",
            None,
            None,
            None,
            "caring_rating",
            None,
            None,
        ),
        (
            "1-005",
            "Registered",
            "2024-01-01",
            None,
            None,
            None,
            None,
            "responsive_rating",
            None,
        ),
        (
            "1-006",
            "Registered",
            "2024-01-01",
            None,
            None,
            None,
            None,
            None,
            "effective_rating",
        ),
        (
            "1-007",
            "Registered",
            "2024-01-01",
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    ]

    expected_remove_blank_rows_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "overall_rating",
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "1-002",
            "Registered",
            "2024-01-01",
            None,
            "safe_rating",
            None,
            None,
            None,
            None,
        ),
        (
            "1-003",
            "Registered",
            "2024-01-01",
            None,
            None,
            "well_led_rating",
            None,
            None,
            None,
        ),
        (
            "1-004",
            "Registered",
            "2024-01-01",
            None,
            None,
            None,
            "caring_rating",
            None,
            None,
        ),
        (
            "1-005",
            "Registered",
            "2024-01-01",
            None,
            None,
            None,
            None,
            "responsive_rating",
            None,
        ),
        (
            "1-006",
            "Registered",
            "2024-01-01",
            None,
            None,
            None,
            None,
            None,
            "effective_rating",
        ),
    ]

    remove_blank_duplicate_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
        ),
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
        ),
    ]

    expected_remove_blank_duplicate_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
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

    # 1-002's latest rating is older than 1-001's, so the flag must be set per location.
    add_latest_rating_flag_multiple_locations_rows = [
        (
            "1-001",
            "Registered",
            "2024-01-01",
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
            "Current",
            "2024-01-02",
        ),
        (
            "1-001",
            "Registered",
            "2023-01-01",
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
            "Current",
            "2023-01-02",
        ),
        (
            "1-002",
            "Registered",
            "2022-01-01",
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
            "Current",
            "2022-01-02",
        ),
        (
            "1-002",
            "Registered",
            "2021-01-01",
            "overall_rating",
            "safe_rating",
            "well_led_rating",
            "caring_rating",
            "responsive_rating",
            "effective_rating",
            "Current",
            "2021-01-02",
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
    ]

    location_id_hash_rows = [
        ("1-123",),
        ("1-1234567890",),
    ]

    # Hashes produced by the previous Spark implementation, used to link anonymised files.
    expected_location_id_hash_rows = [
        (
            "1-123",
            "b022a7e5cc45cf3dc578",
        ),
        (
            "1-1234567890",
            "133d74f156c4fba255e9",
        ),
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

    # 1-999 is not in the ratings data, so it must not add a row to the output.
    ascwds_join_establishment_ids_rows = [
        (
            "estab-1",
            "1-001",
        ),
        (
            "estab-9",
            "1-999",
        ),
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
