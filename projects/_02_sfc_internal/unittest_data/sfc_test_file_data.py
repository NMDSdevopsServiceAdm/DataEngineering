from dataclasses import dataclass
from datetime import date

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CQCCurrentOrHistoricValues,
    CQCRatingsValues,
    LocationType,
    ParentsOrSinglesAndSubs,
    RegistrationStatus,
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
class FlattenCQCRatings:
    test_cqc_locations_rows = [
        (
            "loc_1",
            RegistrationStatus.registered,
            LocationType.social_care_identifier,
            "20240101",
            "2024",
            "01",
            "01",
            {
                CQCL.overall: {
                    CQCL.organisation_id: None,
                    CQCL.rating: "Overall rating Excellent",
                    CQCL.report_date: "report_date",
                    CQCL.report_link_id: None,
                    CQCL.use_of_resources: {
                        CQCL.organisation_id: None,
                        CQCL.summary: None,
                        CQCL.use_of_resources_rating: None,
                        CQCL.combined_quality_summary: None,
                        CQCL.combined_quality_rating: None,
                        CQCL.report_date: None,
                        CQCL.report_link_id: None,
                    },
                    CQCL.key_question_ratings: [
                        {
                            CQCL.name: "Safe",
                            CQCL.rating: "Safe rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Well-led",
                            CQCL.rating: "Well-led rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Caring",
                            CQCL.rating: "Caring rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Responsive",
                            CQCL.rating: "Responsive rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Effective",
                            CQCL.rating: "Effective rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                    ],
                },
                CQCL.service_ratings: [
                    {
                        CQCL.name: None,
                        CQCL.rating: None,
                        CQCL.report_date: None,
                        CQCL.organisation_id: None,
                        CQCL.report_link_id: None,
                        CQCL.key_question_ratings: [
                            {
                                CQCL.name: None,
                                CQCL.rating: None,
                            },
                        ],
                    },
                ],
            },
            [
                {
                    CQCL.report_date: "report_date",
                    CQCL.report_link_id: None,
                    CQCL.organisation_id: None,
                    CQCL.service_ratings: [
                        {
                            CQCL.name: None,
                            CQCL.rating: None,
                            CQCL.key_question_ratings: [
                                {
                                    CQCL.name: None,
                                    CQCL.rating: None,
                                },
                            ],
                        },
                    ],
                    CQCL.overall: {
                        CQCL.rating: "Overall rating Excellent",
                        CQCL.use_of_resources: {
                            CQCL.combined_quality_rating: None,
                            CQCL.combined_quality_summary: None,
                            CQCL.use_of_resources_rating: None,
                            CQCL.use_of_resources_summary: None,
                        },
                        CQCL.key_question_ratings: [
                            {CQCL.name: "Safe", CQCL.rating: "Safe rating Good"},
                            {
                                CQCL.name: "Well-led",
                                CQCL.rating: "Well-led rating Good",
                            },
                            {CQCL.name: "Caring", CQCL.rating: "Caring rating Good"},
                            {
                                CQCL.name: "Responsive",
                                CQCL.rating: "Responsive rating Good",
                            },
                            {
                                CQCL.name: "Effective",
                                CQCL.rating: "Effective rating Good",
                            },
                        ],
                    },
                },
            ],
            [
                {
                    CQCL.assessment_plan_published_datetime: "assessment_plan_published_datetime",
                    CQCL.ratings: {
                        CQCL.overall: [
                            {
                                CQCL.rating: CQCRatingsValues.good,
                                CQCL.status: CQCCurrentOrHistoricValues.current,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                ],
                            },
                            {
                                CQCL.rating: CQCRatingsValues.requires_improvement,
                                CQCL.status: CQCCurrentOrHistoricValues.historic,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                ],
                            },
                        ],
                        CQCL.asg_ratings: [
                            {
                                CQCL.assessment_plan_id: "AP001",
                                CQCL.title: "title",
                                CQCL.assessment_date: "assessment_date_historic",
                                CQCL.assessment_plan_status: "Assessed",
                                CQCL.name: "Care Homes",
                                CQCL.rating: CQCRatingsValues.good,
                                CQCL.status: CQCCurrentOrHistoricValues.historic,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                ],
                            },
                            {
                                CQCL.assessment_plan_id: "AP002",
                                CQCL.title: "title",
                                CQCL.assessment_date: "assessment_date_historic",
                                CQCL.assessment_plan_status: "Assessed",
                                CQCL.name: "Care Homes",
                                CQCL.rating: CQCRatingsValues.good,
                                CQCL.status: CQCCurrentOrHistoricValues.historic,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                ],
                            },
                            {
                                CQCL.assessment_plan_id: "AP003",
                                CQCL.title: "title",
                                CQCL.assessment_date: "assessment_date_current",
                                CQCL.assessment_plan_status: "Assessed",
                                CQCL.name: "Care Homes",
                                CQCL.rating: CQCRatingsValues.good,
                                CQCL.status: CQCCurrentOrHistoricValues.current,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                ],
                            },
                            {
                                CQCL.assessment_plan_id: "AP004",
                                CQCL.title: "title",
                                CQCL.assessment_date: "assessment_date_current",
                                CQCL.assessment_plan_status: "Assessed",
                                CQCL.name: "Supported Living",
                                CQCL.rating: CQCRatingsValues.requires_improvement,
                                CQCL.status: CQCCurrentOrHistoricValues.current,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                ],
                            },
                            {
                                CQCL.assessment_plan_id: "AP005",
                                CQCL.title: "title",
                                CQCL.assessment_date: "assessment_date_historic",
                                CQCL.assessment_plan_status: "Assessed",
                                CQCL.name: "Supported Living",
                                CQCL.rating: CQCRatingsValues.requires_improvement,
                                CQCL.status: CQCCurrentOrHistoricValues.historic,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                ],
                            },
                        ],
                    },
                }
            ],
        ),
    ]
    test_ascwds_workplace_rows = [("loc_1", "estab_1", "20240101", "2021", "01", "01")]
    filter_to_first_import_of_most_recent_month_rows = [
        ("loc_1", "20240101", "2024", "01", "01"),
        ("loc_2", "20231201", "2023", "12", "01"),
    ]
    filter_to_first_import_of_most_recent_month_when_two_imports_in_most_recent_month_rows = [
        ("loc_1", "20240101", "2024", "01", "01"),
        ("loc_2", "20231201", "2023", "12", "01"),
        ("loc_3", "20240104", "2024", "01", "04"),
    ]
    filter_to_first_import_of_most_recent_month_when_earliest_date_is_not_first_of_month_rows = [
        ("loc_1", "20240102", "2024", "01", "02"),
        ("loc_2", "20231201", "2023", "12", "01"),
        ("loc_3", "20240104", "2024", "01", "04"),
    ]
    expected_filter_to_first_import_of_most_recent_month_rows = [
        ("loc_1", "20240101", "2024", "01", "01"),
    ]
    expected_filter_to_first_import_of_most_recent_month_when_earliest_date_is_not_first_of_month_rows = [
        ("loc_1", "20240102", "2024", "01", "02"),
    ]

    flatten_current_ratings_rows = [
        (
            "loc_1",
            "registered",
            {
                CQCL.overall: {
                    CQCL.organisation_id: None,
                    CQCL.rating: "Overall rating Excellent",
                    CQCL.report_date: "report_date",
                    CQCL.report_link_id: None,
                    CQCL.use_of_resources: {
                        CQCL.organisation_id: None,
                        CQCL.summary: None,
                        CQCL.use_of_resources_rating: None,
                        CQCL.combined_quality_summary: None,
                        CQCL.combined_quality_rating: None,
                        CQCL.report_date: None,
                        CQCL.report_link_id: None,
                    },
                    CQCL.key_question_ratings: [
                        {
                            CQCL.name: "Safe",
                            CQCL.rating: "Safe rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Well-led",
                            CQCL.rating: "Well-led rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Caring",
                            CQCL.rating: "Caring rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Responsive",
                            CQCL.rating: "Responsive rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                        {
                            CQCL.name: "Effective",
                            CQCL.rating: "Effective rating Good",
                            CQCL.report_date: None,
                            CQCL.organisation_id: None,
                            CQCL.report_link_id: None,
                        },
                    ],
                },
                CQCL.service_ratings: [
                    {
                        CQCL.name: None,
                        CQCL.rating: None,
                        CQCL.report_date: None,
                        CQCL.organisation_id: None,
                        CQCL.report_link_id: None,
                        CQCL.key_question_ratings: [
                            {
                                CQCL.name: None,
                                CQCL.rating: None,
                            },
                        ],
                    },
                ],
            },
        ),
    ]

    flatten_historic_ratings_rows = [
        (
            "loc_1",
            "registered",
            [
                {
                    CQCL.report_date: "report_date",
                    CQCL.report_link_id: None,
                    CQCL.organisation_id: None,
                    CQCL.service_ratings: [
                        {
                            CQCL.name: None,
                            CQCL.rating: None,
                            CQCL.key_question_ratings: [
                                {
                                    CQCL.name: None,
                                    CQCL.rating: None,
                                },
                            ],
                        },
                    ],
                    CQCL.overall: {
                        CQCL.rating: "Overall rating Excellent",
                        CQCL.use_of_resources: {
                            CQCL.combined_quality_rating: None,
                            CQCL.combined_quality_summary: None,
                            CQCL.use_of_resources_rating: None,
                            CQCL.use_of_resources_summary: None,
                        },
                        CQCL.key_question_ratings: [
                            {CQCL.name: "Safe", CQCL.rating: "Safe rating Good"},
                            {
                                CQCL.name: "Well-led",
                                CQCL.rating: "Well-led rating Good",
                            },
                            {CQCL.name: "Caring", CQCL.rating: "Caring rating Good"},
                            {
                                CQCL.name: "Responsive",
                                CQCL.rating: "Responsive rating Good",
                            },
                            {
                                CQCL.name: "Effective",
                                CQCL.rating: "Effective rating Good",
                            },
                        ],
                    },
                },
            ],
        ),
    ]
    expected_flatten_ratings_rows = [
        (
            "loc_1",
            "registered",
            "report_date",
            "Overall rating Excellent",
            "Safe rating Good",
            "Well-led rating Good",
            "Caring rating Good",
            "Responsive rating Good",
            "Effective rating Good",
        )
    ]

    prepare_assessment_ratings_rows = [
        (
            "loc_1",
            "registered",
            [
                {
                    CQCL.assessment_plan_published_datetime: "assessment_plan_published_datetime",
                    CQCL.ratings: {
                        CQCL.overall: [
                            {
                                CQCL.rating: CQCRatingsValues.good,
                                CQCL.status: CQCCurrentOrHistoricValues.current,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                ],
                            },
                            {
                                CQCL.rating: CQCRatingsValues.requires_improvement,
                                CQCL.status: CQCCurrentOrHistoricValues.historic,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                    },
                                ],
                            },
                        ],
                        CQCL.asg_ratings: [
                            {
                                CQCL.assessment_plan_id: "AP001",
                                CQCL.title: "title",
                                CQCL.assessment_date: "assessment_date_historic",
                                CQCL.assessment_plan_status: "Assessed",
                                CQCL.name: "Care Homes",
                                CQCL.rating: CQCRatingsValues.good,
                                CQCL.status: CQCCurrentOrHistoricValues.historic,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                ],
                            },
                            {
                                CQCL.assessment_plan_id: "AP002",
                                CQCL.title: "title",
                                CQCL.assessment_date: "assessment_date_historic",
                                CQCL.assessment_plan_status: "Assessed",
                                CQCL.name: "Care Homes",
                                CQCL.rating: CQCRatingsValues.good,
                                CQCL.status: CQCCurrentOrHistoricValues.historic,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                ],
                            },
                            {
                                CQCL.assessment_plan_id: "AP003",
                                CQCL.title: "title",
                                CQCL.assessment_date: "assessment_date_current",
                                CQCL.assessment_plan_status: "Assessed",
                                CQCL.name: "Care Homes",
                                CQCL.rating: CQCRatingsValues.good,
                                CQCL.status: CQCCurrentOrHistoricValues.current,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                ],
                            },
                            {
                                CQCL.assessment_plan_id: "AP004",
                                CQCL.title: "title",
                                CQCL.assessment_date: "assessment_date_current",
                                CQCL.assessment_plan_status: "Assessed",
                                CQCL.name: "Supported Living",
                                CQCL.rating: CQCRatingsValues.requires_improvement,
                                CQCL.status: CQCCurrentOrHistoricValues.current,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                ],
                            },
                            {
                                CQCL.assessment_plan_id: "AP005",
                                CQCL.title: "title",
                                CQCL.assessment_date: "assessment_date_historic",
                                CQCL.assessment_plan_status: "Assessed",
                                CQCL.name: "Supported Living",
                                CQCL.rating: CQCRatingsValues.requires_improvement,
                                CQCL.status: CQCCurrentOrHistoricValues.historic,
                                CQCL.key_question_ratings: [
                                    {
                                        CQCL.name: CQCL.safe,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.well_led,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.caring,
                                        CQCL.rating: CQCRatingsValues.requires_improvement,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.responsive,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                    {
                                        CQCL.name: CQCL.effective,
                                        CQCL.rating: CQCRatingsValues.good,
                                        CQCL.status: "Assessed",
                                        CQCL.percentage_score: "15",
                                    },
                                ],
                            },
                        ],
                    },
                }
            ],
        )
    ]
    expected_prepare_assessment_ratings_rows = [
        # --- Current Overall (Good, Current) ---
        (
            "loc_1",
            "registered",
            "assessment_plan_published_datetime",
            None,
            None,
            None,
            None,
            "SAF",
            None,
            CQCCurrentOrHistoricValues.current,
            CQCRatingsValues.good,
            "assessment.ratings.overall",
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
        ),
        # --- Historic Overall (Requires Improvement, Historic) ---
        (
            "loc_1",
            "registered",
            "assessment_plan_published_datetime",
            None,
            None,
            None,
            None,
            "SAF",
            None,
            CQCCurrentOrHistoricValues.historic,
            CQCRatingsValues.requires_improvement,
            "assessment.ratings.overall",
            CQCRatingsValues.requires_improvement,
            CQCRatingsValues.good,
            CQCRatingsValues.requires_improvement,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
        ),
        # --- Current asg_ratings---
        (
            "loc_1",
            "registered",
            "assessment_plan_published_datetime",
            "AP003",
            "title",
            "assessment_date_current",
            "Assessed",
            "SAF",
            "Care Homes",
            CQCCurrentOrHistoricValues.current,
            CQCRatingsValues.good,
            "assessment.ratings.asg_ratings",
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
        ),
        (
            "loc_1",
            "registered",
            "assessment_plan_published_datetime",
            "AP004",
            "title",
            "assessment_date_current",
            "Assessed",
            "SAF",
            "Supported Living",
            CQCCurrentOrHistoricValues.current,
            CQCRatingsValues.requires_improvement,
            "assessment.ratings.asg_ratings",
            CQCRatingsValues.requires_improvement,
            CQCRatingsValues.good,
            CQCRatingsValues.requires_improvement,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
        ),
        (
            "loc_1",
            "registered",
            "assessment_plan_published_datetime",
            "AP001",
            "title",
            "assessment_date_historic",
            "Assessed",
            "SAF",
            "Care Homes",
            CQCCurrentOrHistoricValues.historic,
            CQCRatingsValues.good,
            "assessment.ratings.asg_ratings",
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
        ),
        (
            "loc_1",
            "registered",
            "assessment_plan_published_datetime",
            "AP002",
            "title",
            "assessment_date_historic",
            "Assessed",
            "SAF",
            "Care Homes",
            CQCCurrentOrHistoricValues.historic,
            CQCRatingsValues.good,
            "assessment.ratings.asg_ratings",
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
        ),
        (
            "loc_1",
            "registered",
            "assessment_plan_published_datetime",
            "AP005",
            "title",
            "assessment_date_historic",
            "Assessed",
            "SAF",
            "Supported Living",
            CQCCurrentOrHistoricValues.historic,
            CQCRatingsValues.requires_improvement,
            "assessment.ratings.asg_ratings",
            CQCRatingsValues.requires_improvement,
            CQCRatingsValues.good,
            CQCRatingsValues.requires_improvement,
            CQCRatingsValues.good,
            CQCRatingsValues.good,
        ),
    ]

    raise_error_when_assessment_df_contains_overall_data_with_overall_data_rows = [
        ("loc-1", "assessment.ratings.overall", CQCRatingsValues.good),
        ("loc-2", "assessment.ratings.asg_ratings", CQCRatingsValues.good),
    ]

    assessment_ratings_for_merging_rows = [
        (
            "loc_1",
            "Registered",
            "2024-09-11 08:00:00",
            "AP004",
            "title",
            "2024-09-11",
            "Assessed",
            "Supported Living",
            "assessment.ratings.asg_ratings",
            "SAF",
            CQCCurrentOrHistoricValues.current,
            "asg.rating",
            "asg.safe",
            "asg.effective",
            "asg.caring",
            "asg.responsive",
            "asg.well-led",
        ),
    ]
    standard_ratings_for_merging_rows = [
        (
            "loc_1",
            "Registered",
            "2024-09-15",
            CQCCurrentOrHistoricValues.current,
            "pre-saf.ratings.overall",
            "pre-saf.ratings.safe",
            "pre-saf.ratings.well-led",
            "pre-saf.ratings.caring",
            "pre-saf.ratings.responsive",
            "pre-saf.ratings.effective",
        )
    ]
    expected_merge_cqc_ratings_rows = [
        (
            "loc_1",
            "Registered",
            "2024-09-15",
            None,
            None,
            None,
            None,
            None,
            None,
            "Pre SAF",
            CQCCurrentOrHistoricValues.current,
            "pre-saf.ratings.overall",
            "pre-saf.ratings.safe",
            "pre-saf.ratings.well-led",
            "pre-saf.ratings.caring",
            "pre-saf.ratings.responsive",
            "pre-saf.ratings.effective",
        ),
        (
            "loc_1",
            "Registered",
            "2024-09-11",
            "AP004",
            "title",
            "2024-09-11",
            "Assessed",
            "Supported Living",
            "assessment.ratings.asg_ratings",
            "SAF",
            CQCCurrentOrHistoricValues.current,
            "asg.rating",
            "asg.safe",
            "asg.well-led",
            "asg.caring",
            "asg.responsive",
            "asg.effective",
        ),
    ]

    recode_unknown_to_null_rows = [
        (
            "loc_1",
            "registered",
            "report_date",
            "Excellent",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_2",
            "registered",
            "report_date",
            "Inspected but not rated",
            "No published rating",
            "Insufficient evidence to rate",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_3",
            "No published rating",
            "",
            "Excellent",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_4",
            "deregistered",
            "report_date",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
        ),
        (
            "loc_4",
            "deregistered",
            "report_date",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Inspected but not rated",
            "Insufficient evidence to rate",
        ),
    ]
    expected_recode_unknown_to_null_rows = [
        (
            "loc_1",
            "registered",
            "report_date",
            "Excellent",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_2",
            "registered",
            "report_date",
            None,
            None,
            None,
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_3",
            "No published rating",
            "",
            "Excellent",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_4",
            "deregistered",
            "report_date",
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    ]

    add_current_or_historic_rows = [
        ("loc_1",),
    ]
    expected_add_current_rows = [
        ("loc_1", CQCCurrentOrHistoricValues.current),
    ]
    expected_add_historic_rows = [
        ("loc_1", CQCCurrentOrHistoricValues.historic),
    ]

    remove_blank_rows_rows = [
        (
            "loc_1",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_2",
            "Registered",
            "20240101",
            None,
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_3",
            "Registered",
            "20240101",
            "Good",
            None,
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_4",
            "Registered",
            "20240101",
            "Good",
            "Good",
            None,
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_5",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            None,
            "Good",
            "Good",
        ),
        (
            "loc_6",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            None,
            "Good",
        ),
        (
            "loc_7",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            None,
        ),
        ("loc_8", "Registered", "20240101", None, None, None, None, None, None),
        (
            "loc_1",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
    ]
    expected_remove_blank_rows_rows = [
        (
            "loc_1",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_2",
            "Registered",
            "20240101",
            None,
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_3",
            "Registered",
            "20240101",
            "Good",
            None,
            "Good",
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_4",
            "Registered",
            "20240101",
            "Good",
            "Good",
            None,
            "Good",
            "Good",
            "Good",
        ),
        (
            "loc_5",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            None,
            "Good",
            "Good",
        ),
        (
            "loc_6",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            None,
            "Good",
        ),
        (
            "loc_7",
            "Registered",
            "20240101",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            None,
        ),
    ]

    # fmt: off
    add_rating_sequence_rows = [
        ("loc_1", "2024-1-1", "2024-1-1"), #paired dates, 1+ day between cases
        ("loc_1", "2024-1-2", "2024-1-2"), #paired dates, 1+ day between cases
        ("loc_2", "2024-1-1", "2024-1-1"), #paired dates, 1+ month between cases
        ("loc_2", "2024-2-1", "2024-2-1"), #paired dates, 1+ month between cases
        ("loc_3", "2023-1-1", "2023-1-1"), #paired dates, 1+ year between cases
        ("loc_3", "2024-1-1", "2024-1-1"), #paired dates, 1+ year between cases
        ("loc_4", "2023-1-1", "2023-1-1"), #different rating date, same assessment_date
        ("loc_4", "2024-1-1", "2023-1-1"), #different rating date, same assessment_date
        ("loc_5", "2023-1-1", "2023-1-1"), #same rating date, different assessment_date
        ("loc_5", "2023-1-1", "2024-1-1"), #same rating date, different assessment_date
        ("loc_6", "2023-1-1", None), #different rating date, missing assessment_date
        ("loc_6", "2024-1-1", None), #different rating date, missing assessment_date
        ("loc_7", "2023-1-1", None), #same rating date, missing assessment_date
        ("loc_7", "2023-1-1", None), #same rating date, missing assessment_date
    ]
    expected_add_rating_sequence_rows = [
        ("loc_1", "2024-1-1", "2024-1-1", 1),
        ("loc_1", "2024-1-2", "2024-1-2", 2),
        ("loc_2", "2024-1-1", "2024-1-1", 1),
        ("loc_2", "2024-2-1", "2024-2-1", 2),
        ("loc_3", "2023-1-1", "2023-1-1", 1),
        ("loc_3", "2024-1-1", "2024-1-1", 2),
        ("loc_4", "2023-1-1", "2023-1-1", 1),
        ("loc_4", "2024-1-1", "2023-1-1", 2),
        ("loc_5", "2023-1-1", "2023-1-1", 1),
        ("loc_5", "2023-1-1", "2024-1-1", 2),
        ("loc_6", "2023-1-1", None, 1),
        ("loc_6", "2024-1-1", None, 2),
        ("loc_7", "2023-1-1", None, 1),
        ("loc_7", "2023-1-1", None, 2),
    ]
    expected_reversed_add_rating_sequence_rows = [
        ("loc_1", "2024-1-2", "2024-1-2", 1),
        ("loc_1", "2024-1-1", "2024-1-1", 2),
        ("loc_2", "2024-2-1", "2024-2-1", 1),
        ("loc_2", "2024-1-1", "2024-1-1", 2),
        ("loc_3", "2024-1-1", "2024-1-1", 1),
        ("loc_3", "2023-1-1", "2023-1-1", 2),
        ("loc_4", "2024-1-1", "2023-1-1", 1),
        ("loc_4", "2023-1-1", "2023-1-1", 2),
        ("loc_5", "2023-1-1", "2024-1-1", 1),
        ("loc_5", "2023-1-1", "2023-1-1", 2),
        ("loc_6", "2024-1-1", None, 1),
        ("loc_6", "2023-1-1", None, 2),
        ("loc_7", "2023-1-1", None, 1),
        ("loc_7", "2023-1-1", None, 2),
    ]
    # fmt: on

    add_latest_rating_flag_rows = [
        ("loc_1", 1),
        ("loc_2", 1),
        ("loc_2", 2),
    ]
    expected_add_latest_rating_flag_rows = [
        ("loc_1", 1, 1),
        ("loc_2", 1, 1),
        ("loc_2", 2, 0),
    ]

    create_standard_rating_dataset_rows = [
        (
            "loc_1",
            "Registered",
            "2024-01-01",
            "assessment_plan_id",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "name",
            "source_path",
            "dataset",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Current",
            1,
            1,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
        (
            "loc_1",
            "Registered",
            "2024-01-01",
            "assessment_plan_id",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "name",
            "source_path",
            "dataset",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Current",
            1,
            1,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
        (
            "loc_1",
            "Degistered",
            "2024-01-01",
            "assessment_plan_id",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "name",
            "source_path",
            "dataset",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Current",
            1,
            1,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
        (
            "loc_1",
            "Registered",
            "2024-01-01",
            "assessment_plan_id",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "name",
            "source_path",
            "dataset",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Historic",
            1,
            1,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
        (
            "loc_1",
            "Registered",
            "2024-01-01",
            "assessment_plan_id",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "name",
            "source_path",
            "dataset",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Current",
            1,
            0,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
    ]
    expected_create_standard_rating_dataset_rows = [
        (
            "loc_1",
            "Registered",
            "2024-01-01",
            "assessment_plan_id",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "name",
            "source_path",
            "dataset",
            1,
            "Current",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            3,
            3,
            3,
            3,
            3,
            15,
        ),
        (
            "loc_1",
            "Degistered",
            "2024-01-01",
            "assessment_plan_id",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "name",
            "source_path",
            "dataset",
            1,
            "Current",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            3,
            3,
            3,
            3,
            3,
            15,
        ),
        (
            "loc_1",
            "Registered",
            "2024-01-01",
            "assessment_plan_id",
            "title",
            "assessment_date",
            "assessment_plan_status",
            "name",
            "source_path",
            "dataset",
            1,
            "Historic",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            3,
            3,
            3,
            3,
            3,
            15,
        ),
    ]
    # fmt: off
    select_ratings_for_benchmarks_rows = [
        ("loc_1", RegistrationStatus.registered, "Care home", CQCCurrentOrHistoricValues.current),
        ("loc_1", RegistrationStatus.registered, "Other service", CQCCurrentOrHistoricValues.current),
        ("loc_2", RegistrationStatus.registered, "Care home", CQCCurrentOrHistoricValues.historic),
        ("loc_3", RegistrationStatus.deregistered, "Care home", CQCCurrentOrHistoricValues.current),
        ("loc_4", RegistrationStatus.deregistered, "Care home", CQCCurrentOrHistoricValues.historic),
    ]
    expected_select_ratings_for_benchmarks_rows = [
        ("loc_1", RegistrationStatus.registered, "Care home", CQCCurrentOrHistoricValues.current),
        ("loc_1", RegistrationStatus.registered, "Other service", CQCCurrentOrHistoricValues.current),
    ]
    # fmt: on

    add_good_or_outstanding_flag_rows = [
        ("loc_1", None, 4),
        ("loc_2", None, 3),
        ("loc_3", "Care Home", 4),
        ("loc_3", "Other service", 3),
        ("loc_4", "Same service", 4),
        ("loc_4", "Same service", 2),
        ("loc_5", "Care Home", 4),
        ("loc_5", "Other service", 2),
    ]
    expected_add_good_or_outstanding_flag_rows = [
        ("loc_1", None, 4, 1),
        ("loc_2", None, 3, 1),
        ("loc_3", "Care Home", 4, 1),
        ("loc_3", "Other service", 3, 1),
        ("loc_4", "Same service", 4, 0),
        ("loc_4", "Same service", 2, 0),
        ("loc_5", "Care Home", 4, 0),
        ("loc_5", "Other service", 2, 0),
    ]

    ratings_join_establishment_ids_rows = [
        ("loc_1", "ratings data"),
        ("loc_3", "ratings data"),
    ]
    ascwds_join_establishment_ids_rows = [
        ("loc_1", "estab_1", "20240101"),
        ("loc_2", "estab_2", "20240101"),
    ]
    expected_join_establishment_ids_rows = [
        ("loc_1", "ratings data", "estab_1"),
        ("loc_3", "ratings data", None),
    ]

    # fmt: off
    create_benchmark_ratings_dataset_rows = [
        ("loc_1", "estab_1", "Care Homes", "assessment.ratings.asg_ratings", 1, "Good", "2024-01-01", ""),
        ("loc_2", "estab_2", "Care Homes", "assessment.ratings.asg_ratings", 0, "Requires improvement", "2024-01-01", ""),
        ("loc_3", None, "Care Homes", "assessment.ratings.asg_ratings", 1, "Good", "2024-01-01", ""),
        ("loc_4", "estab_2", "Care Homes", "assessment.ratings.asg_ratings", 0, None, "2024-01-01", ""),
        ("loc_5", None, "Care Homes", "assessment.ratings.asg_ratings", 0, None, "2024-01-01", ""),
    ]
    expected_create_benchmark_ratings_dataset_rows = [
        ("loc_1", "estab_1", "Care Homes", "assessment.ratings.asg_ratings", 1, "Good", "2024-01-01"),
        ("loc_2", "estab_2", "Care Homes", "assessment.ratings.asg_ratings", 0, "Requires improvement", "2024-01-01"),
    ]
    # fmt: on

    add_numerical_ratings_rows = [
        (
            "loc 1",
            CQCRatingsValues.good,
            CQCRatingsValues.outstanding,
            CQCRatingsValues.requires_improvement,
            CQCRatingsValues.inadequate,
            CQCRatingsValues.good,
            None,
        ),
    ]
    expected_add_numerical_ratings_rows = [
        (
            "loc 1",
            CQCRatingsValues.good,
            CQCRatingsValues.outstanding,
            CQCRatingsValues.requires_improvement,
            CQCRatingsValues.inadequate,
            CQCRatingsValues.good,
            None,
            3,
            4,
            2,
            1,
            3,
            0,
            10,
        ),
    ]

    location_id_hash_rows = [
        ("1-123",),
    ]
    expected_location_id_hash_rows = [
        ("1-123", "b022a7e5cc45cf3dc578"),
    ]
    location_id_hash_ten_digit_rows = [
        ("1-123456789",),
    ]
    expected_location_id_hash_ten_digit_rows = [
        ("1-123456789", "4a5a7fdc6afede351ffd"),
    ]
    location_id_hash_eleven_digit_rows = [
        ("1-1234567890",),
    ]
    expected_location_id_hash_eleven_digit_rows = [
        ("1-1234567890", "133d74f156c4fba255e9"),
    ]
    location_id_hash_twelve_digit_rows = [
        ("1-12345678901",),
    ]
    expected_location_id_hash_twelve_digit_rows = [
        ("1-12345678901", "cf16d3a6b6648d845fda"),
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
