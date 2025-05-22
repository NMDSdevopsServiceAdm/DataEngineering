from dataclasses import dataclass
from datetime import date

from pyspark.ml.linalg import Vectors

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
    NewCqcLocationApiColumns as CQCLNew,
)
from utils.column_values.categorical_columns_by_dataset import (
    DiagnosticOnKnownFilledPostsCategoricalValues as CatValues,
)
from utils.column_values.categorical_column_values import (
    AscwdsFilteringRule,
    CareHome,
    CQCCurrentOrHistoricValues,
    CQCRatingsValues,
    Dormancy,
    EstimateFilledPostsSource,
    IsParent,
    LocationType,
    JobGroupLabels,
    MainJobRoleLabels,
    ParentsOrSinglesAndSubs,
    PrimaryServiceType,
    Region,
    RegistrationStatus,
    RelatedLocation,
    RUI,
    Sector,
    Services,
    SingleSubDescription,
    Specialisms,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_difference_within_range import (
    ascwds_filled_posts_difference_within_range_source_description,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_return_worker_record_count_if_equal_to_total_staff import (
    ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description,
)
from utils.raw_data_adjustments import RecordsToRemoveInLocationsData
from utils.validation.validation_rule_custom_type import CustomValidationRules
from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.column_values.categorical_column_values import MainJobRoleLabels


@dataclass
class ASCWDSWorkerData:
    worker_rows = [
        ("1-000000001", "101", "100", "1", "20200101", "2020", "01", "01"),
        ("1-000000002", "102", "101", "1", "20200101", "2020", "01", "01"),
        ("1-000000003", "103", "102", "1", "20200101", "2020", "01", "01"),
        ("1-000000004", "104", "103", "1", "20190101", "2019", "01", "01"),
        ("1-000000005", "104", "104", "2", "19000101", "1900", "01", "01"),
        ("1-000000006", "invalid", "105", "3", "20200101", "2020", "01", "01"),
        ("1-000000007", "999", "106", "1", "20200101", "2020", "01", "01"),
    ]

    expected_remove_workers_without_workplaces_rows = [
        ("1-000000001", "101", "100", "1", "20200101", "2020", "01", "01"),
        ("1-000000002", "102", "101", "1", "20200101", "2020", "01", "01"),
        ("1-000000003", "103", "102", "1", "20200101", "2020", "01", "01"),
        ("1-000000004", "104", "103", "1", "20190101", "2019", "01", "01"),
    ]

    create_clean_main_job_role_column_rows = [
        ("101", date(2024, 1, 1), "-1"),
        ("101", date(2025, 1, 1), "1"),
        ("102", date(2025, 1, 1), "-1"),
        ("103", date(2024, 1, 1), "3"),
        ("103", date(2025, 1, 1), "4"),
        ("141", date(2025, 1, 1), "41"),
    ]
    expected_create_clean_main_job_role_column_rows = [
        ("101", date(2024, 1, 1), "-1", "1", MainJobRoleLabels.senior_management),
        ("101", date(2025, 1, 1), "1", "1", MainJobRoleLabels.senior_management),
        ("103", date(2024, 1, 1), "3", "3", MainJobRoleLabels.first_line_manager),
        ("103", date(2025, 1, 1), "4", "4", MainJobRoleLabels.registered_manager),
        ("141", date(2025, 1, 1), "41", "40", MainJobRoleLabels.care_coordinator),
    ]

    replace_care_navigator_with_care_coordinator_values_updated_when_care_navigator_is_present_rows = [
        ("41", "41"),
    ]
    expected_replace_care_navigator_with_care_coordinator_values_updated_when_care_navigator_is_present_rows = [
        ("41", "40"),
    ]
    replace_care_navigator_with_care_coordinator_values_remain_unchanged_when_care_navigator_not_present_rows = [
        ("25", "25"),
        ("40", "40"),
    ]
    expected_replace_care_navigator_with_care_coordinator_values_remain_unchanged_when_care_navigator_not_present_rows = [
        ("25", "25"),
        ("40", "40"),
    ]

    impute_not_known_job_roles_returns_next_known_value_when_before_first_known_value_rows = [
        ("1001", date(2024, 1, 1), "-1"),
        ("1001", date(2024, 3, 1), "8"),
        ("1002", date(2024, 1, 1), "-1"),
        ("1002", date(2024, 6, 1), "7"),
    ]
    expected_impute_not_known_job_roles_returns_next_known_value_when_before_first_known_value_rows = [
        ("1001", date(2024, 1, 1), "8"),
        ("1001", date(2024, 3, 1), "8"),
        ("1002", date(2024, 1, 1), "7"),
        ("1002", date(2024, 6, 1), "7"),
    ]

    impute_not_known_job_roles_returns_previously_known_value_when_after_known_value_rows = [
        ("1001", date(2024, 3, 1), "8"),
        ("1001", date(2024, 4, 1), "-1"),
        ("1002", date(2024, 3, 1), "7"),
        ("1002", date(2024, 8, 1), "-1"),
    ]
    expected_impute_not_known_job_roles_returns_previously_known_value_when_after_known_value_rows = [
        ("1001", date(2024, 3, 1), "8"),
        ("1001", date(2024, 4, 1), "8"),
        ("1002", date(2024, 3, 1), "7"),
        ("1002", date(2024, 8, 1), "7"),
    ]

    impute_not_known_job_roles_returns_previously_known_value_when_in_between_known_values_rows = [
        ("1001", date(2024, 3, 1), "8"),
        ("1001", date(2024, 4, 1), "-1"),
        ("1001", date(2024, 5, 1), "-1"),
        ("1001", date(2024, 6, 1), "7"),
    ]
    expected_impute_not_known_job_roles_returns_previously_known_value_when_in_between_known_values_rows = [
        ("1001", date(2024, 3, 1), "8"),
        ("1001", date(2024, 4, 1), "8"),
        ("1001", date(2024, 5, 1), "8"),
        ("1001", date(2024, 6, 1), "7"),
    ]

    impute_not_known_job_roles_returns_not_known_when_job_role_never_known_rows = [
        ("1001", date(2024, 1, 1), "-1"),
    ]
    expected_impute_not_known_job_roles_returns_not_known_when_job_role_never_known_rows = [
        ("1001", date(2024, 1, 1), "-1"),
    ]

    remove_workers_with_not_known_job_role_rows = [
        ("1001", date(2024, 3, 1), "8"),
        ("1002", date(2024, 3, 1), "-1"),
        ("1002", date(2024, 4, 1), "-1"),
    ]
    expected_remove_workers_with_not_known_job_role_rows = [
        ("1001", date(2024, 3, 1), "8"),
    ]


@dataclass
class CQCProviderData:
    sample_rows_full = [
        (
            "1-10000000001",
            ["1-12000000001"],
            "Provider",
            "Organisation",
            "Independent Healthcare Org",
            "10000000001",
            "Care Solutions Direct Limited",
            "Registered",
            "2022-01-14",
            None,
            "Threefield House",
            "Southampton",
            None,
            "South East",
            "AA10 3LP",
            50.93761444091797,
            -1.452439546585083,
            "0238206106",
            "10000001",
            "Adult social care",
            "Southampton, Itchen",
            "Southampton",
            "20230405",
        ),
        (
            "1-10000000002",
            ["1-12000000002"],
            "Provider",
            "Partnership",
            "Social Care Org",
            "10000000002",
            "Care Solutions Direct Limited",
            "Registered",
            "2022-01-14",
            None,
            "Threefield House",
            "Southampton",
            "Some County",
            "South East",
            "AA10 3LP",
            50.93761444091797,
            -1.452439546585083,
            "0238206106",
            "10000002",
            "Adult social care",
            "Southampton, Itchen",
            "Southampton",
            "20230405",
        ),
        (
            "1-10000000003",
            ["1-12000000003"],
            "Provider",
            "Individual",
            "Social Care Org",
            "10000000003",
            "Care Solutions Direct Limited",
            "Deregistered",
            "2022-01-14",
            "2022-03-07",
            "Threefield House",
            "Southampton",
            None,
            "South East",
            "SO14 3LP",
            50.93761444091797,
            -1.452439546585083,
            "0238206106",
            "10000003",
            "Adult social care",
            "Southampton, Itchen",
            "Southampton",
            "20230405",
        ),
    ]

    sector_rows = [
        "1-10000000002",
        "1-10000000003",
        "1-10000000004",
        "1-10000000005",
    ]

    rows_without_cqc_sector = [
        ("1-10000000001", "data"),
        ("1-10000000002", None),
        ("1-10000000003", "data"),
    ]

    expected_rows_with_cqc_sector = [
        ("1-10000000001", "data", Sector.independent),
        ("1-10000000002", None, Sector.local_authority),
        ("1-10000000003", "data", Sector.local_authority),
    ]


@dataclass
class CapacityTrackerCareHomeData:
    capacity_tracker_care_home_rows = [
        (
            "loc 1",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "2024",
            "01",
            "01",
            "20240101",
            "other data",
        ),
    ]

    remove_matching_agency_and_non_agency_rows = [
        ("loc 1", "1", "2", "3", "4", "5", "6"),
        ("loc 2", "1", "2", "3", "1", "5", "6"),
        ("loc 3", "1", "2", "3", "4", "2", "6"),
        ("loc 4", "1", "2", "3", "4", "5", "3"),
        ("loc 5", "1", "2", "3", "1", "2", "6"),
        ("loc 6", "1", "2", "3", "1", "5", "3"),
        ("loc 7", "1", "2", "3", "4", "2", "3"),
        ("loc 8", "1", "2", "3", "1", "2", "3"),
    ]
    expected_remove_matching_agency_and_non_agency_rows = [
        ("loc 1", "1", "2", "3", "4", "5", "6"),
        ("loc 2", "1", "2", "3", "1", "5", "6"),
        ("loc 3", "1", "2", "3", "4", "2", "6"),
        ("loc 4", "1", "2", "3", "4", "5", "3"),
        ("loc 5", "1", "2", "3", "1", "2", "6"),
        ("loc 6", "1", "2", "3", "1", "5", "3"),
        ("loc 7", "1", "2", "3", "4", "2", "3"),
    ]

    create_new_columns_with_totals_rows = [
        ("loc 1", 1, 2, 3, 40, 50, 60),
    ]
    expected_create_new_columns_with_totals_rows = [
        ("loc 1", 1, 2, 3, 40, 50, 60, 6, 150, 156),
    ]


@dataclass
class CapacityTrackerNonResData:
    capacity_tracker_non_res_rows = [
        ("loc 1", "12", "300", "2024", "01", "01", "20240101", "other data"),
    ]


@dataclass
class CQCLocationsData:
    sample_rows = [
        (
            "location1",
            "provider1",
            "Location",
            "Social Care Org",
            "name of location",
            "Registered",
            "2020-01-01",
            None,
            "N",
            20,
            "www.website.org",
            "1 The Street",
            "Leeds",
            "West Yorkshire",
            "Yorkshire",
            "LS1 2AB",
            "50.123455",
            "-5.6789",
            "Y",
            "Adult social care",
            "01234567891",
            "Trafford",
            [
                {
                    "name": "Personal care",
                    "code": "RA1",
                    "contacts": [
                        {
                            "personfamilyname": "Doe",
                            "persongivenname": "John",
                            "personroles": ["Registered Manager"],
                            "persontitle": "Mr",
                        }
                    ],
                }
            ],
            [{"name": "Homecare agencies", "description": "Domiciliary care service"}],
            [{"name": "Services for everyone"}],
            {
                CQCL.overall: {
                    CQCL.organisation_id: None,
                    CQCL.rating: "Overall rating Excellent",
                    CQCL.report_date: "report_date",
                    CQCL.report_link_id: None,
                    CQCLNew.use_of_resources: {
                        CQCL.organisation_id: None,
                        CQCLNew.summary: None,
                        CQCLNew.use_of_resources_rating: None,
                        CQCLNew.combined_quality_summary: None,
                        CQCLNew.combined_quality_rating: None,
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
                CQCLNew.service_ratings: [
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
                    CQCLNew.service_ratings: [
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
                        CQCLNew.use_of_resources: {
                            CQCLNew.combined_quality_rating: None,
                            CQCLNew.combined_quality_summary: None,
                            CQCLNew.use_of_resources_rating: None,
                            CQCLNew.use_of_resources_summary: None,
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
                    CQCL.related_location_id: "1",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                }
            ],
            "2020-01-01",
        ),
    ]

    impute_historic_relationships_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.registered, None),
        ("1-002", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.registered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        ("1-003", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
        ("1-004", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        ("1-005", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-005",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        ("1-006", date(2024, 1, 1), RegistrationStatus.deregistered, None),
        (
            "1-006",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        ("1-007", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-007",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 3, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    expected_impute_historic_relationships_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.registered, None, None),
        (
            "1-002",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.registered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        ("1-003", date(2024, 1, 1), RegistrationStatus.registered, None, None),
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
        (
            "1-004",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-005",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-005",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-006",
            date(2024, 1, 1),
            RegistrationStatus.deregistered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-006",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-007",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 3, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    impute_historic_relationships_when_type_is_none_returns_none_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.registered, None),
    ]
    expected_impute_historic_relationships_when_type_is_none_returns_none_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.registered, None, None),
    ]
    impute_historic_relationships_when_type_is_predecessor_returns_predecessor_rows = [
        ("1-002", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.registered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    expected_impute_historic_relationships_when_type_is_predecessor_returns_predecessor_rows = [
        (
            "1-002",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.registered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    impute_historic_relationships_when_type_is_successor_returns_none_when_registered_rows = [
        ("1-003", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
    ]
    expected_impute_historic_relationships_when_type_is_successor_returns_none_when_registered_rows = [
        ("1-003", date(2024, 1, 1), RegistrationStatus.registered, None, None),
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
    ]
    impute_historic_relationships_when_type_is_successor_returns_successor_when_deregistered_rows = [
        ("1-004", date(2024, 1, 1), RegistrationStatus.deregistered, None),
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
    ]
    expected_impute_historic_relationships_when_type_is_successor_returns_successor_when_deregistered_rows = [
        (
            "1-004",
            date(2024, 1, 1),
            RegistrationStatus.deregistered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
    ]
    impute_historic_relationships_when_type_has_both_types_only_returns_predecessors_when_registered_rows = [
        ("1-005", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-005",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]
    expected_impute_historic_relationships_when_type_has_both_types_only_returns_predecessors_when_registered_rows = [
        (
            "1-005",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-005",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
            [
                {
                    CQCL.related_location_id: "1-0051",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0052",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0053",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]
    impute_historic_relationships_when_type_has_both_types_returns_original_values_when_deregistered_rows = [
        ("1-006", date(2024, 1, 1), RegistrationStatus.deregistered, None),
        (
            "1-006",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]
    expected_impute_historic_relationships_when_type_has_both_types_returns_original_values_when_deregistered_rows = [
        (
            "1-006",
            date(2024, 1, 1),
            RegistrationStatus.deregistered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
        (
            "1-006",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
            [
                {
                    CQCL.related_location_id: "1-0061",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0062",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0063",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]
    impute_historic_relationships_where_different_relationships_over_time_returns_first_found_rows = [
        ("1-007", date(2024, 1, 1), RegistrationStatus.registered, None),
        (
            "1-007",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 3, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    expected_impute_historic_relationships_where_different_relationships_over_time_returns_first_found_rows = [
        (
            "1-007",
            date(2024, 1, 1),
            RegistrationStatus.registered,
            None,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0071",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
        (
            "1-007",
            date(2024, 3, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0072",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]

    get_relationships_where_type_is_none_returns_none_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.deregistered, None),
    ]
    expected_get_relationships_where_type_is_none_returns_none_rows = [
        ("1-001", date(2024, 1, 1), RegistrationStatus.deregistered, None, None),
    ]
    get_relationships_where_type_is_successor_returns_none_rows = [
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
        ),
    ]
    expected_get_relationships_where_type_is_successor_returns_none_rows = [
        (
            "1-002",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0021",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                }
            ],
            None,
        ),
    ]
    get_relationships_where_type_is_predecessor_returns_predecessor_rows = [
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    expected_get_relationships_where_type_is_predecessor_returns_predecessor_rows = [
        (
            "1-003",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
            [
                {
                    CQCL.related_location_id: "1-0031",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                }
            ],
        ),
    ]
    get_relationships_where_type_has_both_types_only_returns_predecessor_rows = [
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0042",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0043",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]
    expected_get_relationships_where_type_has_both_types_only_returns_predecessor_rows = [
        (
            "1-004",
            date(2024, 2, 1),
            RegistrationStatus.deregistered,
            [
                {
                    CQCL.related_location_id: "1-0041",
                    CQCL.related_location_name: "Name after",
                    CQCL.type: "HSCA Successor",
                    CQCL.reason: "Location Move",
                },
                {
                    CQCL.related_location_id: "1-0042",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0043",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
            [
                {
                    CQCL.related_location_id: "1-0042",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
                {
                    CQCL.related_location_id: "1-0043",
                    CQCL.related_location_name: "Name before",
                    CQCL.type: "HSCA Predecessor",
                    CQCL.reason: "New Provider",
                },
            ],
        ),
    ]

    # fmt: off
    impute_missing_struct_column_rows = [
        ("1-001", date(2024, 1, 1), []),
        ("1-001", date(2024, 2, 1), None),
        ("1-001", date(2024, 3, 1), [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 4, 1), []),
        ("1-001", date(2024, 5, 1), None),
        ("1-001", date(2024, 6, 1), [{"name": "Name B", "description": "Desc B"}, {"name": "Name C", "description": "Desc C"}]),
        ("1-001", date(2024, 7, 1), None),
        ("1-001", date(2024, 8, 1), []),
        ("1-002", date(2024, 1, 1), []),
        ("1-002", date(2024, 2, 1), None),
    ]
    expected_impute_missing_struct_column_rows = [
        ("1-001", date(2024, 1, 1), [], [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 2, 1), None, [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 3, 1), [{"name": "Name A", "description": "Desc A"}], [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 4, 1), [], [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 5, 1), None, [{"name": "Name A", "description": "Desc A"}]),
        ("1-001", date(2024, 6, 1), [{"name": "Name B", "description": "Desc B"}, {"name": "Name C", "description": "Desc C"}], [{"name": "Name B", "description": "Desc B"}, {"name": "Name C", "description": "Desc C"}]),
        ("1-001", date(2024, 7, 1), None, [{"name": "Name B", "description": "Desc B"}, {"name": "Name C", "description": "Desc C"}]),
        ("1-001", date(2024, 8, 1), [], [{"name": "Name B", "description": "Desc B"}, {"name": "Name C", "description": "Desc C"}]),
        ("1-002", date(2024, 1, 1), [], None),
        ("1-002", date(2024, 2, 1), None, None),
    ]
    # fmt: on

    remove_locations_that_never_had_regulated_activities_rows = [
        (
            "loc 1",
            [
                {
                    "name": "Personal care",
                    "code": "RA1",
                    "contacts": [
                        {
                            "personfamilyname": "Doe",
                            "persongivenname": "John",
                            "personroles": ["Registered Manager"],
                            "persontitle": "Mr",
                        }
                    ],
                }
            ],
        ),
        ("loc 2", None),
    ]
    expected_remove_locations_that_never_had_regulated_activities_rows = [
        (
            "loc 1",
            [
                {
                    "name": "Personal care",
                    "code": "RA1",
                    "contacts": [
                        {
                            "personfamilyname": "Doe",
                            "persongivenname": "John",
                            "personroles": ["Registered Manager"],
                            "persontitle": "Mr",
                        }
                    ],
                }
            ],
        ),
    ]

    extract_from_struct_rows = [
        (
            "1-001",
            [
                {
                    CQCL.name: "Homecare agencies",
                    CQCL.description: Services.domiciliary_care_service,
                }
            ],
        ),
        (
            "1-002",
            [
                {
                    CQCL.name: "With nursing",
                    CQCL.description: Services.care_home_service_with_nursing,
                },
                {
                    CQCL.name: "Without nursing",
                    CQCL.description: Services.care_home_service_without_nursing,
                },
            ],
        ),
        (
            "1-003",
            None,
        ),
    ]
    expected_extract_from_struct_rows = [
        (
            "1-001",
            [
                {
                    CQCL.name: "Homecare agencies",
                    CQCL.description: Services.domiciliary_care_service,
                },
            ],
            [Services.domiciliary_care_service],
        ),
        (
            "1-002",
            [
                {
                    CQCL.name: "With nursing",
                    CQCL.description: Services.care_home_service_with_nursing,
                },
                {
                    CQCL.name: "Without nursing",
                    CQCL.description: Services.care_home_service_without_nursing,
                },
            ],
            [
                Services.care_home_service_with_nursing,
                Services.care_home_service_without_nursing,
            ],
        ),
        (
            "1-003",
            None,
            None,
        ),
    ]

    primary_service_type_rows = [
        (
            "location1",
            "provider1",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                }
            ],
        ),
        (
            "location2",
            "provider2",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                }
            ],
        ),
        (
            "location3",
            "provider3",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                }
            ],
        ),
        (
            "location4",
            "provider4",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
            ],
        ),
        (
            "location5",
            "provider5",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Fake",
                    "description": "Fake service",
                },
            ],
        ),
        (
            "location6",
            "provider6",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
            ],
        ),
        (
            "location7",
            "provider7",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
            ],
        ),
        (
            "location8",
            "provider8",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
            ],
        ),
        (
            "location9",
            "provider9",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
            ],
        ),
        (
            "location10",
            "provider10",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
            ],
        ),
    ]
    expected_primary_service_type_rows = [
        (
            "location1",
            "provider1",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                }
            ],
            PrimaryServiceType.non_residential,
        ),
        (
            "location2",
            "provider2",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                }
            ],
            PrimaryServiceType.care_home_with_nursing,
        ),
        (
            "location3",
            "provider3",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                }
            ],
            PrimaryServiceType.care_home_only,
        ),
        (
            "location4",
            "provider4",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
            ],
            PrimaryServiceType.care_home_with_nursing,
        ),
        (
            "location5",
            "provider5",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Fake",
                    "description": "Fake service",
                },
            ],
            PrimaryServiceType.care_home_only,
        ),
        (
            "location6",
            "provider6",
            [
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
            ],
            PrimaryServiceType.care_home_with_nursing,
        ),
        (
            "location7",
            "provider7",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
            ],
            PrimaryServiceType.care_home_with_nursing,
        ),
        (
            "location8",
            "provider8",
            [
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
            ],
            PrimaryServiceType.care_home_only,
        ),
        (
            "location9",
            "provider9",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
                {
                    "name": "Without nursing",
                    "description": "Care home service without nursing",
                },
            ],
            PrimaryServiceType.care_home_only,
        ),
        (
            "location10",
            "provider10",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
                {
                    "name": "With nursing",
                    "description": "Care home service with nursing",
                },
            ],
            PrimaryServiceType.care_home_with_nursing,
        ),
    ]

    realign_carehome_column_rows = [
        ("1", CareHome.care_home, PrimaryServiceType.care_home_only),
        ("2", CareHome.not_care_home, PrimaryServiceType.care_home_only),
        ("3", CareHome.care_home, PrimaryServiceType.care_home_with_nursing),
        ("4", CareHome.not_care_home, PrimaryServiceType.care_home_with_nursing),
        ("5", CareHome.care_home, PrimaryServiceType.non_residential),
        ("6", CareHome.not_care_home, PrimaryServiceType.non_residential),
    ]
    expected_realign_carehome_column_rows = [
        ("1", CareHome.care_home, PrimaryServiceType.care_home_only),
        ("2", CareHome.care_home, PrimaryServiceType.care_home_only),
        ("3", CareHome.care_home, PrimaryServiceType.care_home_with_nursing),
        ("4", CareHome.care_home, PrimaryServiceType.care_home_with_nursing),
        ("5", CareHome.not_care_home, PrimaryServiceType.non_residential),
        ("6", CareHome.not_care_home, PrimaryServiceType.non_residential),
    ]

    small_location_rows = [
        (
            "loc-1",
            "prov-1",
            "20200101",
        ),
        (
            "loc-2",
            "prov-1",
            "20200101",
        ),
        (
            "loc-3",
            "prov-2",
            "20200101",
        ),
        (
            "loc-4",
            "prov-2",
            "20210101",
        ),
    ]

    join_provider_rows = [
        (
            "prov-1",
            "Apple Tree Care Homes",
            "Local authority",
            date(2020, 1, 1),
        ),
        (
            "prov-2",
            "Sunshine Domestic Care",
            "Independent",
            date(2020, 1, 1),
        ),
        (
            "prov-3",
            "Sunny Days Domestic Care",
            "Independent",
            date(2020, 1, 1),
        ),
    ]

    expected_joined_rows = [
        (
            "loc-1",
            "prov-1",
            "Apple Tree Care Homes",
            "Local authority",
            date(2020, 1, 1),
            date(2020, 1, 1),
        ),
        (
            "loc-2",
            "prov-1",
            "Apple Tree Care Homes",
            "Local authority",
            date(2020, 1, 1),
            date(2020, 1, 1),
        ),
        (
            "loc-3",
            "prov-2",
            "Sunshine Domestic Care",
            "Independent",
            date(2020, 1, 1),
            date(2020, 1, 1),
        ),
        (
            "loc-4",
            "prov-2",
            "Sunshine Domestic Care",
            "Independent",
            date(2021, 1, 1),
            date(2020, 1, 1),
        ),
    ]

    test_invalid_postcode_data = [
        ("loc-1", "B69 E3G"),
        ("loc-2", "UB4 0EJ."),
        ("loc-3", "PO20 3BD"),
        ("loc-4", "PR! 9HL"),
        ("loc-5", None),
    ]

    expected_invalid_postcode_data = [
        ("loc-1", "B69 3EG"),
        ("loc-2", "UB4 0EJ"),
        ("loc-3", "PO20 3BD"),
        ("loc-4", "PR1 9HL"),
        ("loc-5", None),
    ]

    registration_status_with_missing_data_rows = [
        (
            "loc-1",
            "Registered",
        ),
        (
            "loc-2",
            "Deregistered",
        ),
        (
            "loc-3",
            "new value",
        ),
    ]

    registration_status_rows = [
        (
            "loc-1",
            "Registered",
        ),
        (
            "loc-2",
            "Deregistered",
        ),
    ]

    expected_registered_rows = [
        (
            "loc-1",
            "Registered",
        ),
    ]

    social_care_org_rows = [
        (
            "loc-1",
            "Any none ASC org",
        ),
        (
            "loc-2",
            "Social Care Org",
        ),
        (
            "loc-3",
            None,
        ),
    ]

    expected_social_care_org_rows = [
        (
            "loc-2",
            "Social Care Org",
        ),
    ]

    ons_postcode_directory_rows = [
        (
            "LS12AB",
            date(2021, 1, 1),
            "Leeds",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "Leeds",
            "Yorkshire & Humber",
        ),
        (
            "B693EG",
            date(2021, 1, 1),
            "York",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "York",
            "Yorkshire & Humber",
        ),
        (
            "PR19HL",
            date(2019, 1, 1),
            "Hull",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "East Riding of Yorkshire",
            "Yorkshire & Humber",
        ),
    ]

    locations_for_ons_join_rows = [
        ("loc-1", "prov-1", date(2020, 1, 1), "PR1 9AB", "Registered"),
        ("loc-2", "prov-1", date(2018, 1, 1), "B69 3EG", "Deregistered"),
        (
            "loc-3",
            "prov-2",
            date(2020, 1, 1),
            "PR1 9HL",
            "Deregistered",
        ),
        ("loc-4", "prov-2", date(2021, 1, 1), "LS1 2AB", "Registered"),
    ]

    expected_ons_join_with_null_rows = [
        (
            date(2019, 1, 1),
            "PR19AB",
            date(2020, 1, 1),
            "loc-1",
            "prov-1",
            None,
            None,
            None,
            None,
            None,
            "Registered",
        ),
        (
            None,
            "B693EG",
            date(2018, 1, 1),
            "loc-2",
            "prov-1",
            None,
            None,
            None,
            None,
            None,
            "Deregistered",
        ),
        (
            date(2019, 1, 1),
            "PR19HL",
            date(2020, 1, 1),
            "loc-3",
            "prov-2",
            "Hull",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "East Riding of Yorkshire",
            "Yorkshire & Humber",
            "Deregistered",
        ),
        (
            date(2021, 1, 1),
            "LS12AB",
            date(2021, 1, 1),
            "loc-4",
            "prov-2",
            "Leeds",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "Leeds",
            "Yorkshire & Humber",
            "Registered",
        ),
    ]

    expected_split_registered_no_nulls_rows = [
        (
            date(2019, 1, 1),
            "PR19AB",
            date(2020, 1, 1),
            "loc-1",
            "prov-1",
            "Somerset",
            "Oxen Lane",
            date(2021, 1, 1),
            "Somerset",
            "English Region",
            "Registered",
        ),
        (
            date(2021, 1, 1),
            "LS12AB",
            date(2021, 1, 1),
            "loc-4",
            "prov-2",
            "Leeds",
            "Yorkshire & Humber",
            date(2021, 1, 1),
            "Leeds",
            "Yorkshire & Humber",
            "Registered",
        ),
    ]

    # fmt: off
    remove_time_from_date_column_rows = [
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2018-01-01 00:00:00", "20231201", "2018-01-01 00:00:00"),
        ("loc_1", None, "20231101", None),
    ]
    expected_remove_time_from_date_column_rows = [
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2018-01-01 00:00:00", "20231201", "2018-01-01"),
        ("loc_1", None, "20231101", None),
    ]
    remove_late_registration_dates_rows = [
        ("loc_1", "20240101", "2024-01-01"),
        ("loc_2", "20240101", "2024-01-02"),
        ("loc_3", "20240101", "2023-12-31"),
        ("loc_4", "20240201", "2024-02-02"),
        ("loc_4", "20240301", "2024-02-02"),
    ]
    expected_remove_late_registration_dates_rows = [
        ("loc_1", "20240101", "2024-01-01"),
        ("loc_2", "20240101", None),
        ("loc_3", "20240101", "2023-12-31"),
        ("loc_4", "20240201", None),
        ("loc_4", "20240301", None),
    ]
    clean_registration_date_column_rows = [
        ("loc_1", "2018-01-01", "20240101"),
        ("loc_1", "2018-01-01 00:00:00", "20231201"),
        ("loc_1", None, "20231101"),
        ("loc_2", None, "20240101"),
        ("loc_2", None, "20231201"),
        ("loc_2", None, "20231101"),
    ]
    expected_clean_registration_date_column_rows = [
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2018-01-01 00:00:00", "20231201", "2018-01-01"),
        ("loc_1", None, "20231101", "2018-01-01"),
        ("loc_2", None, "20240101", "2023-11-01"),
        ("loc_2", None, "20231201", "2023-11-01"),
        ("loc_2", None, "20231101", "2023-11-01"),
    ]
    impute_missing_registration_dates_rows=expected_remove_time_from_date_column_rows
    expected_impute_missing_registration_dates_rows=[
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2018-01-01 00:00:00", "20231201", "2018-01-01"),
        ("loc_1", None, "20231101", "2018-01-01"),
    ]
    impute_missing_registration_dates_different_rows=[
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2017-01-01 00:00:00", "20231201", "2017-01-01"),
        ("loc_1", None, "20231101", None),
    ]
    expected_impute_missing_registration_dates_different_rows=[
        ("loc_1", "2018-01-01", "20240101", "2018-01-01"),
        ("loc_1", "2017-01-01 00:00:00", "20231201", "2017-01-01"),
        ("loc_1", None, "20231101", "2017-01-01"),
    ]
    impute_missing_registration_dates_missing_rows=[
        ("loc_2", None, "20240101", None),
        ("loc_2", None, "20231201", None),
        ("loc_2", None, "20231101", None),
    ]
    expected_impute_missing_registration_dates_missing_rows=[
        ("loc_2", None, "20240101", "2023-11-01"),
        ("loc_2", None, "20231201", "2023-11-01"),
        ("loc_2", None, "20231101", "2023-11-01"),
    ]
    # fmt: on

    calculate_time_registered_same_day_rows = [
        ("1-0001", date(2025, 1, 1), date(2025, 1, 1)),
    ]
    expected_calculate_time_registered_same_day_rows = [
        ("1-0001", date(2025, 1, 1), date(2025, 1, 1), 1),
    ]

    calculate_time_registered_exact_months_apart_rows = [
        ("1-0001", date(2024, 2, 1), date(2024, 1, 1)),
        ("1-0002", date(2020, 1, 1), date(2019, 1, 1)),
    ]
    expected_calculate_time_registered_exact_months_apart_rows = [
        ("1-0001", date(2024, 2, 1), date(2024, 1, 1), 2),
        ("1-0002", date(2020, 1, 1), date(2019, 1, 1), 13),
    ]

    calculate_time_registered_one_day_less_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 1), date(2024, 12, 2)),
        ("1-0002", date(2025, 6, 8), date(2025, 1, 9)),
    ]
    expected_calculate_time_registered_one_day_less_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 1), date(2024, 12, 2), 1),
        ("1-0002", date(2025, 6, 8), date(2025, 1, 9), 5),
    ]

    calculate_time_registered_one_day_more_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 2), date(2024, 12, 1)),
        ("1-0002", date(2025, 6, 1), date(2025, 1, 31)),
    ]
    expected_calculate_time_registered_one_day_more_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 2), date(2024, 12, 1), 2),
        ("1-0002", date(2025, 6, 1), date(2025, 1, 31), 5),
    ]

    clean_provider_id_column_rows = [
        ("loc_1", None, "20240101"),
        ("loc_1", "123456789", "20240201"),
        ("loc_1", None, "20240201"),
        ("loc_2", "223456789 223456789", "20240101"),
        ("loc_2", "223456789", "20240201"),
        ("loc_2", None, "20240301"),
    ]
    expected_clean_provider_id_column_rows = [
        ("loc_1", "123456789", "20240101"),
        ("loc_1", "123456789", "20240201"),
        ("loc_1", "123456789", "20240201"),
        ("loc_2", "223456789", "20240101"),
        ("loc_2", "223456789", "20240201"),
        ("loc_2", "223456789", "20240301"),
    ]
    long_provider_id_column_rows = [
        ("loc_2", "223456789 223456789", "20240101"),
        ("loc_2", "223456789", "20240201"),
        ("loc_2", None, "20240301"),
    ]
    expected_long_provider_id_column_rows = [
        ("loc_2", None, "20240101"),
        ("loc_2", "223456789", "20240201"),
        ("loc_2", None, "20240301"),
    ]
    fill_missing_provider_id_column_rows = [
        ("loc_1", None, "20240101"),
        ("loc_1", "123456789", "20240201"),
        ("loc_1", None, "20240201"),
    ]

    expected_fill_missing_provider_id_column_rows = [
        ("loc_1", "123456789", "20240101"),
        ("loc_1", "123456789", "20240201"),
        ("loc_1", "123456789", "20240201"),
    ]

    impute_missing_data_from_provider_dataset_single_value_rows = [
        (
            "prov_1",
            None,
            date(2024, 1, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 2, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 3, 1),
        ),
    ]

    expected_impute_missing_data_from_provider_dataset_rows = [
        (
            "prov_1",
            Sector.independent,
            date(2024, 1, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 2, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 3, 1),
        ),
    ]

    impute_missing_data_from_provider_dataset_multiple_values_rows = [
        (
            "prov_1",
            None,
            date(2024, 1, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 3, 1),
        ),
        (
            "prov_1",
            Sector.local_authority,
            date(2024, 2, 1),
        ),
    ]

    expected_impute_missing_data_from_provider_dataset_multiple_values_rows = [
        (
            "prov_1",
            Sector.local_authority,
            date(2024, 1, 1),
        ),
        (
            "prov_1",
            Sector.local_authority,
            date(2024, 2, 1),
        ),
        (
            "prov_1",
            Sector.independent,
            date(2024, 3, 1),
        ),
    ]
    test_only_service_specialist_colleges_rows = [
        (
            "loc 1",
            [Services.specialist_college_service],
        ),
        (
            "loc 4",
            [Services.care_home_service_with_nursing],
        ),
    ]
    test_multiple_services_specialist_colleges_rows = [
        (
            "loc 2",
            [
                Services.specialist_college_service,
                Services.acute_services_with_overnight_beds,
            ],
        ),
        (
            "loc 3",
            [
                Services.acute_services_with_overnight_beds,
                Services.specialist_college_service,
            ],
        ),
    ]
    test_without_specialist_colleges_rows = [
        (
            "loc 4",
            [Services.care_home_service_with_nursing],
        ),
    ]
    test_empty_array_specialist_colleges_rows = [
        (
            "loc 5",
            [],
        ),
    ]
    test_null_row_specialist_colleges_rows = [
        (
            "loc 6",
            None,
        ),
    ]
    expected_only_service_specialist_colleges_rows = [
        (
            "loc 4",
            [Services.care_home_service_with_nursing],
        ),
    ]
    expected_multiple_services_specialist_colleges_rows = (
        test_multiple_services_specialist_colleges_rows
    )
    expected_without_specialist_colleges_rows = test_without_specialist_colleges_rows
    expected_empty_array_specialist_colleges_rows = (
        test_empty_array_specialist_colleges_rows
    )
    expected_null_row_specialist_colleges_rows = test_null_row_specialist_colleges_rows

    add_related_location_column_rows = [
        ("loc 1", None),
        ("loc 2", []),
        (
            "loc 3",
            [
                {
                    CQCL.related_location_id: "1",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                }
            ],
        ),
        (
            "loc 4",
            [
                {
                    CQCL.related_location_id: "1",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                },
                {
                    CQCL.related_location_id: "2",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                },
            ],
        ),
    ]
    expected_add_related_location_column_rows = [
        ("loc 1", None, RelatedLocation.no_related_location),
        ("loc 2", [], RelatedLocation.no_related_location),
        (
            "loc 3",
            [
                {
                    CQCL.related_location_id: "1",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                }
            ],
            RelatedLocation.has_related_location,
        ),
        (
            "loc 4",
            [
                {
                    CQCL.related_location_id: "1",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                },
                {
                    CQCL.related_location_id: "2",
                    CQCL.related_location_name: "name",
                    CQCL.type: "type",
                    CQCL.reason: "reason",
                },
            ],
            RelatedLocation.has_related_location,
        ),
    ]


@dataclass
class ExtractRegisteredManagerNamesData:
    extract_registered_manager_rows = [
        (
            "1-001",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 1",
                    CQCL.code: "A1",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                        {
                            CQCL.person_family_name: "Surname_1b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
        (
            "1-002",
            date(2024, 1, 1),
            [
                {CQCL.name: "Activity 2a", CQCL.code: "A2a", CQCL.contacts: []},
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
    ]

    extract_contacts_information_two_activities_one_contact_each_rows = [
        (
            "1-002",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 2a",
                    CQCL.code: "A2a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
    ]
    expected_extract_contacts_information_two_activities_one_contact_each_rows = [
        (
            "1-002",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 2a",
                    CQCL.code: "A2a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            {
                CQCL.person_family_name: "Surname_2a",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
        (
            "1-002",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 2a",
                    CQCL.code: "A2a",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            {
                CQCL.person_family_name: "Surname_2b",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]

    extract_contacts_information_one_activity_two_contacts_rows = [
        (
            "1-001",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 1",
                    CQCL.code: "A1",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                        {
                            CQCL.person_family_name: "Surname_1b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        )
    ]
    expected_extract_contacts_information_one_activity_two_contacts_rows = [
        (
            "1-001",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 1",
                    CQCL.code: "A1",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                        {
                            CQCL.person_family_name: "Surname_1b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            {
                CQCL.person_family_name: "Surname_1a",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
        (
            "1-001",
            date(2024, 1, 1),
            [
                {
                    CQCL.name: "Activity 1",
                    CQCL.code: "A1",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_1a",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                        {
                            CQCL.person_family_name: "Surname_1b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            {
                CQCL.person_family_name: "Surname_1b",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]

    extract_contacts_information_two_activities_but_only_one_with_contacts_rows = [
        (
            "1-002",
            date(2024, 1, 1),
            [
                {CQCL.name: "Activity 2a", CQCL.code: "A2a", CQCL.contacts: []},
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
        ),
    ]
    expected_extract_contacts_information_two_activities_but_only_one_with_contacts_rows = [
        (
            "1-002",
            date(2024, 1, 1),
            [
                {CQCL.name: "Activity 2a", CQCL.code: "A2a", CQCL.contacts: []},
                {
                    CQCL.name: "Activity 2b",
                    CQCL.code: "A2b",
                    CQCL.contacts: [
                        {
                            CQCL.person_family_name: "Surname_2b",
                            CQCL.person_given_name: "Name",
                            CQCL.person_roles: ["Registered Manager"],
                            CQCL.person_title: "M",
                        },
                    ],
                },
            ],
            {
                CQCL.person_family_name: "Surname_2b",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]

    select_and_create_full_name_rows = [
        (
            "1-001",
            date(2024, 1, 1),
            "Y",
            {
                CQCL.person_family_name: "Surname_1",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
        (
            "1-002",
            date(2024, 1, 1),
            "Y",
            {
                CQCL.person_family_name: "Surname_2",
                CQCL.person_given_name: "Name",
                CQCL.person_roles: ["Registered Manager"],
                CQCL.person_title: "M",
            },
        ),
    ]
    expected_select_and_create_full_name_rows = [
        ("1-001", date(2024, 1, 1), "Name Surname_1"),
        ("1-002", date(2024, 1, 1), "Name Surname_2"),
    ]

    group_and_collect_names_with_duplicate_names_rows = [
        ("1-001", date(2024, 1, 1), "Y", "Name Surname_1"),
        ("1-001", date(2024, 1, 1), "Y", "Name Surname_1"),
        ("1-001", date(2024, 1, 1), "Y", "Name Surname_2"),
    ]
    expected_group_and_collect_names_with_duplicate_names_rows = [
        ("1-001", date(2024, 1, 1), ["Name Surname_2", "Name Surname_1"]),
    ]

    group_and_collect_names_with_different_ids_and_dates_rows = [
        ("1-001", date(2024, 1, 1), "Y", "Name Surname_1"),
        ("1-001", date(2024, 2, 1), "Y", "Name Surname_1"),
        ("1-001", date(2024, 2, 1), "Y", "Name Surname_2"),
        ("1-002", date(2024, 1, 1), "N", "Name Surname_3"),
    ]
    expected_group_and_collect_names_with_different_ids_and_dates_rows = [
        ("1-001", date(2024, 1, 1), ["Name Surname_1"]),
        ("1-001", date(2024, 2, 1), ["Name Surname_2", "Name Surname_1"]),
        ("1-002", date(2024, 1, 1), ["Name Surname_3"]),
    ]

    original_test_rows = [
        ("1-001", date(2024, 1, 1), "Y", 5),
        ("1-001", date(2024, 2, 1), "Y", 5),
        ("1-002", date(2024, 1, 1), "Y", 10),
        ("1-002", date(2024, 2, 1), "Y", 10),
    ]
    registered_manager_names_rows = [
        ("1-001", date(2024, 1, 1), ["Name Surname_1"]),
        ("1-001", date(2024, 2, 1), ["Name Surname_1", "Name Surname_2"]),
        ("1-002", date(2024, 2, 1), ["Name Surname_3"]),
    ]
    expected_join_with_original_rows = [
        ("1-001", date(2024, 1, 1), "Y", 5, ["Name Surname_1"]),
        ("1-001", date(2024, 2, 1), "Y", 5, ["Name Surname_1", "Name Surname_2"]),
        ("1-002", date(2024, 1, 1), "Y", 10, None),
        ("1-002", date(2024, 2, 1), "Y", 10, ["Name Surname_3"]),
    ]


@dataclass
class UtilsData:
    cqc_pir_rows = [
        ("1-1199876096", "Y", date(2022, 2, 1), date(2021, 5, 7)),
        ("1-1199876096", "Y", date(2022, 7, 1), date(2022, 5, 20)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 12)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 24)),
        ("1-1199876096", "N", date(2023, 6, 1), date(2023, 5, 24)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 24)),
    ]

    filter_to_max_value_rows = [
        ("1", date(2024, 1, 1), "20220101"),
        ("2", date(2024, 1, 1), "20230101"),
        ("3", date(2023, 1, 1), "20240101"),
    ]
    expected_filter_to_max_date_rows = [
        ("1", date(2024, 1, 1), "20220101"),
        ("2", date(2024, 1, 1), "20230101"),
    ]
    expected_filter_to_max_string_rows = [
        ("3", date(2023, 1, 1), "20240101"),
    ]

    select_rows_with_value_rows = [
        ("id_1", "keep"),
        ("id_2", "remove"),
    ]

    select_rows_with_non_null_values_rows = [
        ("1-00001", None),
        ("1-00002", 12.34),
        ("1-00003", -1.0),
    ]
    expected_select_rows_with_non_null_values_rows = [
        ("1-00002", 12.34),
        ("1-00003", -1.0),
    ]


@dataclass
class CleaningUtilsData:
    worker_rows = [
        ("1", "1", "100"),
        ("2", "1", "101"),
        ("3", "2", "102"),
        ("4", "2", "103"),
        ("5", None, "103"),
        ("6", "2", None),
    ]

    gender = {
        "1": "male",
        "2": "female",
    }

    nationality = {
        "100": "British",
        "101": "French",
        "102": "Spanish",
        "103": "Portuguese",
    }

    expected_rows_with_new_columns = [
        ("1", "1", "100", "male", "British"),
        ("2", "1", "101", "male", "French"),
        ("3", "2", "102", "female", "Spanish"),
        ("4", "2", "103", "female", "Portuguese"),
        ("5", None, "103", None, "Portuguese"),
        ("6", "2", None, "female", None),
    ]

    expected_rows_without_new_columns = [
        ("1", "male", "British"),
        ("2", "male", "French"),
        ("3", "female", "Spanish"),
        ("4", "female", "Portuguese"),
        ("5", None, "Portuguese"),
        ("6", "female", None),
    ]

    scale_data = [
        (23, 10.1, "non scale"),
        (-1, 10.1, "non scale"),
        (24, -20.345, "non scale"),
        (-234, 999.99, "non scale"),
    ]

    expected_scale_data = [
        (23, 10.1, "non scale", 23, 10.1),
        (-1, 10.1, "non scale", None, 10.1),
        (24, -20.345, "non scale", 24, None),
        (-234, 999.99, "non scale", None, None),
    ]
    #
    align_dates_primary_rows = [
        (date(2020, 1, 1), "loc 1"),
        (date(2020, 1, 8), "loc 1"),
        (date(2021, 1, 1), "loc 1"),
        (date(2021, 1, 1), "loc 2"),
    ]

    align_dates_secondary_rows = [
        (date(2018, 1, 1), "loc 1"),
        (date(2019, 1, 1), "loc 1"),
        (date(2020, 1, 1), "loc 1"),
        (date(2020, 2, 1), "loc 1"),
        (date(2021, 1, 8), "loc 1"),
        (date(2020, 2, 1), "loc 2"),
    ]

    expected_aligned_dates_rows = [
        (
            date(2020, 1, 1),
            date(2020, 1, 1),
        ),
        (
            date(2020, 1, 8),
            date(2020, 1, 1),
        ),
        (
            date(2021, 1, 1),
            date(2020, 2, 1),
        ),
    ]

    align_later_dates_secondary_rows = [
        (date(2020, 2, 1), "loc 1"),
        (date(2021, 1, 8), "loc 1"),
        (date(2020, 2, 1), "loc 2"),
    ]

    expected_later_aligned_dates_rows = [
        (
            date(2021, 1, 1),
            date(2020, 2, 1),
        ),
    ]

    expected_cross_join_rows = [
        (
            date(2020, 1, 1),
            date(2019, 1, 1),
        ),
        (
            date(2020, 1, 8),
            date(2019, 1, 1),
        ),
        (
            date(2021, 1, 1),
            date(2019, 1, 1),
        ),
        (
            date(2020, 1, 1),
            date(2020, 1, 1),
        ),
        (
            date(2020, 1, 8),
            date(2020, 1, 1),
        ),
        (
            date(2021, 1, 1),
            date(2020, 1, 1),
        ),
        (
            date(2020, 1, 1),
            date(2020, 2, 1),
        ),
        (
            date(2020, 1, 8),
            date(2020, 2, 1),
        ),
        (
            date(2021, 1, 1),
            date(2020, 2, 1),
        ),
        (
            date(2020, 1, 1),
            date(2021, 1, 8),
        ),
        (
            date(2020, 1, 8),
            date(2021, 1, 8),
        ),
        (
            date(2021, 1, 1),
            date(2021, 1, 8),
        ),
        (
            date(2020, 1, 1),
            date(2018, 1, 1),
        ),
        (
            date(2020, 1, 8),
            date(2018, 1, 1),
        ),
        (
            date(2021, 1, 1),
            date(2018, 1, 1),
        ),
    ]

    expected_merged_rows = [
        (date(2020, 1, 1), date(2020, 1, 1), "loc 1"),
        (date(2020, 1, 8), date(2020, 1, 1), "loc 1"),
        (date(2021, 1, 1), date(2020, 2, 1), "loc 1"),
        (date(2021, 1, 1), date(2020, 2, 1), "loc 2"),
    ]

    expected_later_merged_rows = [
        (date(2020, 1, 1), None, "loc 1"),
        (date(2020, 1, 8), None, "loc 1"),
        (date(2021, 1, 1), date(2020, 2, 1), "loc 1"),
        (date(2021, 1, 1), date(2020, 2, 1), "loc 2"),
    ]

    column_to_date_data = [
        ("20230102", date(2023, 1, 2)),
        ("20220504", date(2022, 5, 4)),
        ("20191207", date(2019, 12, 7)),
        ("19081205", date(1908, 12, 5)),
    ]
    reduce_dataset_to_earliest_file_per_month_rows = [
        ("loc 1", "20220101", "2022", "01", "01"),
        ("loc 2", "20220105", "2022", "01", "05"),
        ("loc 3", "20220205", "2022", "02", "05"),
        ("loc 4", "20220207", "2022", "02", "07"),
        ("loc 5", "20220301", "2022", "03", "01"),
        ("loc 6", "20220402", "2022", "04", "02"),
    ]
    expected_reduce_dataset_to_earliest_file_per_month_rows = [
        ("loc 1", "20220101", "2022", "01", "01"),
        ("loc 3", "20220205", "2022", "02", "05"),
        ("loc 5", "20220301", "2022", "03", "01"),
        ("loc 6", "20220402", "2022", "04", "02"),
    ]

    cast_to_int_rows = [
        (
            "loc 1",
            "20",
            "18",
        ),
    ]

    cast_to_int_errors_rows = [
        (
            "loc 1",
            "20",
            "18",
        ),
        (
            "loc 2",
            "ZO",
            "18",
        ),
        (
            "loc 3",
            "20",
            "IB",
        ),
        (
            "loc 4",
            "ZO",
            "IB",
        ),
    ]

    cast_to_int_expected_rows = [
        (
            "loc 1",
            20,
            18,
        ),
    ]

    cast_to_int_errors_expected_rows = [
        (
            "loc 1",
            20,
            18,
        ),
        (
            "loc 2",
            None,
            18,
        ),
        (
            "loc 3",
            20,
            None,
        ),
        (
            "loc 4",
            None,
            None,
        ),
    ]

    filled_posts_per_bed_ratio_rows = [
        ("1-000000001", 5.0, 100, CareHome.care_home),
        ("1-000000002", 2.0, 1, CareHome.care_home),
        ("1-000000003", None, 100, CareHome.care_home),
        ("1-000000004", 0.0, 1, CareHome.care_home),
        ("1-000000005", 5.0, None, CareHome.care_home),
        ("1-000000006", 2.0, 0, CareHome.care_home),
        ("1-000000007", None, 0, CareHome.care_home),
        ("1-000000008", 0.0, None, CareHome.care_home),
        ("1-000000009", None, None, CareHome.care_home),
        ("1-000000010", 0.0, 0, CareHome.care_home),
        ("1-000000011", 4.0, 10, CareHome.not_care_home),
    ]
    expected_filled_posts_per_bed_ratio_rows = [
        ("1-000000001", 5.0, 100, CareHome.care_home, 0.05),
        ("1-000000002", 2.0, 1, CareHome.care_home, 2.0),
        ("1-000000003", None, 100, CareHome.care_home, None),
        ("1-000000004", 0.0, 1, CareHome.care_home, 0.0),
        ("1-000000005", 5.0, None, CareHome.care_home, None),
        ("1-000000006", 2.0, 0, CareHome.care_home, None),
        ("1-000000007", None, 0, CareHome.care_home, None),
        ("1-000000008", 0.0, None, CareHome.care_home, None),
        ("1-000000009", None, None, CareHome.care_home, None),
        ("1-000000010", 0.0, 0, CareHome.care_home, None),
        ("1-000000011", 4.0, 10, CareHome.not_care_home, None),
    ]

    filled_posts_from_beds_and_ratio_rows = [
        ("loc 1", 0.5, 10),
        ("loc 2", None, 10),
        ("loc 3", 0.5, None),
    ]
    expected_filled_posts_from_beds_and_ratio_rows = [
        ("loc 1", 0.5, 10, 5.0),
        ("loc 2", None, 10, None),
        ("loc 3", 0.5, None, None),
    ]

    remove_duplicate_locationids_rows = [
        (date(2024, 1, 1), "1-001", date(2023, 1, 1)),
        (date(2024, 1, 1), "1-001", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-001", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-002", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-002", date(2023, 2, 1)),
    ]
    expected_remove_duplicate_locationids_descending_rows = [
        (date(2024, 1, 1), "1-001", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-001", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-002", date(2023, 2, 1)),
    ]
    expected_remove_duplicate_locationids_ascending_rows = [
        (date(2024, 1, 1), "1-001", date(2023, 1, 1)),
        (date(2024, 2, 1), "1-001", date(2023, 2, 1)),
        (date(2024, 2, 1), "1-002", date(2023, 2, 1)),
    ]

    create_banded_bed_count_column_rows = [
        ("1-001", CareHome.care_home, 1),
        ("1-002", CareHome.care_home, 24),
        ("1-003", CareHome.care_home, 500),
        ("1-004", CareHome.not_care_home, None),
    ]
    expected_create_banded_bed_count_column_rows = [
        ("1-001", CareHome.care_home, 1, 1.0),
        ("1-002", CareHome.care_home, 24, 6.0),
        ("1-003", CareHome.care_home, 500, 8.0),
        ("1-004", CareHome.not_care_home, None, None),
    ]


@dataclass
class MergeIndCQCData:
    clean_cqc_location_for_merge_rows = [
        (date(2024, 1, 1), "1-000000001", "Independent", "Y", 10),
        (date(2024, 1, 1), "1-000000002", "Independent", "N", None),
        (date(2024, 1, 1), "1-000000003", "Independent", "N", None),
        (date(2024, 2, 1), "1-000000001", "Independent", "Y", 10),
        (date(2024, 2, 1), "1-000000002", "Independent", "N", None),
        (date(2024, 2, 1), "1-000000003", "Independent", "N", None),
        (date(2024, 3, 1), "1-000000001", "Independent", "Y", 10),
        (date(2024, 3, 1), "1-000000002", "Independent", "N", None),
        (date(2024, 3, 1), "1-000000003", "Independent", "N", None),
    ]

    data_to_merge_without_care_home_col_rows = [
        (date(2024, 1, 1), "1-000000001", "1", 1),
        (date(2024, 1, 1), "1-000000003", "3", 2),
        (date(2024, 1, 5), "1-000000001", "1", 3),
        (date(2024, 1, 9), "1-000000001", "1", 4),
        (date(2024, 1, 9), "1-000000003", "3", 5),
        (date(2024, 3, 1), "1-000000003", "4", 6),
    ]
    # fmt: off
    expected_merged_without_care_home_col_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Independent", "Y", 10, "1", 1,),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, None, None,),
        ("1-000000003", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, "3", 2,),
        ("1-000000001", date(2024, 1, 9), date(2024, 2, 1), "Independent", "Y", 10, "1", 4,),
        ("1-000000002", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, None, None,),
        ("1-000000003", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, "3", 5,),
        ("1-000000001", date(2024, 3, 1), date(2024, 3, 1), "Independent", "Y", 10, None, None,),
        ("1-000000002", date(2024, 3, 1), date(2024, 3, 1), "Independent", "N", None, None, None,),
        ("1-000000003", date(2024, 3, 1), date(2024, 3, 1), "Independent", "N", None, "4", 6,),
    ]
    # fmt: on

    data_to_merge_with_care_home_col_rows = [
        ("1-000000001", "Y", date(2024, 1, 1), 10),
        ("1-000000002", "N", date(2024, 1, 1), 20),
        ("1-000000003", "Y", date(2024, 1, 1), 30),
        ("1-000000001", "Y", date(2024, 2, 1), 1),
        ("1-000000002", "N", date(2024, 2, 1), 4),
    ]
    # fmt: off
    expected_merged_with_care_home_col_rows = [
        (date(2024, 1, 1), "1-000000001", "Independent", "Y", 10, 10, date(2024, 1, 1)),
        (date(2024, 1, 1), "1-000000002", "Independent", "N", None, 20, date(2024, 1, 1)),
        (date(2024, 1, 1), "1-000000003", "Independent", "N", None, None, date(2024, 1, 1)),
        (date(2024, 2, 1), "1-000000001", "Independent", "Y", 10, 1, date(2024, 2, 1)),
        (date(2024, 2, 1), "1-000000002", "Independent", "N", None, 4, date(2024, 2, 1)),
        (date(2024, 2, 1), "1-000000003", "Independent", "N", None, None, date(2024, 2, 1)),
        (date(2024, 3, 1), "1-000000001", "Independent", "Y", 10, 1, date(2024, 2, 1)),
        (date(2024, 3, 1), "1-000000002", "Independent", "N", None, 4, date(2024, 2, 1)),
        (date(2024, 3, 1), "1-000000003", "Independent", "N", None, None, date(2024, 2, 1)),
    ]
    # fmt: on


@dataclass
class MergeCoverageData:
    # fmt: off
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
    ]
    # fmt: on

    clean_ascwds_workplace_for_merge_rows = [
        (date(2024, 1, 1), "1-000000001", date(2024, 1, 1), "1", 1),
        (date(2024, 1, 1), "1-000000003", date(2024, 1, 1), "3", 2),
        (date(2024, 1, 5), "1-000000001", date(2024, 1, 1), "1", 3),
        (date(2024, 1, 9), "1-000000001", date(2024, 1, 1), "1", 4),
        (date(2024, 1, 9), "1-000000003", date(2024, 1, 1), "3", 5),
        (date(2024, 3, 1), "1-000000003", date(2024, 1, 1), "4", 6),
    ]

    # fmt: off
    expected_cqc_and_ascwds_merged_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Name 1", "AB1 2CD", "Independent", "Y", 10, date(2024, 1, 1), "1", 1),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Name 2", "EF3 4GH", "Independent", "N", None, None, None, None),
        ("1-000000003", date(2024, 1, 1), date(2024, 1, 1), "Name 3", "IJ5 6KL", "Independent", "N", None, date(2024, 1, 1), "3", 2),
        ("1-000000001", date(2024, 1, 9), date(2024, 2, 1), "Name 1", "AB1 2CD", "Independent", "Y", 10, date(2024, 1, 1), "1", 4),
        ("1-000000002", date(2024, 1, 9), date(2024, 2, 1), "Name 2", "EF3 4GH", "Independent", "N", None, None, None, None),
        ("1-000000003", date(2024, 1, 9), date(2024, 2, 1), "Name 3", "IJ5 6KL", "Independent", "N", None, date(2024, 1, 1), "3", 5),
        ("1-000000001", date(2024, 3, 1), date(2024, 3, 1), "Name 1", "AB1 2CD", "Independent", "Y", 10, None, None, None),
        ("1-000000002", date(2024, 3, 1), date(2024, 3, 1), "Name 2", "EF3 4GH", "Independent", "N", None, None, None, None),
        ("1-000000003", date(2024, 3, 1), date(2024, 3, 1), "Name 3", "IJ5 6KL", "Independent", "N", None, date(2024, 1, 1), "4", 6),
    ]
    # fmt: on

    sample_in_ascwds_rows = [
        (None,),
        ("1",),
    ]

    expected_in_ascwds_rows = [
        (None, 0),
        ("1", 1),
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


@dataclass
class LmEngagementUtilsData:
    # fmt: off
    add_columns_for_locality_manager_dashboard_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024"), # in ascwds on both dates
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024"),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024"), # joins ascwds
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024"),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024"), # leaves ascwds
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024"),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024"), # multiple locations in one cssr
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024"),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024"),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024"),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024"),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024"),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024"),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024"),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024"),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024"),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024"),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024"),
    ]
    expected_add_columns_for_locality_manager_dashboard_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0, None, 1, 1, 1),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0, 0.0, 0, 0, 1),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0, 1.0, 1, 1, 1),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0, None, 1, 1, 1),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0, -1.0, -1, 0, 1),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0, 0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75, 0.75, 3, 3, 3),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1, 4),
    ]

    expected_calculate_la_coverage_monthly_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0),
    ]

    calculate_coverage_monthly_change_rows = expected_calculate_la_coverage_monthly_rows

    expected_calculate_coverage_monthly_change_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0, None),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0, 0.0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0, None),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0, 1.0),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0, None),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0, -1.0),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75, 0.75),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25),
    ]
    calculate_locations_monthly_change_rows = expected_calculate_coverage_monthly_change_rows

    expected_calculate_locations_monthly_change_rows = [
        ("loc 1", date(2024, 1, 1), "cssr 1", 1, "2024", 1.0, None, 0, 1),
        ("loc 1", date(2024, 2, 1), "cssr 1", 1, "2024", 1.0, 0.0, 1, 0),
        ("loc 2", date(2024, 1, 1), "cssr 2", 0, "2024", 0.0, None, 0, 0),
        ("loc 2", date(2024, 2, 1), "cssr 2", 1, "2024", 1.0, 1.0, 0, 1),
        ("loc 3", date(2024, 1, 1), "cssr 3", 1, "2024", 1.0, None, 0, 1),
        ("loc 3", date(2024, 2, 1), "cssr 3", 0, "2024", 0.0, -1.0, 1, -1),
        ("loc 4", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 4", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 0, 3),
        ("loc 4", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1),
        ("loc 5", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 5", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 0, 3),
        ("loc 5", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1),
        ("loc 6", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 6", date(2024, 2, 1), "cssr 4", 1, "2024", 0.75, 0.75, 0, 3),
        ("loc 6", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 1, 1),
        ("loc 7", date(2024, 1, 1), "cssr 4", 0, "2024", 0.0, None, 0, 0),
        ("loc 7", date(2024, 2, 1), "cssr 4", 0, "2024", 0.75, 0.75, 0, 3),
        ("loc 7", date(2024, 3, 1), "cssr 4", 1, "2024", 1.0, 0.25, 0, 1),
    ]

    calculate_new_registrations_rows = expected_calculate_locations_monthly_change_rows

    expected_calculate_new_registrations_rows = expected_add_columns_for_locality_manager_dashboard_rows
    # fmt: on


@dataclass
class IndCQCDataUtils:
    input_rows_for_adding_estimate_filled_posts_and_source = [
        ("1-000001", 10.0, None, 80.0),
        ("1-000002", None, 30.0, 50.0),
        ("1-000003", 20.0, 70.0, 60.0),
        ("1-000004", None, None, 40.0),
        ("1-000005", None, 0.5, 40.0),
        ("1-000006", -1.0, 10.0, 30.0),
    ]

    expected_rows_with_estimate_filled_posts_and_source = [
        ("1-000001", 10.0, None, 80.0, 10.0, "model_name_1"),
        ("1-000002", None, 30.0, 50.0, 30.0, "model_name_2"),
        ("1-000003", 20.0, 70.0, 60.0, 20.0, "model_name_1"),
        ("1-000004", None, None, 40.0, 40.0, "model_name_3"),
        ("1-000005", None, 0.5, 40.0, 40.0, "model_name_3"),
        ("1-000006", -1.0, 10.0, 30.0, 10.0, "model_name_2"),
    ]

    merge_columns_in_order_when_df_has_columns_of_multiple_datatypes = [
        (
            "1-000001",
            10.0,
            {
                MainJobRoleLabels.care_worker: 0.5,
                MainJobRoleLabels.registered_nurse: 0.5,
            },
        )
    ]

    merge_columns_in_order_when_columns_are_datatype_string = [
        ("1-000001", "string", "string")
    ]

    list_of_map_columns_to_be_merged = [
        IndCQC.ascwds_job_role_ratios,
        IndCQC.ascwds_job_role_rolling_ratio,
    ]

    # fmt: off
    merge_map_columns_in_order_when_only_ascwds_known = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         None)
    ]
    # fmt: on

    # fmt: off
    expected_merge_map_columns_in_order_when_only_ascwds_known = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         None,
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         IndCQC.ascwds_job_role_ratios)
    ]
    # fmt: on

    # fmt: off
    merge_map_columns_in_order_when_only_primary_service_known = [
        ("1-001",
         None,
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4})
    ]
    # fmt: on

    # fmt: off
    expected_merge_map_columns_in_order_when_only_primary_service_known = [
        ("1-001",
         None,
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4},
         IndCQC.ascwds_job_role_rolling_ratio)
    ]
    # fmt: on

    # fmt: off
    merge_map_columns_in_order_when_both_map_columns_populated = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4})
    ]
    # fmt: on

    # fmt: off
    expected_merge_map_columns_in_order_when_both_map_columns_populated = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4},
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         IndCQC.ascwds_job_role_ratios)
    ]
    # fmt: on

    merge_map_columns_in_order_when_both_null = [("1-001", None, None)]
    expected_merge_map_columns_in_order_when_both_null = [
        ("1-001", None, None, None, None)
    ]
    # fmt: on

    # fmt: off
    merge_map_columns_in_order_when_both_map_columns_populated_at_multiple_locations = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4}),
        ("1-002",
         {MainJobRoleLabels.care_worker: 0.7, MainJobRoleLabels.registered_nurse: 0.3},
         {MainJobRoleLabels.care_worker: 0.8, MainJobRoleLabels.registered_nurse: 0.2})
    ]
    # fmt: on

    # fmt: off
    expected_merge_map_columns_in_order_when_both_map_columns_populated_at_multiple_locations = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4},
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         IndCQC.ascwds_job_role_ratios),
        ("1-002",
         {MainJobRoleLabels.care_worker: 0.7, MainJobRoleLabels.registered_nurse: 0.3},
         {MainJobRoleLabels.care_worker: 0.8, MainJobRoleLabels.registered_nurse: 0.2},
         {MainJobRoleLabels.care_worker: 0.7, MainJobRoleLabels.registered_nurse: 0.3},
         IndCQC.ascwds_job_role_ratios)
    ]
    # fmt: on

    source_missing_rows = [
        ("1-000001", 8.0, None),
        ("1-000002", None, None),
        ("1-000003", 4.0, "already_populated"),
    ]

    expected_source_added_rows = [
        ("1-000001", 8.0, "model_name"),
        ("1-000002", None, None),
        ("1-000003", 4.0, "already_populated"),
    ]

    test_first_selection_rows = [
        ("loc 1", 1, None, 100.0),
        ("loc 1", 2, 2.0, 50.0),
        ("loc 1", 3, 3.0, 25.0),
    ]
    expected_test_first_selection_rows = [
        ("loc 1", 1, None, 100.0, 50.0),
        ("loc 1", 2, 2.0, 50.0, 50.0),
        ("loc 1", 3, 3.0, 25.0, 50.0),
    ]
    test_last_selection_rows = [
        ("loc 1", 1, 1.0, 100.0),
        ("loc 1", 2, 2.0, 50.0),
        ("loc 1", 3, None, 25.0),
    ]
    expected_test_last_selection_rows = [
        ("loc 1", 1, 1.0, 100.0, 50.0),
        ("loc 1", 2, 2.0, 50.0, 50.0),
        ("loc 1", 3, None, 25.0, 50.0),
    ]

    # fmt: off
    copy_and_fill_filled_posts_when_becoming_not_dormant_rows = [
        ("1-001", date(2025, 1, 1), PrimaryServiceType.non_residential, "Y", 10.0),
        ("1-001", date(2025, 2, 1), PrimaryServiceType.non_residential, "Y", 11.0),
        ("1-001", date(2025, 3, 1), PrimaryServiceType.non_residential, "N", 100.0),
        ("1-001", date(2025, 4, 1), PrimaryServiceType.non_residential, "N", 100.0),
        ("1-001", date(2025, 5, 1), PrimaryServiceType.non_residential, "Y", 10.0),
        ("1-001", date(2025, 6, 1), PrimaryServiceType.non_residential, "Y", 10.0),
        ("1-002", date(2025, 1, 1), PrimaryServiceType.non_residential, "Y", 10.0),
        ("1-002", date(2025, 2, 1), PrimaryServiceType.non_residential, None, 10.0),
        ("1-003", date(2025, 1, 1), PrimaryServiceType.non_residential, "N", 10.0),
        ("1-003", date(2025, 2, 1), PrimaryServiceType.non_residential, "Y", 10.0),
        ("1-004", date(2025, 1, 1), PrimaryServiceType.non_residential, "N", 10.0),
        ("1-004", date(2025, 2, 1), PrimaryServiceType.non_residential, None, 10.0),
        ("1-005", date(2025, 1, 1), PrimaryServiceType.non_residential, None, 10.0),
        ("1-005", date(2025, 2, 1), PrimaryServiceType.non_residential, "Y", 10.0),
        ("1-006", date(2025, 1, 1), PrimaryServiceType.non_residential, None, 10.0),
        ("1-006", date(2025, 2, 1), PrimaryServiceType.non_residential, "N", 10.0),
        ("1-007", date(2025, 1, 1), PrimaryServiceType.non_residential, None, 10.0),
        ("1-007", date(2025, 2, 1), PrimaryServiceType.non_residential, None, 10.0),
        ("1-008", date(2025, 1, 1), PrimaryServiceType.care_home_only, "Y", 10.0),
        ("1-008", date(2025, 2, 1), PrimaryServiceType.care_home_only, "N", 10.0),
    ]
    expected_copy_and_fill_filled_posts_when_becoming_not_dormant_rows = [
        ("1-001", date(2025, 1, 1), PrimaryServiceType.non_residential, "Y", 10.0, None),
        ("1-001", date(2025, 2, 1), PrimaryServiceType.non_residential, "Y", 11.0, 11.0),
        ("1-001", date(2025, 3, 1), PrimaryServiceType.non_residential, "N", 100.0, 11.0),
        ("1-001", date(2025, 4, 1), PrimaryServiceType.non_residential, "N", 100.0, None),
        ("1-001", date(2025, 5, 1), PrimaryServiceType.non_residential, "Y", 10.0, None),
        ("1-001", date(2025, 6, 1), PrimaryServiceType.non_residential, "Y", 10.0, None),
        ("1-002", date(2025, 1, 1), PrimaryServiceType.non_residential, "Y", 10.0, None),
        ("1-002", date(2025, 2, 1), PrimaryServiceType.non_residential, None, 10.0, None),
        ("1-003", date(2025, 1, 1), PrimaryServiceType.non_residential, "N", 10.0, None),
        ("1-003", date(2025, 2, 1), PrimaryServiceType.non_residential, "Y", 10.0, None),
        ("1-004", date(2025, 1, 1), PrimaryServiceType.non_residential, "N", 10.0, None),
        ("1-004", date(2025, 2, 1), PrimaryServiceType.non_residential, None, 10.0, None),
        ("1-005", date(2025, 1, 1), PrimaryServiceType.non_residential, None, 10.0, None),
        ("1-005", date(2025, 2, 1), PrimaryServiceType.non_residential, "Y", 10.0, None),
        ("1-006", date(2025, 1, 1), PrimaryServiceType.non_residential, None, 10.0, None),
        ("1-006", date(2025, 2, 1), PrimaryServiceType.non_residential, "N", 10.0, None),
        ("1-007", date(2025, 1, 1), PrimaryServiceType.non_residential, None, 10.0, None),
        ("1-007", date(2025, 2, 1), PrimaryServiceType.non_residential, None, 10.0, None),
        ("1-008", date(2025, 1, 1), PrimaryServiceType.care_home_only, "Y", 10.0, None),
        ("1-008", date(2025, 2, 1), PrimaryServiceType.care_home_only, "N", 10.0, None),
    ]
    # fmt: on

    overwrite_estimate_filled_posts_with_imputed_rows = [
        (1.0, 2.0),
        (None, 2.0),
    ]
    expected_overwrite_estimate_filled_posts_with_imputed_rows = [
        (1.0, 1.0),
        (None, 2.0),
    ]

    flag_dormancy_has_changed_over_time_rows = [
        ("loc 1", date(2025, 1, 1), "Y"),
        ("loc 1", date(2025, 2, 1), "N"),
        ("loc 2", date(2025, 1, 1), None),
        ("loc 2", date(2025, 2, 1), "Y"),
        ("loc 2", date(2025, 3, 1), "N"),
        ("loc 3", date(2025, 1, 1), None),
        ("loc 3", date(2025, 2, 1), "Y"),
        ("loc 4", date(2025, 1, 1), "Y"),
        ("loc 4", date(2025, 2, 1), "Y"),
        ("loc 5", date(2025, 1, 1), None),
        ("loc 5", date(2025, 2, 1), None),
    ]
    expected_flag_dormancy_has_changed_over_time_rows = [
        ("loc 1", date(2025, 1, 1), "Y", True),
        ("loc 1", date(2025, 2, 1), "N", True),
        ("loc 2", date(2025, 1, 1), None, None),
        ("loc 2", date(2025, 2, 1), "Y", True),
        ("loc 2", date(2025, 3, 1), "N", True),
        ("loc 3", date(2025, 1, 1), None, None),
        ("loc 3", date(2025, 2, 1), "Y", False),
        ("loc 4", date(2025, 1, 1), "Y", False),
        ("loc 4", date(2025, 2, 1), "Y", False),
        ("loc 5", date(2025, 1, 1), None, None),
        ("loc 5", date(2025, 2, 1), None, None),
    ]

    # fmt: off
    get_period_when_dormancy_changed_rows = [
        ("loc 1", date(2025, 1, 1), "Y", 10.0),
        ("loc 1", date(2025, 1, 2), "Y", 10.0),
        ("loc 1", date(2025, 1, 3), "N", 100.0),
        ("loc 2", date(2025, 1, 1), "N", 100.0),
        ("loc 2", date(2025, 1, 2), "N", 100.0),
        ("loc 2", date(2025, 1, 3), "Y", 10.0),
        ("loc 3", date(2025, 1, 1), "Y", 1.0),
        ("loc 3", date(2025, 1, 2), "Y", 10.0),
        ("loc 3", date(2025, 1, 3), "N", 100.0),
        ("loc 3", date(2025, 1, 4), "N", 1000.0),
        ("loc 3", date(2025, 1, 5), "Y", 10.0),
        ("loc 3", date(2025, 1, 6), "Y", 1.0),
        ("loc 4", date(2025, 1, 1), "Y", 10.0),
        ("loc 4", date(2025, 1, 2), "Y", 10.0),
        ("loc 5", date(2025, 1, 1), None, 10.0),
        ("loc 5", date(2025, 1, 2), None, 10.0),
        ("loc 6", date(2025, 1, 1), None, 10.0),
        ("loc 6", date(2025, 1, 2), "Y", 10.0),
        ("loc 7", date(2025, 1, 1), "Y", 10.0),
        ("loc 7", date(2025, 1, 2), None, 10.0),
    ]
    expected_get_period_when_dormancy_changed_rows = [
        ("loc 1", date(2025, 1, 1), "Y", 10.0, None, None, None, None, 10.0),
        ("loc 1", date(2025, 1, 2), "Y", 10.0, None, None, None, None, 10.0),
        ("loc 1", date(2025, 1, 3), "N", 100.0, "Y", date(2025, 1, 2), 10.0, 1.0, 11.0),
        ("loc 2", date(2025, 1, 1), "N", 100.0, None, None, None, None, 100.0),
        ("loc 2", date(2025, 1, 2), "N", 100.0, None, None, None, None, 100.0),
        ("loc 2", date(2025, 1, 3), "Y", 10.0, "N", date(2025, 1, 2), 100.0, 1.0, 90.0),
        ("loc 3", date(2025, 1, 1), "Y", 1.0, None, None, None, None, 1.0),
        ("loc 3", date(2025, 1, 2), "Y", 10.0, None, None, None, None, 10.0),
        ("loc 3", date(2025, 1, 3), "N", 100.0, "Y", date(2025, 1, 2), 10.0, 1.0, 11.0),
        ("loc 3", date(2025, 1, 4), "N", 1000.0, "Y", date(2025, 1, 2), 10.0, 2.0, 12.0),
        ("loc 3", date(2025, 1, 5), "Y", 10.0, "N", date(2025, 1, 2), 10.0, 3.0, 7.0),
        ("loc 3", date(2025, 1, 6), "Y", 1.0, "N", date(2025, 1, 2), 10.0, 4.0, 6.0),
        ("loc 4", date(2025, 1, 1), "Y", 10.0, None, None, None, None, 10.0),
        ("loc 4", date(2025, 1, 2), "Y", 10.0, None, None, None, None, 10.0),
        ("loc 5", date(2025, 1, 1), None, 10.0, None, None, None, None, 10.0),
        ("loc 5", date(2025, 1, 2), None, 10.0, None, None, None, None, 10.0),
        ("loc 6", date(2025, 1, 1), None, 10.0, None, None, None, None, 10.0),
        ("loc 6", date(2025, 1, 2), "Y", 10.0, None, None, None, None, 10.0),
        ("loc 7", date(2025, 1, 1), "Y", 10.0, None, None, None, None, 10.0),
        ("loc 7", date(2025, 1, 2), None, 10.0, None, None, None, None, 10.0),
    ]
    # fmt: on

    flag_location_has_ascwds_value_rows = [
        ("loc 1", 1, EstimateFilledPostsSource.ascwds_pir_merged),
        ("loc 1", 2, EstimateFilledPostsSource.ascwds_pir_merged),
        ("loc 2", 1, EstimateFilledPostsSource.ascwds_pir_merged),
        ("loc 2", 2, EstimateFilledPostsSource.non_res_combined_model),
        ("loc 3", 1, EstimateFilledPostsSource.ascwds_pir_merged),
        ("loc 3", 2, None),
        ("loc 4", 1, EstimateFilledPostsSource.non_res_combined_model),
        ("loc 4", 2, EstimateFilledPostsSource.non_res_combined_model),
        ("loc 5", 1, None),
        ("loc 5", 2, None),
    ]
    expected_flag_location_has_ascwds_value_rows = [
        ("loc 1", 1, EstimateFilledPostsSource.ascwds_pir_merged, True),
        ("loc 1", 2, EstimateFilledPostsSource.ascwds_pir_merged, True),
        ("loc 2", 1, EstimateFilledPostsSource.ascwds_pir_merged, True),
        ("loc 2", 2, EstimateFilledPostsSource.non_res_combined_model, True),
        ("loc 3", 1, EstimateFilledPostsSource.ascwds_pir_merged, True),
        ("loc 3", 2, None, True),
        ("loc 4", 1, EstimateFilledPostsSource.non_res_combined_model, False),
        ("loc 4", 2, EstimateFilledPostsSource.non_res_combined_model, False),
        ("loc 5", 1, None, False),
        ("loc 5", 2, None, False),
    ]


@dataclass
class CleanIndCQCData:
    # fmt: off
    merged_rows_for_cleaning_job = [
        ("1-1000001", "20220201", date(2020, 2, 1), "South East", "Surrey", "Rural", "Y", 0, 5, 82, None, "Care home without nursing", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000001", "20220101", date(2022, 1, 1), "South East", "Surrey", "Rural", "Y", 5, 5, None, 67, "Care home without nursing", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000002", "20220101", date(2022, 1, 1), "South East", "Surrey", "Rural", "N", 0, 17, None, None, "non-residential", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000002", "20220201", date(2022, 2, 1), "South East", "Surrey", "Rural", "N", 0, 34, None, None, "non-residential", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000003", "20220301", date(2022, 3, 1), "North West", "Bolton", "Urban", "N", 0, 34, None, None, "non-residential", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000003", "20220308", date(2022, 3, 8), "North West", "Bolton", "Rural", "N", 0, 15, None, None, "non-residential", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000004", "20220308", date(2022, 3, 8), "South West", "Dorset", "Urban", "Y", 9, 0, 25, 25, "Care home with nursing", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
    ]
    # fmt: on

    remove_cqc_duplicates_when_carehome_and_asc_data_populated_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2018, 1, 1),
        ),
        (
            "loc 2",
            date(2024, 1, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2022, 1, 1),
        ),
    ]
    expected_remove_cqc_duplicates_when_carehome_and_asc_data_populated_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2018, 1, 1),
        ),
    ]

    remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_earlier_reg_date_rows = [
        (
            "loc 1",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            None,
            None,
            date(2018, 1, 1),
        ),
        (
            "loc 2",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2022, 1, 1),
        ),
    ]
    expected_remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_earlier_reg_date_rows = [
        (
            "loc 1",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2018, 1, 1),
        ),
    ]

    remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_later_reg_date_rows = [
        (
            "loc 1",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2018, 1, 1),
        ),
        (
            "loc 2",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            None,
            None,
            date(2022, 1, 1),
        ),
    ]
    expected_remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_later_reg_date_rows = [
        (
            "loc 1",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2018, 1, 1),
        ),
    ]

    remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_all_reg_dates_rows = [
        (
            "loc 1",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            None,
            None,
            date(2018, 1, 1),
        ),
        (
            "loc 2",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            None,
            None,
            date(2022, 1, 1),
        ),
    ]
    expected_remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_all_reg_dates_rows = [
        (
            "loc 1",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            None,
            None,
            date(2018, 1, 1),
        ),
    ]

    remove_cqc_duplicates_when_carehome_and_asc_data_different_on_all_reg_dates_rows = [
        (
            "loc 1",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2018, 1, 1),
        ),
        (
            "loc 2",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            11,
            11,
            date(2022, 1, 1),
        ),
    ]
    expected_remove_cqc_duplicates_when_carehome_and_asc_data_different_on_all_reg_dates_rows = [
        (
            "loc 1",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2018, 1, 1),
        ),
    ]

    remove_cqc_duplicates_when_carehome_and_registration_dates_the_same_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2022, 1, 1),
        ),
        (
            "loc 1",
            date(2024, 1, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2022, 1, 1),
        ),
    ]
    expected_remove_cqc_duplicates_when_carehome_and_registration_dates_the_same_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2022, 1, 1),
        ),
    ]

    remove_cqc_duplicates_when_non_res_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            "not care home",
            "AB1 2CD",
            CareHome.not_care_home,
            None,
            None,
            date(2022, 1, 1),
        ),
        (
            "loc 2",
            date(2024, 1, 1),
            "not care home",
            "AB1 2CD",
            CareHome.not_care_home,
            10,
            10,
            date(2022, 1, 1),
        ),
    ]
    expected_remove_cqc_duplicates_when_non_res_rows = (
        remove_cqc_duplicates_when_non_res_rows
    )

    repeated_value_rows = [
        ("1", 1, date(2023, 2, 1)),
        ("1", 2, date(2023, 3, 1)),
        ("1", 2, date(2023, 4, 1)),
        ("1", 3, date(2023, 8, 1)),
        ("2", 3, date(2023, 2, 1)),
        ("2", 9, date(2023, 4, 1)),
        ("2", 3, date(2024, 1, 1)),
        ("2", 3, date(2024, 2, 1)),
    ]

    expected_without_repeated_values_rows = [
        ("1", 1, date(2023, 2, 1), 1),
        ("1", 2, date(2023, 3, 1), 2),
        ("1", 2, date(2023, 4, 1), None),
        ("1", 3, date(2023, 8, 1), 3),
        ("2", 3, date(2023, 2, 1), 3),
        ("2", 9, date(2023, 4, 1), 9),
        ("2", 3, date(2024, 1, 1), 3),
        ("2", 3, date(2024, 2, 1), None),
    ]


@dataclass
class CalculateAscwdsFilledPostsData:
    # fmt: off
    calculate_ascwds_filled_posts_rows = [
        # Both 0: Return None
        ("1-000001", 0, None, None, None,),
        # Both 500: Return 500
        ("1-000002", 500, 500, None, None,),
        # Only know total_staff: Return None
        ("1-000003", 10, None, None, None,),
        # worker_record_count below min permitted: return None
        ("1-000004", 23, 1, None, None,),
        # Only know worker_records: None
        ("1-000005", None, 100, None, None,),
        # None of the rules apply: Return None
        ("1-000006", 900, 600, None, None,),
        # Absolute difference is within absolute bounds: Return Average
        ("1-000007", 12, 11, None, None,),
        # Absolute difference is within percentage bounds: Return Average
        ("1-000008", 500, 475, None, None,),
        # Already populated, shouldn't change it
        ("1-000009", 10, 10, 8.0, "already populated"),
    ]
    # fmt: on

    # fmt: off
    expected_ascwds_filled_posts_rows = [
        # Both 0: Return None
        ("1-000001", 0, None, None, None,),
        # Both 500: Return 500
        ("1-000002", 500, 500, 500.0, ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description,),
        # Only know total_staff: Return None
        ("1-000003", 10, None, None, None,),
        # worker_record_count below min permitted: return None
        ("1-000004", 23, 1, None, None,),
        # Only know worker_records: Return None
        ("1-000005", None, 100, None, None,),
        # None of the rules apply: Return None
        ("1-000006", 900, 600, None, None,),
        # Absolute difference is within absolute bounds: Return Average
        ("1-000007", 12, 11, 11.5, ascwds_filled_posts_difference_within_range_source_description,),
        # Absolute difference is within percentage bounds: Return Average
        ("1-000008", 500, 475, 487.5, ascwds_filled_posts_difference_within_range_source_description,),
        # Already populated, shouldn't change it
        ("1-000009", 10, 10, 10.0, ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description),
    ]
    # fmt: on


@dataclass
class CalculateAscwdsFilledPostsTotalStaffEqualWorkerRecordsData:
    # fmt: off
    calculate_ascwds_filled_posts_rows = [
        # Both 0: Return None
        ("1-000001", 0, None, None, None,),
        # Both 500: Return 500
        ("1-000002", 500, 500, None, None,),
        # Only know total_staff: Return None
        ("1-000003", 10, None, None, None,),
        # worker_record_count below min permitted: return None
        ("1-000004", 23, 1, None, None,),
        # Only know worker_records: None
        ("1-000005", None, 100, None, None,),
        # None of the rules apply: Return None
        ("1-000006", 900, 600, None, None,),
        # Absolute difference is within absolute bounds: Return Average
        ("1-000007", 12, 11, None, None,),
        # Absolute difference is within percentage bounds: Return Average
        ("1-000008", 500, 475, None, None,),
        # Already populated, shouldn't change it
        ("1-000009", 10, 10, 8.0, "already populated"),
    ]
    # fmt: on


@dataclass
class CalculateAscwdsFilledPostsDifferenceInRangeData:
    # fmt: off
    calculate_ascwds_filled_posts_rows = [
        # Both 0: Return None
        ("1-000001", 0, None, None, None,),
        # Both 500: Return 500
        ("1-000002", 500, 500, None, None,),
        # Only know total_staff: Return None
        ("1-000003", 10, None, None, None,),
        # worker_record_count below min permitted: return None
        ("1-000004", 23, 1, None, None,),
        # Only know worker_records: None
        ("1-000005", None, 100, None, None,),
        # None of the rules apply: Return None
        ("1-000006", 900, 600, None, None,),
        # Absolute difference is within absolute bounds: Return Average
        ("1-000007", 12, 11, None, None,),
        # Absolute difference is within percentage bounds: Return Average
        ("1-000008", 500, 475, None, None,),
        # Already populated, shouldn't change it
        ("1-000009", 10, 10, 8.0, "already populated"),
    ]
    # fmt: on

    # fmt: off
    expected_ascwds_filled_posts_rows = [
        # Both 0: Return None
        ("1-000001", 0, None, None, None,),
        # Both 500: Return 500
        ("1-000002", 500, 500, 500.0, ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description,),
        # Only know total_staff: Return None
        ("1-000003", 10, None, None, None,),
        # worker_record_count below min permitted: return None
        ("1-000004", 23, 1, None, None,),
        # Only know worker_records: Return None
        ("1-000005", None, 100, None, None,),
        # None of the rules apply: Return None
        ("1-000006", 900, 600, None, None,),
        # Absolute difference is within absolute bounds: Return Average
        ("1-000007", 12, 11, 11.5, ascwds_filled_posts_difference_within_range_source_description,),
        # Absolute difference is within percentage bounds: Return Average
        ("1-000008", 500, 475, 487.5, ascwds_filled_posts_difference_within_range_source_description,),
        # Already populated, shouldn't change it
        ("1-000009", 10, 10, 10.0, ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description),
    ]
    # fmt: on


@dataclass
class ReconciliationData:
    # fmt: off
    input_ascwds_workplace_rows = [
        (date(2024, 4, 1), "100", "A100", "No", "100", "Workplace has ownership", "Private sector", "Not regulated", None, "10", "Est Name 00", "1"),  # Single - not CQC regtype - INCLUDED
        (date(2024, 4, 1), "101", "A101", "No", "101", "Workplace has ownership", "Private sector", "CQC regulated", "1-001", "10", "Est Name 01", "1"),  # Single - ID matches - EXCLUDED
        (date(2024, 4, 1), "102", "A102", "No", "102", "Workplace has ownership", "Private sector", "CQC regulated", "1-902", "10", "Est Name 02", "2"),  # Single - ID matches dereg - EXCLUDED as deregistered before previous month
        (date(2024, 4, 1), "103", "A103", "No", "103", "Workplace has ownership", "Private sector", "CQC regulated", "1-903", "10", "Est Name 03", "3"),  # Single - ID matches dereg - INCLUDED
        (date(2024, 4, 1), "104", "A104", "No", "104", "Workplace has ownership", "Private sector", "CQC regulated", "1-501", "10", "Est Name 04", "4"),  # Single - ID doesn't exist in CQC - INCLUDED
        (date(2024, 4, 1), "105", "A105", "No", "105", "Workplace has ownership", "Private sector", "CQC regulated", None, "10", "Est Name 05", "5"),  # Single - missing CQC ID - INCLUDED
        (date(2024, 4, 1), "106", "A106", "No", "206", "Workplace has ownership", "Private sector", "CQC regulated", "1-002", "10", "Est Name 06", "6"),  # Sub - ID matches - EXCLUDED
        (date(2024, 4, 1), "107", "A107", "No", "207", "Workplace has ownership", "Private sector", "CQC regulated", "1-912", "10", "Est Name 07", "7"),  # Sub - ID matches dereg - EXCLUDED as deregistered before previous month
        (date(2024, 4, 1), "108", "A108", "No", "208", "Workplace has ownership", "Private sector", "CQC regulated", "1-913", "10", "Est Name 08", "8"),  # Sub - ID matches dereg - INCLUDED
        (date(2024, 4, 1), "109", "A109", "No", "209", "Workplace has ownership", "Private sector", "CQC regulated", "1-502", "10", "Est Name 09", "9"),  # Sub - ID doesn't exist in CQC - INCLUDED
        (date(2024, 4, 1), "110", "A110", "No", "210", "Workplace has ownership", "Private sector", "CQC regulated", None, "10", "Est Name 10", "9"),  # Sub - missing CQC ID - INCLUDED
        (date(2024, 4, 1), "111", "A111", "No", "211", "Workplace has ownership", "Private sector", "CQC regulated", "1-995", "10", "Est Name 11", "9"),  # Sub - ID dereg but in current month - EXCLUDED
        (date(2024, 4, 1), "112", "A112", "No", "212", "Workplace has ownership", "Private sector", "CQC regulated", "1-913", "72", "Est Name 08", "8"),  # Sub - ID matches dereg - INCLUDED (keep head office for incorect ID)
        (date(2024, 4, 1), "201", "A201", "Yes", "201", "Workplace has ownership", "Private sector", "Not regulated", None, "10", "Parent 01", "1"),  # Parent - has issues - INCLUDED
        (date(2024, 4, 1), "202", "A202", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", "1-003", "10", "Est Name 22", "2"),  # Parent - ID matches - EXCLUDED
        (date(2024, 4, 1), "203", "A203", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", "1-922", "10", "Est Name 23", "3"),  # Parent - ID matches dereg - INCLUDED (deregistered before previous month)
        (date(2024, 4, 1), "204", "A204", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", "1-923", "10", "Est Name 24", "4"),  # Parent - ID matches dereg - INCLUDED (deregistered in previous month)
        (date(2024, 4, 1), "205", "A205", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", "1-503", "10", "Est Name 25", "5"),  # Parent - ID doesn't exist in CQC - INCLUDED
        (date(2024, 4, 1), "206", "A206", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", None, "10", "Est Name 26", "6"),  # Parent - missing CQC ID - INCLUDED
        (date(2024, 4, 1), "206", "A206", "No", "201", "Parent has ownership", "Private sector", "CQC regulated", None, "72", "Est Name 26", "6"),  # Parent - head office - EXCLUDED
        (date(2024, 4, 1), "301", "A301", "Yes", "301", "Workplace has ownership", "Private sector", "CQC regulated", "1-004", "10", "Parent 02", "1"),  # Parent - no issues - EXCLUDED
    ]
    input_cqc_location_api_rows = [
        ("20240101", "1-901", "Deregistered", "2024-01-01"),
        ("20240401", "1-001", "Registered", None),
        ("20240401", "1-002", "Registered", None),
        ("20240401", "1-003", "Registered", None),
        ("20240401", "1-004", "Registered", None),
        ("20240401", "1-902", "Deregistered", "2024-01-01"),
        ("20240401", "1-903", "Deregistered", "2024-03-01"),
        ("20240401", "1-904", "Deregistered", "2024-03-01"),
        ("20240401", "1-912", "Deregistered", "2024-01-01"),
        ("20240401", "1-913", "Deregistered", "2024-03-01"),
        ("20240401", "1-922", "Deregistered", "2024-01-01"),
        ("20240401", "1-923", "Deregistered", "2024-03-01"),
        ("20240401", "1-995", "Deregistered", "2024-04-01"),
    ]


@dataclass
class CleanAscwdsFilledPostOutliersData:
    # fmt: off
    unfiltered_ind_cqc_rows = [
        ("01", "prov 1", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 30.0),
        ("02", "prov 1", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 35.0),
        ("03", "prov 1", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, None, 8.0),
    ]
    # fmt: on


@dataclass
class WinsorizeCareHomeFilledPostsPerBedRatioOutliersData:
    # fmt: off
    unfiltered_ind_cqc_rows = [
        ("01", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 2.0, 2.0, 2.0, 0.04, AscwdsFilteringRule.populated),
        ("02", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 4.0, 4.0, 4.0, 0.08, AscwdsFilteringRule.populated),
        ("03", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 6.0, 6.0, 6.0, 0.12, AscwdsFilteringRule.populated),
        ("04", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 8.0, 8.0, 8.0, 0.16, AscwdsFilteringRule.populated),
        ("05", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 10.0, 10.0, 10.0, 0.2, AscwdsFilteringRule.populated),
        ("06", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 15.0, 15.0, 15.0, 0.3, AscwdsFilteringRule.populated),
        ("07", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 20.0, 20.0, 20.0, 0.4, AscwdsFilteringRule.populated),
        ("08", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 25.0, 25.0, 25.0, 0.2, AscwdsFilteringRule.populated),
        ("09", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 30.0, 30.0, 30.0, 0.6, AscwdsFilteringRule.populated),
        ("10", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 35.0, 35.0, 35.0, 0.7, AscwdsFilteringRule.populated),
        ("11", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 37.0, 37.0, 37.0, 0.74, AscwdsFilteringRule.populated),
        ("12", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 37.5, 37.5, 37.5, 0.75, AscwdsFilteringRule.populated),
        ("13", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 38.0, 38.0, 38.0, 0.76, AscwdsFilteringRule.populated),
        ("14", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 39.0, 39.0, 39.0, 0.78, AscwdsFilteringRule.populated),
        ("15", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 40.0, 40.0, 40.0, 0.8, AscwdsFilteringRule.populated),
        ("16", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 41.0, 41.0, 41.0, 0.82, AscwdsFilteringRule.populated),
        ("17", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 42.0, 42.0, 42.0, 0.84, AscwdsFilteringRule.populated),
        ("18", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 43.0, 43.0, 43.0, 0.86, AscwdsFilteringRule.populated),
        ("19", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 44.0, 44.0, 44.0, 0.88, AscwdsFilteringRule.populated),
        ("20", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 45.0, 45.0, 45.0, 0.9, AscwdsFilteringRule.populated),
        ("21", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 46.0, 46.0, 46.0, 0.92, AscwdsFilteringRule.populated),
        ("22", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 47.0, 47.0, 47.0, 0.94, AscwdsFilteringRule.populated),
        ("23", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 48.0, 48.0, 48.0, 0.96, AscwdsFilteringRule.populated),
        ("24", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 49.0, 49.0, 49.0, 0.98, AscwdsFilteringRule.populated),
        ("25", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 50.0, 50.0, 50.0, 1.0, AscwdsFilteringRule.populated),
        ("26", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 51.0, 51.0, 51.0, 1.02, AscwdsFilteringRule.populated),
        ("27", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 52.0, 52.0, 52.0, 1.04, AscwdsFilteringRule.populated),
        ("28", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 53.0, 53.0, 53.0, 1.06, AscwdsFilteringRule.populated),
        ("29", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 54.0, 54.0, 54.0, 1.08, AscwdsFilteringRule.populated),
        ("30", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 55.0, 55.0, 55.0, 1.10, AscwdsFilteringRule.populated),
        ("31", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 56.0, 56.0, 56.0, 1.12, AscwdsFilteringRule.populated),
        ("32", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 57.0, 57.0, 57.0, 1.14, AscwdsFilteringRule.populated),
        ("33", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 58.0, 58.0, 58.0, 1.16, AscwdsFilteringRule.populated),
        ("34", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 59.0, 59.0, 59.0, 1.18, AscwdsFilteringRule.populated),
        ("35", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 60.0, 60.0, 60.0, 1.20, AscwdsFilteringRule.populated),
        ("36", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 61.0, 61.0, 61.0, 1.22, AscwdsFilteringRule.populated),
        ("37", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 62.0, 62.0, 62.0, 1.24, AscwdsFilteringRule.populated),
        ("38", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 63.0, 63.0, 63.0, 1.26, AscwdsFilteringRule.populated),
        ("39", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 250.0, 250.0, 250.0, 5.0, AscwdsFilteringRule.populated),
        ("40", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 500.0, 500.0, 500.0, 10.0, AscwdsFilteringRule.populated),
        ("41", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 6.0, None, None, None, None, AscwdsFilteringRule.missing_data),
        ("42", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, None, None, 42.0, 42.0, 42.0, None, AscwdsFilteringRule.populated),
        ("43", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, 25, 6.0, 43.0, 43.0, 43.0, 0.92, AscwdsFilteringRule.populated),
        ("44", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, None, None, 44.0, 44.0, 44.0, None, AscwdsFilteringRule.populated),
    ]
    # fmt: on

    # fmt: off
    expected_care_home_jobs_per_bed_ratio_filtered_rows = [
        ("01", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 2.0, 2.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("02", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 4.0, 4.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("03", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 6.0, 6.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("04", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 8.0, 8.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("05", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 10.0, 10.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("06", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 15.0, 15.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("07", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 20.0, 20.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("08", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 25.0, 25.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("09", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 30.0, 30.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("10", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 35.0, 35.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("11", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 37.0, 37.0, 37.5, 0.75, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("12", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 37.5, 37.5, 37.5, 0.75, AscwdsFilteringRule.populated),
        ("13", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 38.0, 38.0, 38.0, 0.76, AscwdsFilteringRule.populated),
        ("14", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 39.0, 39.0, 39.0, 0.78, AscwdsFilteringRule.populated),
        ("15", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 40.0, 40.0, 40.0, 0.8, AscwdsFilteringRule.populated),
        ("16", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 41.0, 41.0, 41.0, 0.82, AscwdsFilteringRule.populated),
        ("17", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 42.0, 42.0, 42.0, 0.84, AscwdsFilteringRule.populated),
        ("18", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 43.0, 43.0, 43.0, 0.86, AscwdsFilteringRule.populated),
        ("19", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 44.0, 44.0, 44.0, 0.88, AscwdsFilteringRule.populated),
        ("20", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 45.0, 45.0, 45.0, 0.9, AscwdsFilteringRule.populated),
        ("21", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 46.0, 46.0, 46.0, 0.92, AscwdsFilteringRule.populated),
        ("22", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 47.0, 47.0, 47.0, 0.94, AscwdsFilteringRule.populated),
        ("23", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 48.0, 48.0, 48.0, 0.96, AscwdsFilteringRule.populated),
        ("24", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 49.0, 49.0, 49.0, 0.98, AscwdsFilteringRule.populated),
        ("25", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 50.0, 50.0, 50.0, 1.0, AscwdsFilteringRule.populated),
        ("26", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 51.0, 51.0, 51.0, 1.02, AscwdsFilteringRule.populated),
        ("27", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 52.0, 52.0, 52.0, 1.04, AscwdsFilteringRule.populated),
        ("28", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 53.0, 53.0, 53.0, 1.06, AscwdsFilteringRule.populated),
        ("29", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 54.0, 54.0, 54.0, 1.08, AscwdsFilteringRule.populated),
        ("30", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 55.0, 55.0, 55.0, 1.10, AscwdsFilteringRule.populated),
        ("31", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 56.0, 56.0, 56.0, 1.12, AscwdsFilteringRule.populated),
        ("32", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 57.0, 57.0, 57.0, 1.14, AscwdsFilteringRule.populated),
        ("33", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 58.0, 58.0, 58.0, 1.16, AscwdsFilteringRule.populated),
        ("34", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 59.0, 59.0, 59.0, 1.18, AscwdsFilteringRule.populated),
        ("35", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 60.0, 60.0, 60.0, 1.20, AscwdsFilteringRule.populated),
        ("36", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 61.0, 61.0, 61.0, 1.22, AscwdsFilteringRule.populated),
        ("37", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 62.0, 62.0, 62.0, 1.24, AscwdsFilteringRule.populated),
        ("38", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 63.0, 63.0, 63.0, 1.26, AscwdsFilteringRule.populated),
        ("39", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 250.0, 250.0, 250.0, 5.0, AscwdsFilteringRule.populated),
        ("40", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 50, 7.0, 500.0, 500.0, 250.0, 5.0, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("41", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 6.0, None, None, None, None, AscwdsFilteringRule.missing_data),
        ("42", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, None, None, 42.0, 42.0, 42.0, None, AscwdsFilteringRule.populated),
        ("43", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, 25, 6.0, 43.0, 43.0, 43.0, 0.92, AscwdsFilteringRule.populated),
        ("44", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, None, None, 44.0, 44.0, 44.0, None, AscwdsFilteringRule.populated),
    ]
    # fmt: on

    filter_df_to_care_homes_with_known_beds_and_filled_posts_rows = [
        ("01", "Y", None, None),
        ("02", "Y", None, 0.0),
        ("03", "Y", None, 1.0),
        ("04", "Y", 0, None),
        ("05", "Y", 0, 0.0),
        ("06", "Y", 0, 1.0),
        ("07", "Y", 1, None),
        ("08", "Y", 1, 0.0),
        ("09", "Y", 1, 1.0),
        ("10", "N", None, None),
        ("11", "N", None, 0.0),
        ("12", "N", None, 1.0),
        ("13", "N", 0, None),
        ("14", "N", 0, 0.0),
        ("15", "N", 0, 1.0),
        ("16", "N", 1, None),
        ("17", "N", 1, 0.0),
        ("18", "N", 1, 1.0),
    ]

    expected_filtered_df_to_care_homes_with_known_beds_and_filled_posts_rows = [
        ("09", "Y", 1, 1.0),
    ]

    calculate_standardised_residuals_rows = [
        ("1", 55.5, 64.0),
        ("2", 25.0, 16.0),
    ]
    expected_calculate_standardised_residuals_rows = [
        ("1", 55.5, 64.0, -1.0625),
        ("2", 25.0, 16.0, 2.25),
    ]

    standardised_residual_percentile_cutoff_rows = [
        ("1", PrimaryServiceType.care_home_with_nursing, 0.54321),
        ("2", PrimaryServiceType.care_home_with_nursing, -3.2545),
        ("3", PrimaryServiceType.care_home_with_nursing, -4.2542),
        ("4", PrimaryServiceType.care_home_with_nursing, 2.41654),
        ("5", PrimaryServiceType.care_home_with_nursing, 25.0),
        ("6", PrimaryServiceType.care_home_only, 1.0),
        ("7", PrimaryServiceType.care_home_only, 2.0),
        ("8", PrimaryServiceType.care_home_only, 3.0),
    ]

    expected_standardised_residual_percentile_cutoff_with_percentiles_rows = [
        ("1", PrimaryServiceType.care_home_with_nursing, 0.54321, -3.454, 6.933),
        ("2", PrimaryServiceType.care_home_with_nursing, -3.2545, -3.454, 6.933),
        ("3", PrimaryServiceType.care_home_with_nursing, -4.2542, -3.454, 6.933),
        ("4", PrimaryServiceType.care_home_with_nursing, 2.41654, -3.454, 6.933),
        ("5", PrimaryServiceType.care_home_with_nursing, 25.0, -3.454, 6.933),
        ("6", PrimaryServiceType.care_home_only, 1.0, 1.4, 2.6),
        ("7", PrimaryServiceType.care_home_only, 2.0, 1.4, 2.6),
        ("8", PrimaryServiceType.care_home_only, 3.0, 1.4, 2.6),
    ]

    duplicate_ratios_within_standardised_residual_cutoff_rows = [
        ("1", 1.0, -2.50, -1.23, 1.23),
        ("2", 2.0, -1.23, -1.23, 1.23),
        ("3", 3.0, 0.00, -1.23, 1.23),
        ("4", 4.0, 1.23, -1.23, 1.23),
        ("5", 5.0, 1.25, -1.23, 1.23),
    ]
    expected_duplicate_ratios_within_standardised_residual_cutoff_rows = [
        ("1", 1.0, -2.50, -1.23, 1.23, None),
        ("2", 2.0, -1.23, -1.23, 1.23, 2.0),
        ("3", 3.0, 0.00, -1.23, 1.23, 3.0),
        ("4", 4.0, 1.23, -1.23, 1.23, 4.0),
        ("5", 5.0, 1.25, -1.23, 1.23, None),
    ]

    min_and_max_permitted_ratios_rows = [
        ("1", 0.55, 1.0),
        ("2", 5.88, 1.0),
        ("3", None, 1.0),
        ("4", 3.21, 2.0),
        ("5", 4.88, 2.0),
        ("6", None, 2.0),
    ]
    expected_min_and_max_permitted_ratios_rows = [
        ("1", 0.55, 1.0, 0.75, 5.88),
        ("2", 5.88, 1.0, 0.75, 5.88),
        ("3", None, 1.0, 0.75, 5.88),
        ("4", 3.21, 2.0, 3.21, 5.0),
        ("5", 4.88, 2.0, 3.21, 5.0),
        ("6", None, 2.0, 3.21, 5.0),
    ]

    winsorize_outliers_rows = [
        ("1", CareHome.care_home, 9.0, 15, 0.6, 1.0, 5.0),
        ("2", CareHome.care_home, 30.0, 15, 2.0, 1.0, 5.0),
        ("3", CareHome.care_home, 90.0, 15, 6.0, 1.0, 5.0),
    ]
    expected_winsorize_outliers_rows = [
        ("1", CareHome.care_home, 15.0, 15, 1.0, 1.0, 5.0),
        ("2", CareHome.care_home, 30.0, 15, 2.0, 1.0, 5.0),
        ("3", CareHome.care_home, 75.0, 15, 5.0, 1.0, 5.0),
    ]

    set_minimum_permitted_ratio_rows = [
        ("1", 0.05),
        ("2", 2.55),
    ]
    expected_set_minimum_permitted_ratio_rows = [
        ("1", 0.75),
        ("2", 2.55),
    ]

    combine_dataframes_care_home_rows = [
        (
            "01",
            date(2023, 1, 1),
            "Y",
            PrimaryServiceType.care_home_only,
            25,
            6.0,
            1.0,
            1.0,
            None,
            0.04,
            AscwdsFilteringRule.populated,
            10.0,
        ),
        (
            "02",
            date(2023, 1, 1),
            "Y",
            PrimaryServiceType.care_home_only,
            25,
            6.0,
            2.0,
            2.0,
            2.0,
            0.08,
            AscwdsFilteringRule.populated,
            20.0,
        ),
    ]

    combine_dataframes_non_care_home_rows = [
        (
            "03",
            date(2023, 1, 1),
            "N",
            PrimaryServiceType.non_residential,
            None,
            None,
            3.0,
            3.0,
            3.0,
            None,
            AscwdsFilteringRule.populated,
        ),
    ]

    expected_combined_dataframes_rows = [
        (
            "01",
            date(2023, 1, 1),
            "Y",
            PrimaryServiceType.care_home_only,
            25,
            6.0,
            1.0,
            1.0,
            None,
            0.04,
            AscwdsFilteringRule.populated,
        ),
        (
            "02",
            date(2023, 1, 1),
            "Y",
            PrimaryServiceType.care_home_only,
            25,
            6.0,
            2.0,
            2.0,
            2.0,
            0.08,
            AscwdsFilteringRule.populated,
        ),
        (
            "03",
            date(2023, 1, 1),
            "N",
            PrimaryServiceType.non_residential,
            None,
            None,
            3.0,
            3.0,
            3.0,
            None,
            AscwdsFilteringRule.populated,
        ),
    ]


@dataclass
class NonResAscwdsFeaturesData(object):
    # fmt: off
    rows = [
        ("1-00001", date(2022, 2, 1), date(2019, 1, 1), 35, Region.south_east, Dormancy.dormant, [Services.domiciliary_care_service], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia], PrimaryServiceType.non_residential, None, 20.0, 17.5, CareHome.not_care_home, RUI.rural_hamlet, RelatedLocation.has_related_location, '2022', '02', '01', '20220201'),
        ("1-00002", date(2022, 2, 1), date(2019, 2, 1), 36, Region.south_east, Dormancy.not_dormant, [Services.domiciliary_care_service], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia], PrimaryServiceType.non_residential, 67.0, 20.0, 20.0, CareHome.not_care_home, RUI.rural_hamlet, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
        ("1-00003", date(2022, 2, 1), date(2019, 2, 1), 36, Region.south_west, Dormancy.dormant, [Services.urgent_care_services, Services.supported_living_service], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia], PrimaryServiceType.non_residential, None, 20.0, 20.0, CareHome.not_care_home, RUI.rural_hamlet, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
        ("1-00004", date(2022, 2, 1), date(2019, 2, 1), 36, Region.north_east, None, [Services.domiciliary_care_service], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia], PrimaryServiceType.non_residential, None, 20.0, 20.0, CareHome.not_care_home, RUI.rural_hamlet, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
        ("1-00005", date(2022, 2, 1), date(2019, 2, 1), 36, Region.north_east, Dormancy.not_dormant, [Services.specialist_college_service, Services.domiciliary_care_service], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia, Specialisms.mental_health], PrimaryServiceType.non_residential, None, 20.0, 20.0, CareHome.not_care_home, RUI.urban_city, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
        ("1-00006", date(2022, 2, 1), date(2019, 2, 1), 36, Region.north_east, Dormancy.not_dormant, [Services.specialist_college_service, Services.domiciliary_care_service], [{IndCQC.name:"name", IndCQC.code: "code"}], None, PrimaryServiceType.non_residential, None, 20.0, 20.0, CareHome.not_care_home, RUI.urban_city, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
        ("1-00007", date(2022, 2, 1), date(2019, 2, 1), 36, Region.north_west, Dormancy.dormant, [Services.supported_living_service, Services.care_home_service_with_nursing], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia], PrimaryServiceType.care_home_with_nursing, None, 20.0, 20.0, CareHome.care_home, RUI.urban_city, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
    ]
    # fmt: on


@dataclass
class CareHomeFeaturesData:
    clean_merged_data_rows = [
        (
            "1-001",
            date(2022, 1, 1),
            Region.south_east,
            0,
            [Services.domiciliary_care_service],
            [Specialisms.dementia],
            [{IndCQC.name: "name", IndCQC.code: "code"}],
            CareHome.not_care_home,
            RUI.rural_hamlet_sparse,
            None,
            None,
            None,
            "2023",
            "01",
            "01",
            "20230101",
        ),
        (
            "1-002",
            date(2022, 1, 1),
            Region.yorkshire_and_the_humber,
            10,
            [Services.care_home_service_with_nursing],
            [Specialisms.dementia],
            [{IndCQC.name: "name", IndCQC.code: "code"}],
            CareHome.care_home,
            RUI.urban_city,
            1.8,
            2.5,
            1.5,
            "2023",
            "01",
            "01",
            "20230101",
        ),
        (
            "1-003",
            date(2022, 1, 1),
            Region.south_west,
            None,
            [Services.dental_service, Services.care_home_service_without_nursing],
            [Specialisms.dementia, Specialisms.mental_health],
            [{IndCQC.name: "name", IndCQC.code: "code"}],
            CareHome.care_home,
            RUI.rural_town,
            1.6,
            1.1,
            1.4,
            "2023",
            "01",
            "01",
            "20230101",
        ),
    ]


@dataclass
class EstimateIndCQCFilledPostsData:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-1783948", date(2022, 2, 1), "South East", "South East", 0, ["Domiciliary care service"], "non-residential", 5, None, None, "N", "Independent", "Rural hamlet and isolated dwellings in a sparse setting", "Rural hamlet and isolated dwellings in a sparse setting", "rule_1", "Registered"),
        ("1-1783948", date(2022, 1, 1), "South East", "South East", 0, ["Domiciliary care service"], "non-residential", 5, 67.0, 67.0, "N", "Independent", "Rural hamlet and isolated dwellings in a sparse setting", "Rural hamlet and isolated dwellings in a sparse setting", "rule_2", "Registered"),
        ("1-348374832", date(2022, 1, 12), "Merseyside", "Merseyside", 0, ["Extra Care housing services"], "non-residential", None, 34.0, 34.0, "N", "Local authority", "Rural hamlet and isolated dwellings", "Rural hamlet and isolated dwellings", "rule_3", "Registered"),
        ("1-683746776", date(2022, 1, 1), "Merseyside", "Merseyside", 0, ["Doctors treatment service", "Long term conditions services", "Shared Lives"], "non-residential", 34, None, None, "N", "Local authority", "Rural hamlet and isolated dwellings", "Rural hamlet and isolated dwellings", "rule_1", "Registered"),
        ("1-10478686", date(2022, 1, 1), "London Senate", "London Senate", 0, ["Community health care services - Nurses Agency only"], "non-residential", None, None, None, "N", "", "Rural hamlet and isolated dwellings", "Rural hamlet and isolated dwellings", "rule_1", "Registered"),
        ("1-10235302415", date(2022, 1, 12), "South West", "South West", 0, ["Urgent care services", "Supported living service"], "non-residential", 17, None, None, "N", "Independent", "Rural hamlet and isolated dwellings", "Rural hamlet and isolated dwellings", "rule_3", "Registered"),
        ("1-1060912125", date(2022, 1, 12), "Yorkshire and the Humber", "Yorkshire and the Humber", 0, ["Hospice services at home"], "non-residential", 34, None, None, "N", "Independent", "Rural hamlet and isolated dwellings", "Rural hamlet and isolated dwellings", "rule_2", "Registered"),
        ("1-107095666", date(2022, 3, 1), "Yorkshire and the Humber", "Yorkshire and the Humber", 0, ["Specialist college service", "Community based services for people who misuse substances", "Urgent care services'"], "non-residential", 34, None, None, "N", "Independent", "Urban city and town", "Urban city and town", "rule_3", "Registered"),
        ("1-108369587", date(2022, 3, 8), "South West", "South West", 0, ["Specialist college service"], "non-residential", 15, None, None, "N", "Independent", "Rural town and fringe in a sparse setting", "Rural town and fringe in a sparse setting", "rule_1", "Registered"),
        ("1-10758359583", date(2022, 3, 8), None, None, 0, ["Mobile doctors service"], "non-residential", 17, None, None, "N", "Local authority", "Urban city and town", "Urban city and town", "rule_2", "Registered"),
        ("1-000000001", date(2022, 3, 8), "Yorkshire and the Humber", "Yorkshire and the Humber", 67, ["Care home service with nursing"], "Care home with nursing", None, None, None, "Y", "Local authority", "Urban city and town", "Urban city and town", "rule_1", "Registered"),
        ("1-10894414510", date(2022, 3, 8), "Yorkshire and the Humber", "Yorkshire and the Humber", 10, ["Care home service with nursing"], "Care home with nursing", 0, 25.0, 25.0, "Y", "Independent", "Urban city and town", "Urban city and town", "rule_3", "Registered"),
        ("1-108950835", date(2022, 3, 15), "Merseyside", "Merseyside", 20, ["Care home service without nursing"], "Care home without nursing", 23, None, None, "Y", "", "Urban city and town", "Urban city and town", "rule_1", "Registered"),
        ("1-108967195", date(2022, 4, 22), "North West", "North West", 0, ["Supported living service", "Acute services with overnight beds"], "non-residential", 11, None, None, "N", "Independent", "Urban city and town", "Urban city and town", "rule_3", "Registered"),
    ]
    # fmt: on


@dataclass
class ModelPrimaryServiceRateOfChange:
    # fmt: off
    primary_service_rate_of_change_rows = [
        ("1-001", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 3.0),
        ("1-001", CareHome.care_home, 1704153600, PrimaryServiceType.care_home_only, 10, 2.8),
        ("1-001", CareHome.care_home, 1704240000, PrimaryServiceType.care_home_only, 10, 3.4),
        ("1-001", CareHome.care_home, 1704326400, PrimaryServiceType.care_home_only, 10, 3.2),
        ("1-002", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 2.0),
        ("1-002", CareHome.care_home, 1704153600, PrimaryServiceType.care_home_only, 10, None),
        ("1-002", CareHome.care_home, 1704240000, PrimaryServiceType.care_home_only, 10, None),
        ("1-002", CareHome.care_home, 1704326400, PrimaryServiceType.care_home_only, 10, 3.2),
        ("1-003", CareHome.not_care_home, 1704067200, PrimaryServiceType.non_residential, None, 40.0),
        ("1-003", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 50.0),
        ("1-004", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 60.0),
        ("1-005", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 4.0),
        ("1-005", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 50.0),
    ]
    expected_primary_service_rate_of_change_rows = [
        ("1-001", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 3.0, 1.0),
        ("1-001", CareHome.care_home, 1704153600, PrimaryServiceType.care_home_only, 10, 2.8, 1.03999),
        ("1-001", CareHome.care_home, 1704240000, PrimaryServiceType.care_home_only, 10, 3.4, 1.1176),
        ("1-001", CareHome.care_home, 1704326400, PrimaryServiceType.care_home_only, 10, 3.2, 1.0854),
        ("1-002", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 2.0, 1.0),
        ("1-002", CareHome.care_home, 1704153600, PrimaryServiceType.care_home_only, 10, None, 1.03999),
        ("1-002", CareHome.care_home, 1704240000, PrimaryServiceType.care_home_only, 10, None, 1.1176),
        ("1-002", CareHome.care_home, 1704326400, PrimaryServiceType.care_home_only, 10, 3.2, 1.0854),
        ("1-003", CareHome.not_care_home, 1704067200, PrimaryServiceType.non_residential, None, 40.0, 1.0),
        ("1-003", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 50.0, 1.25),
        ("1-004", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 60.0, 1.25),
        ("1-005", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 4.0, 1.0),
        ("1-005", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 50.0, 1.25),
    ]
    # fmt: on

    clean_column_with_values_rows = [
        ("1-001", 1000000001, CareHome.care_home, 10.0),
        ("1-001", 1000000002, CareHome.care_home, None),
        ("1-001", 1000000003, CareHome.care_home, 10.0),
    ]
    expected_clean_column_with_values_rows = [
        ("1-001", 1000000001, CareHome.care_home, 10.0, 1, 2),
        ("1-001", 1000000002, CareHome.care_home, None, 1, 2),
        ("1-001", 1000000003, CareHome.care_home, 10.0, 1, 2),
    ]

    clean_column_with_values_one_submission_rows = [
        ("1-001", 1000000001, CareHome.care_home, 10.0),
        ("1-001", 1000000002, CareHome.care_home, None),
    ]
    expected_clean_column_with_values_one_submission_rows = [
        ("1-001", 1000000001, CareHome.care_home, None, 1, 1),
        ("1-001", 1000000002, CareHome.care_home, None, 1, 1),
    ]

    clean_column_with_values_both_statuses_rows = [
        ("1-001", 1000000001, CareHome.care_home, 10.0),
        ("1-001", 1000000002, CareHome.care_home, 10.0),
        ("1-001", 1000000003, CareHome.not_care_home, 10.0),
    ]
    expected_clean_column_with_values_both_statuses_rows = [
        ("1-001", 1000000001, CareHome.care_home, None, 2, 2),
        ("1-001", 1000000002, CareHome.care_home, None, 2, 2),
        ("1-001", 1000000003, CareHome.not_care_home, None, 2, 1),
    ]

    calculate_care_home_status_count_rows = [
        ("1-001", CareHome.care_home),
        ("1-001", CareHome.care_home),
        ("1-002", CareHome.care_home),
        ("1-002", CareHome.not_care_home),
    ]
    expected_calculate_care_home_status_count_rows = [
        ("1-001", CareHome.care_home, 1),
        ("1-001", CareHome.care_home, 1),
        ("1-002", CareHome.care_home, 2),
        ("1-002", CareHome.not_care_home, 2),
    ]

    calculate_submission_count_same_care_home_status_rows = [
        ("1-001", CareHome.care_home, None),
        ("1-001", CareHome.care_home, None),
        ("1-002", CareHome.care_home, None),
        ("1-002", CareHome.care_home, 10.0),
        ("1-003", CareHome.care_home, 10.0),
        ("1-003", CareHome.care_home, 10.0),
    ]
    expected_calculate_submission_count_same_care_home_status_rows = [
        ("1-001", CareHome.care_home, None, 0),
        ("1-001", CareHome.care_home, None, 0),
        ("1-002", CareHome.care_home, None, 1),
        ("1-002", CareHome.care_home, 10.0, 1),
        ("1-003", CareHome.care_home, 10.0, 2),
        ("1-003", CareHome.care_home, 10.0, 2),
    ]

    calculate_submission_count_mixed_care_home_status_rows = [
        ("1-001", CareHome.not_care_home, 10.0),
        ("1-001", CareHome.care_home, 10.0),
        ("1-001", CareHome.care_home, 10.0),
    ]
    expected_calculate_submission_count_mixed_care_home_status_rows = [
        ("1-001", CareHome.not_care_home, 10.0, 1),
        ("1-001", CareHome.care_home, 10.0, 2),
        ("1-001", CareHome.care_home, 10.0, 2),
    ]

    interpolate_column_with_values_rows = [
        ("1-001", 1704067200, 30.0),
        ("1-001", 1704153600, None),
        ("1-001", 1704240000, 34.0),
        ("1-001", 1704326400, None),
    ]
    expected_interpolate_column_with_values_rows = [
        ("1-001", 1704067200, 30.0, 30.0),
        ("1-001", 1704153600, None, 32.0),
        ("1-001", 1704240000, 34.0, 34.0),
        ("1-001", 1704326400, None, None),
    ]

    add_previous_value_column_rows = [
        ("1-001", 1672531200, 1.1),
        ("1-001", 1672617600, 1.2),
        ("1-001", 1672704000, 1.3),
        ("1-001", 1672790400, 1.4),
        ("1-002", 1672617600, 10.2),
        ("1-002", 1672704000, 10.3),
    ]
    expected_add_previous_value_column_rows = [
        ("1-001", 1672531200, 1.1, None),
        ("1-001", 1672617600, 1.2, 1.1),
        ("1-001", 1672704000, 1.3, 1.2),
        ("1-001", 1672790400, 1.4, 1.3),
        ("1-002", 1672617600, 10.2, None),
        ("1-002", 1672704000, 10.3, 10.2),
    ]

    # fmt: off
    add_rolling_sum_columns_rows = [
        ("1-001", PrimaryServiceType.care_home_only, 1672531200, 1.1, None),
        ("1-001", PrimaryServiceType.care_home_only, 1672617600, 1.2, 1.1),
        ("1-001", PrimaryServiceType.care_home_only, 1672704000, 1.3, 1.2),
        ("1-001", PrimaryServiceType.care_home_only, 1672790400, None, 1.3),
        ("1-002", PrimaryServiceType.care_home_only, 1672531200, 1.4, None),
        ("1-002", PrimaryServiceType.care_home_only, 1672617600, 1.3, 1.4),
        ("1-003", PrimaryServiceType.non_residential, 1672531200, 10.0, None),
        ("1-003", PrimaryServiceType.non_residential, 1672617600, 20.0, 10.0),
        ("1-003", PrimaryServiceType.non_residential, 1672704000, 30.0, 20.0),
    ]
    expected_add_rolling_sum_columns_rows = [
        ("1-001", PrimaryServiceType.care_home_only, 1672531200, 1.1, None, None, None),
        ("1-001", PrimaryServiceType.care_home_only, 1672617600, 1.2, 1.1, 2.5, 2.5),
        ("1-001", PrimaryServiceType.care_home_only, 1672704000, 1.3, 1.2, 3.8, 3.7),
        ("1-001", PrimaryServiceType.care_home_only, 1672790400, None, 1.3, 3.8, 3.7),
        ("1-002", PrimaryServiceType.care_home_only, 1672531200, 1.4, None, None, None),
        ("1-002", PrimaryServiceType.care_home_only, 1672617600, 1.3, 1.4, 2.5, 2.5),
        ("1-003", PrimaryServiceType.non_residential, 1672531200, 10.0, None, None, None),
        ("1-003", PrimaryServiceType.non_residential, 1672617600, 20.0, 10.0, 20.0, 10.0),
        ("1-003", PrimaryServiceType.non_residential, 1672704000, 30.0, 20.0, 50.0, 30.0),
    ]
    # fmt: on

    calculate_rate_of_change_rows = [
        ("1-001", 12.0, 10.0),
        ("1-002", 15.0, None),
        ("1-003", None, 20.0),
        ("1-004", None, None),
    ]
    expected_calculate_rate_of_change_rows = [
        ("1-001", 12.0, 10.0, 1.2),
        ("1-002", 15.0, None, 1.0),
        ("1-003", None, 20.0, 1.0),
        ("1-004", None, None, 1.0),
    ]


@dataclass
class ModelPrimaryServiceRateOfChangeTrendlineData:
    # fmt: off
    primary_service_rate_of_change_trendline_rows = [
        ("1-001", 1704067200, CareHome.care_home, PrimaryServiceType.care_home_only, 3.0),
        ("1-001", 1704153600, CareHome.care_home, PrimaryServiceType.care_home_only, 2.8),
        ("1-001", 1704240000, CareHome.care_home, PrimaryServiceType.care_home_only, 3.4),
        ("1-001", 1704326400, CareHome.care_home, PrimaryServiceType.care_home_only, 3.2),
        ("1-002", 1704067200, CareHome.care_home, PrimaryServiceType.care_home_only, 2.0),
        ("1-002", 1704153600, CareHome.care_home, PrimaryServiceType.care_home_only, None),
        ("1-002", 1704240000, CareHome.care_home, PrimaryServiceType.care_home_only, None),
        ("1-002", 1704326400, CareHome.care_home, PrimaryServiceType.care_home_only, 3.2),
        ("1-003", 1704067200, CareHome.not_care_home, PrimaryServiceType.non_residential, 40.0),
        ("1-003", 1704153600, CareHome.not_care_home, PrimaryServiceType.non_residential, 50.0),
        ("1-004", 1704153600, CareHome.not_care_home, PrimaryServiceType.non_residential, 60.0),
        ("1-005", 1704067200, CareHome.care_home, PrimaryServiceType.care_home_only, 4.0),
        ("1-005", 1704153600, CareHome.not_care_home, PrimaryServiceType.non_residential, 50.0),
    ]
    expected_primary_service_rate_of_change_trendline_rows = [
        ("1-001", 1704067200, CareHome.care_home, PrimaryServiceType.care_home_only, 3.0, 1.0),
        ("1-001", 1704153600, CareHome.care_home, PrimaryServiceType.care_home_only, 2.8, 1.03999),
        ("1-001", 1704240000, CareHome.care_home, PrimaryServiceType.care_home_only, 3.4, 1.16235),
        ("1-001", 1704326400, CareHome.care_home, PrimaryServiceType.care_home_only, 3.2, 1.26158),
        ("1-002", 1704067200, CareHome.care_home, PrimaryServiceType.care_home_only, 2.0, 1.0),
        ("1-002", 1704153600, CareHome.care_home, PrimaryServiceType.care_home_only, None, 1.03999),
        ("1-002", 1704240000, CareHome.care_home, PrimaryServiceType.care_home_only, None, 1.16235),
        ("1-002", 1704326400, CareHome.care_home, PrimaryServiceType.care_home_only, 3.2, 1.26158),
        ("1-003", 1704067200, CareHome.not_care_home, PrimaryServiceType.non_residential, 40.0, 1.0),
        ("1-003", 1704153600, CareHome.not_care_home, PrimaryServiceType.non_residential, 50.0, 1.25),
        ("1-004", 1704153600, CareHome.not_care_home, PrimaryServiceType.non_residential, 60.0, 1.25),
        ("1-005", 1704067200, CareHome.care_home, PrimaryServiceType.care_home_only, 4.0, 1.0),
        ("1-005", 1704153600, CareHome.not_care_home, PrimaryServiceType.non_residential, 50.0, 1.25),
    ]
    # fmt: on

    calculate_rate_of_change_trendline_mock_rows = [
        (PrimaryServiceType.care_home_only, 1672531200, 1.0),
        (PrimaryServiceType.care_home_only, 1672617600, 1.5),
        (PrimaryServiceType.care_home_only, 1672704000, 3.0),
        (PrimaryServiceType.care_home_only, 1672790400, 4.5),
        (PrimaryServiceType.non_residential, 1672531200, 1.0),
        (PrimaryServiceType.non_residential, 1672617600, 1.2),
        (PrimaryServiceType.non_residential, 1672704000, 1.2),
        (PrimaryServiceType.non_residential, 1672790400, 1.8),
    ]

    deduplicate_dataframe_rows = [
        (PrimaryServiceType.care_home_only, 1672531200, 1.0, 2.0),
        (PrimaryServiceType.care_home_only, 1672617600, 1.1, 2.0),
        (PrimaryServiceType.care_home_only, 1672704000, 1.2, 2.0),
        (PrimaryServiceType.care_home_only, 1672790400, 1.3, 2.0),
        (PrimaryServiceType.care_home_only, 1672531200, 1.0, 2.0),
        (PrimaryServiceType.care_home_only, 1672617600, 1.1, 2.0),
        (PrimaryServiceType.non_residential, 1672617600, 10.0, 2.0),
        (PrimaryServiceType.non_residential, 1672617600, 10.0, 2.0),
    ]
    expected_deduplicate_dataframe_rows = [
        (PrimaryServiceType.care_home_only, 1672531200, 1.0),
        (PrimaryServiceType.care_home_only, 1672617600, 1.1),
        (PrimaryServiceType.care_home_only, 1672704000, 1.2),
        (PrimaryServiceType.care_home_only, 1672790400, 1.3),
        (PrimaryServiceType.non_residential, 1672617600, 10.0),
    ]

    calculate_rate_of_change_trendline_rows = [
        (PrimaryServiceType.care_home_only, 1672531200, 1.0),
        (PrimaryServiceType.care_home_only, 1672617600, 1.5),
        (PrimaryServiceType.care_home_only, 1672704000, 2.0),
        (PrimaryServiceType.care_home_only, 1672790400, 1.5),
        (PrimaryServiceType.non_residential, 1672531200, 1.0),
        (PrimaryServiceType.non_residential, 1672617600, 1.2),
        (PrimaryServiceType.non_residential, 1672704000, 1.0),
        (PrimaryServiceType.non_residential, 1672790400, 1.5),
    ]
    expected_calculate_rate_of_change_trendline_rows = [
        (PrimaryServiceType.care_home_only, 1672531200, 1.0),
        (PrimaryServiceType.care_home_only, 1672617600, 1.5),
        (PrimaryServiceType.care_home_only, 1672704000, 3.0),
        (PrimaryServiceType.care_home_only, 1672790400, 4.5),
        (PrimaryServiceType.non_residential, 1672531200, 1.0),
        (PrimaryServiceType.non_residential, 1672617600, 1.2),
        (PrimaryServiceType.non_residential, 1672704000, 1.2),
        (PrimaryServiceType.non_residential, 1672790400, 1.8),
    ]


@dataclass
class ModelRollingAverageData:
    rolling_average_rows = [
        ("1-001", 1672531200, 1.1),
        ("1-001", 1672617600, 1.2),
        ("1-001", 1672704000, 1.3),
        ("1-001", 1672790400, 1.4),
        ("1-001", 1672876800, 1.4),
        ("1-001", 1672876800, 1.3),
        ("1-002", 1672531200, 10.0),
        ("1-002", 1672704000, 20.0),
        ("1-002", 1672876800, 30.0),
    ]
    expected_rolling_average_rows = [
        ("1-001", 1672531200, 1.1, 1.1),
        ("1-001", 1672617600, 1.2, 1.15),
        ("1-001", 1672704000, 1.3, 1.2),
        ("1-001", 1672790400, 1.4, 1.3),
        ("1-001", 1672876800, 1.4, 1.35),
        ("1-001", 1672876800, 1.3, 1.35),
        ("1-002", 1672531200, 10.0, 10.0),
        ("1-002", 1672704000, 20.0, 15.0),
        ("1-002", 1672876800, 30.0, 25.0),
    ]


@dataclass
class ModelImputationWithExtrapolationAndInterpolationData:
    imputation_with_extrapolation_and_interpolation_rows = [
        ("1-001", date(2023, 1, 1), 1672531200, CareHome.care_home, 10.0, 15.0),
        ("1-001", date(2023, 2, 1), 1675209600, CareHome.care_home, None, 15.1),
        ("1-001", date(2023, 3, 1), 1677628800, CareHome.care_home, 30.0, 15.2),
        ("1-002", date(2023, 1, 1), 1672531200, CareHome.not_care_home, None, 50.3),
        ("1-002", date(2023, 2, 1), 1675209600, CareHome.not_care_home, 20.0, 50.5),
        ("1-002", date(2023, 3, 1), 1677628800, CareHome.not_care_home, None, 50.7),
        ("1-003", date(2023, 3, 1), 1677628800, CareHome.not_care_home, None, 50.7),
    ]

    split_dataset_rows = [
        ("1-001", date(2024, 1, 1), CareHome.care_home, True),
        ("1-002", date(2024, 1, 1), CareHome.care_home, False),
        ("1-003", date(2024, 1, 1), CareHome.not_care_home, True),
        ("1-004", date(2024, 1, 1), CareHome.not_care_home, False),
    ]
    expected_split_dataset_imputation_df_when_true_rows = [
        ("1-001", date(2024, 1, 1), CareHome.care_home, True),
    ]
    expected_split_dataset_non_imputation_df_when_true_rows = [
        ("1-002", date(2024, 1, 1), CareHome.care_home, False),
        ("1-003", date(2024, 1, 1), CareHome.not_care_home, True),
        ("1-004", date(2024, 1, 1), CareHome.not_care_home, False),
    ]
    expected_split_dataset_imputation_df_when_false_rows = [
        ("1-003", date(2024, 1, 1), CareHome.not_care_home, True),
    ]
    expected_split_dataset_non_imputation_df_when_false_rows = [
        ("1-001", date(2024, 1, 1), CareHome.care_home, True),
        ("1-002", date(2024, 1, 1), CareHome.care_home, False),
        ("1-004", date(2024, 1, 1), CareHome.not_care_home, False),
    ]

    non_null_submission_when_locations_have_a_non_null_value_rows = [
        ("1-001", date(2024, 1, 1), CareHome.care_home, 10.0),
        ("1-002", date(2024, 1, 1), CareHome.care_home, None),
        ("1-002", date(2024, 2, 1), CareHome.care_home, 20.0),
    ]
    expected_non_null_submission_when_locations_have_a_non_null_value_rows = [
        ("1-001", date(2024, 1, 1), CareHome.care_home, 10.0, True),
        ("1-002", date(2024, 1, 1), CareHome.care_home, None, True),
        ("1-002", date(2024, 2, 1), CareHome.care_home, 20.0, True),
    ]
    non_null_submission_when_location_only_has_null_value_rows = [
        ("1-001", date(2024, 1, 1), CareHome.care_home, None),
        ("1-001", date(2024, 2, 1), CareHome.care_home, None),
    ]
    expected_non_null_submission_when_location_only_has_null_value_rows = [
        ("1-001", date(2024, 1, 1), CareHome.care_home, None, False),
        ("1-001", date(2024, 2, 1), CareHome.care_home, None, False),
    ]
    non_null_submission_when_a_location_has_both_care_home_options_rows = [
        ("1-001", date(2024, 1, 1), CareHome.care_home, None),
        ("1-001", date(2024, 2, 1), CareHome.not_care_home, 30.0),
    ]
    expected_non_null_submission_when_a_location_has_both_care_home_options_rows = [
        ("1-001", date(2024, 1, 1), CareHome.care_home, None, False),
        ("1-001", date(2024, 2, 1), CareHome.not_care_home, 30.0, True),
    ]

    column_with_null_values_name: str = "null_values"
    model_column_name: str = "trend_model"
    imputation_model_column_name: str = "imputation_null_values_trend_model"

    imputation_model_rows = [
        ("1-001", None, None, None),
        ("1-002", None, None, 30.0),
        ("1-003", None, None, -2.0),
        ("1-004", None, -2.0, None),
        ("1-005", None, -2.0, 30.0),
        ("1-006", 10.0, None, None),
        ("1-007", 10.0, None, 30.0),
        ("1-008", 10.0, -2.0, None),
        ("1-009", 10.0, -2.0, 30.0),
    ]
    expected_imputation_model_rows = [
        ("1-001", None, None, None, None),
        ("1-002", None, None, 30.0, 30.0),
        ("1-003", None, None, -2.0, 1.0),
        ("1-004", None, -2.0, None, 1.0),
        ("1-005", None, -2.0, 30.0, 1.0),
        ("1-006", 10.0, None, None, 10.0),
        ("1-007", 10.0, None, 30.0, 10.0),
        ("1-008", 10.0, -2.0, None, 10.0),
        ("1-009", 10.0, -2.0, 30.0, 10.0),
    ]


@dataclass
class ModelExtrapolation:
    extrapolation_rows = [
        ("1-001", date(2023, 1, 1), 1672531200, 15.0, 15.0),
        ("1-001", date(2023, 2, 1), 1675209600, None, 15.1),
        ("1-001", date(2023, 3, 1), 1677628800, 30.0, 15.2),
        ("1-002", date(2023, 1, 1), 1672531200, 4.0, 50.3),
        ("1-002", date(2023, 2, 1), 1675209600, None, 50.5),
        ("1-002", date(2023, 3, 1), 1677628800, None, 50.7),
        ("1-002", date(2023, 4, 1), 1680303600, None, 50.1),
        ("1-003", date(2023, 1, 1), 1672531200, None, 50.3),
        ("1-003", date(2023, 2, 1), 1675209600, 20.0, 50.5),
        ("1-003", date(2023, 3, 1), 1677628800, None, 50.7),
        ("1-004", date(2023, 3, 1), 1677628800, None, 50.7),
    ]

    first_and_last_submission_dates_rows = [
        ("1-001", 1672531200, 15.0),
        ("1-001", 1675209600, None),
        ("1-001", 1677628800, 30.0),
        ("1-002", 1672531200, None),
        ("1-002", 1675209600, 4.0),
        ("1-002", 1677628800, None),
        ("1-003", 1677628800, None),
    ]
    expected_first_and_last_submission_dates_rows = [
        ("1-001", 1672531200, 15.0, 1672531200, 1677628800),
        ("1-001", 1675209600, None, 1672531200, 1677628800),
        ("1-001", 1677628800, 30.0, 1672531200, 1677628800),
        ("1-002", 1672531200, None, 1675209600, 1675209600),
        ("1-002", 1675209600, 4.0, 1675209600, 1675209600),
        ("1-002", 1677628800, None, 1675209600, 1675209600),
        ("1-003", 1677628800, None, None, None),
    ]

    extrapolation_forwards_rows = [
        ("1-001", 1672531200, 15.0, 10.0),
        ("1-001", 1675209600, None, 20.0),
        ("1-001", 1677628800, 30.0, 30.0),
        ("1-002", 1672531200, None, 10.0),
        ("1-002", 1675209600, 10.0, 20.0),
        ("1-002", 1677628800, None, 30.0),
        ("1-002", 1677629000, None, 100.0),
        ("1-003", 1672531200, 20.0, 100.0),
        ("1-003", 1675209600, None, 20.0),
        ("1-004", 1677628800, None, 20.0),
    ]
    # fmt: off
    expected_extrapolation_forwards_rows = [
        ("1-001", 1672531200, 15.0, 10.0, None),
        ("1-001", 1675209600, None, 20.0, 30.0),
        ("1-001", 1677628800, 30.0, 30.0, 45.0),
        ("1-002", 1672531200, None, 10.0, None),
        ("1-002", 1675209600, 10.0, 20.0, None),
        ("1-002", 1677628800, None, 30.0, 15.0),
        ("1-002", 1677629000, None, 100.0, 40.0),  # capped at upper cutoff
        ("1-003", 1672531200, 20.0, 100.0, None),
        ("1-003", 1675209600, None, 20.0, 5.0),  # capped at lower cutoff
        ("1-004", 1677628800, None, 20.0, None),
    ]
    # fmt: on
    extrapolation_forwards_mock_rows = [
        ("1-001", 12345, 15.0, 10.0, 15.0, 10.0),
    ]

    extrapolation_backwards_rows = [
        ("1-001", 1672531200, 15.0, 1672531200, 1677628800, 10.0),
        ("1-001", 1675209600, None, 1672531200, 1677628800, 20.0),
        ("1-001", 1677628800, 30.0, 1672531200, 1677628800, 30.0),
        ("1-002", 1672531200, None, 1675209600, 1675209600, 10.0),
        ("1-002", 1675209600, 10.0, 1675209600, 1675209600, 20.0),
        ("1-002", 1677628800, None, 1675209600, 1675209600, 30.0),
        ("1-003", 1672531200, None, 1675209600, 1675209600, 1.0),
        ("1-003", 1675209600, 20.0, 1675209600, 1675209600, 20.0),
        ("1-004", 1672531200, None, 1675209600, 1675209600, 100.0),
        ("1-004", 1675209600, 20.0, 1675209600, 1675209600, 20.0),
        ("1-005", 1677628800, None, None, None, 20.0),
    ]
    # fmt: off
    expected_extrapolation_backwards_rows = [
        ("1-001", 1672531200, 15.0, 1672531200, 1677628800, 10.0, None),
        ("1-001", 1675209600, None, 1672531200, 1677628800, 20.0, None),
        ("1-001", 1677628800, 30.0, 1672531200, 1677628800, 30.0, None),
        ("1-002", 1672531200, None, 1675209600, 1675209600, 10.0, 5.0),
        ("1-002", 1675209600, 10.0, 1675209600, 1675209600, 20.0, None),
        ("1-002", 1677628800, None, 1675209600, 1675209600, 30.0, None),
        ("1-003", 1672531200, None, 1675209600, 1675209600, 1.0, 5.0),  # capped at lower cutoff
        ("1-003", 1675209600, 20.0, 1675209600, 1675209600, 20.0, None),
        ("1-004", 1672531200, None, 1675209600, 1675209600, 100.0, 80.0),  # capped at upper cutoff
        ("1-004", 1675209600, 20.0, 1675209600, 1675209600, 20.0, None),
        ("1-005", 1677628800, None, None, None, 20.0, None),
    ]
    # fmt: on
    extrapolation_backwards_mock_rows = [
        ("1-001", 12345, 15.0, 12345, 12345, 10.0, 15.0, 10.0),
    ]

    combine_extrapolation_rows = [
        ("1-001", 1672531200, 15.0, 1672531200, 1677628800, 15.0, 15.0),
        ("1-001", 1675209600, None, 1672531200, 1677628800, 20.0, 25.0),
        ("1-001", 1677628800, 30.0, 1672531200, 1677628800, 30.0, 30.0),
        ("1-002", 1672531200, None, 1675209600, 1675209600, 3.0, 2.0),
        ("1-002", 1675209600, 4.0, 1675209600, 1675209600, 4.0, 4.0),
        ("1-002", 1677628800, None, 1675209600, 1675209600, 5.0, 6.0),
        ("1-003", 1677628800, None, None, None, None, None),
    ]
    expected_combine_extrapolation_rows = [
        ("1-001", 1672531200, 15.0, 1672531200, 1677628800, 15.0, 15.0, None),
        ("1-001", 1675209600, None, 1672531200, 1677628800, 20.0, 25.0, None),
        ("1-001", 1677628800, 30.0, 1672531200, 1677628800, 30.0, 30.0, None),
        ("1-002", 1672531200, None, 1675209600, 1675209600, 3.0, 2.0, 2.0),
        ("1-002", 1675209600, 4.0, 1675209600, 1675209600, 4.0, 4.0, None),
        ("1-002", 1677628800, None, 1675209600, 1675209600, 5.0, 6.0, 5.0),
        ("1-003", 1677628800, None, None, None, None, None, None),
    ]


@dataclass
class ModelInterpolation:
    interpolation_rows = [
        ("1-001", date(2023, 1, 1), 1672531200, None, None),
        ("1-001", date(2023, 2, 1), 1675209600, None, None),
        ("1-001", date(2023, 3, 1), 1677628800, 40.0, None),
        ("1-001", date(2023, 4, 1), 1680307200, None, 42.0),
        ("1-001", date(2023, 5, 1), 1682899200, None, 44.0),
        ("1-001", date(2023, 6, 1), 1685577600, None, 46.0),
        ("1-001", date(2023, 7, 1), 1688169600, None, 48.0),
        ("1-001", date(2023, 8, 1), 1690848000, None, 50.0),
        ("1-001", date(2023, 9, 1), 1693526400, None, 52.0),
        ("1-001", date(2023, 10, 1), 1696118400, None, 54.0),
        ("1-001", date(2023, 11, 1), 1698796800, None, 56.0),
        ("1-001", date(2023, 12, 1), 1701388800, None, 58.0),
        ("1-001", date(2024, 1, 1), 1704067200, None, 56.0),
        ("1-001", date(2024, 2, 1), 1706745600, None, 54.0),
        ("1-001", date(2024, 3, 1), 1709251200, 5.0, 52.0),
        ("1-001", date(2024, 4, 1), 1711929600, None, 5.31),
        ("1-001", date(2024, 5, 1), 1714521600, 15.0, 5.38),
        ("1-001", date(2024, 6, 1), 1717200000, None, 13.93),
    ]

    calculate_residual_returns_none_when_extrapolation_forwards_is_none_rows = [
        ("1-001", date(2023, 1, 1), 1672531200, None, None),
        ("1-001", date(2023, 3, 1), 1677628800, 40.0, None),
    ]
    expected_calculate_residual_returns_none_when_extrapolation_forwards_is_none_rows = [
        ("1-001", date(2023, 1, 1), 1672531200, None, None, None),
        ("1-001", date(2023, 3, 1), 1677628800, 40.0, None, None),
    ]
    calculate_residual_returns_expected_values_when_extrapolation_forwards_is_known_rows = [
        ("1-001", date(2023, 4, 1), 1680307200, None, 42.0),
        ("1-001", date(2023, 5, 1), 1682899200, None, 44.0),
        ("1-001", date(2024, 3, 1), 1709251200, 5.0, 52.0),
        ("1-001", date(2024, 4, 1), 1711929600, None, 5.1),
        ("1-001", date(2024, 5, 1), 1714521600, 15.0, 5.2),
    ]
    expected_calculate_residual_returns_expected_values_when_extrapolation_forwards_is_known_rows = [
        ("1-001", date(2023, 4, 1), 1680307200, None, 42.0, -47.0),
        ("1-001", date(2023, 5, 1), 1682899200, None, 44.0, -47.0),
        ("1-001", date(2024, 3, 1), 1709251200, 5.0, 52.0, -47.0),
        ("1-001", date(2024, 4, 1), 1711929600, None, 5.1, 9.8),
        ("1-001", date(2024, 5, 1), 1714521600, 15.0, 5.2, 9.8),
    ]
    calculate_residual_returns_none_date_after_final_non_null_submission_rows = [
        ("1-001", date(2024, 5, 1), 1714521600, 15.0, 5.2),
        ("1-001", date(2024, 6, 1), 1717200000, None, 15.3),
    ]
    expected_calculate_residual_returns_none_date_after_final_non_null_submission_rows = [
        ("1-001", date(2024, 5, 1), 1714521600, 15.0, 5.2, 9.8),
        ("1-001", date(2024, 6, 1), 1717200000, None, 15.3, None),
    ]

    time_between_submissions_rows = [
        ("1-001", date(2024, 2, 1), 1000000200, None),
        ("1-001", date(2024, 3, 1), 1000000300, 5.0),
        ("1-001", date(2024, 4, 1), 1000000400, None),
        ("1-001", date(2024, 5, 1), 1000000500, None),
        ("1-001", date(2024, 6, 1), 1000000600, None),
        ("1-001", date(2024, 7, 1), 1000000700, 15.0),
        ("1-001", date(2023, 8, 1), 1000000800, None),
    ]
    expected_time_between_submissions_rows = [
        ("1-001", date(2024, 2, 1), 1000000200, None, None, None),
        ("1-001", date(2024, 3, 1), 1000000300, 5.0, None, None),
        ("1-001", date(2024, 4, 1), 1000000400, None, 400, 0.25),
        ("1-001", date(2024, 5, 1), 1000000500, None, 400, 0.5),
        ("1-001", date(2024, 6, 1), 1000000600, None, 400, 0.75),
        ("1-001", date(2024, 7, 1), 1000000700, 15.0, None, None),
        ("1-001", date(2023, 8, 1), 1000000800, None, None, None),
    ]
    time_between_submissions_mock_rows = [
        ("1-001", date(2024, 2, 1), 12345, None, 12345, 12345),
    ]

    calculate_interpolated_values_rows = [
        ("1-001", 172800, 20.0, None, None, None, None),
        ("1-001", 259200, None, 20.0, 10.0, 345600, 0.25),
        ("1-001", 345600, None, 20.0, 10.0, 345600, 0.5),
        ("1-001", 432000, None, 20.0, 10.0, 345600, 0.75),
        ("1-001", 518400, 30.0, 20.0, 10.0, None, None),
        ("1-001", 604800, None, None, None, None, None),
    ]
    expected_calculate_interpolated_values_when_within_max_days_rows = [
        ("1-001", 172800, 20.0, None, None, None, None, None),
        ("1-001", 259200, None, 20.0, 10.0, 345600, 0.25, 22.5),
        ("1-001", 345600, None, 20.0, 10.0, 345600, 0.5, 25.0),
        ("1-001", 432000, None, 20.0, 10.0, 345600, 0.75, 27.5),
        ("1-001", 518400, 30.0, 20.0, 10.0, None, None, None),
        ("1-001", 604800, None, None, None, None, None, None),
    ]
    expected_calculate_interpolated_values_when_outside_of_max_days_rows = [
        ("1-001", 172800, 20.0, None, None, None, None, None),
        ("1-001", 259200, None, 20.0, 10.0, 345600, 0.25, None),
        ("1-001", 345600, None, 20.0, 10.0, 345600, 0.5, None),
        ("1-001", 432000, None, 20.0, 10.0, 345600, 0.75, None),
        ("1-001", 518400, 30.0, 20.0, 10.0, None, None, None),
        ("1-001", 604800, None, None, None, None, None, None),
    ]


@dataclass
class ModelFeatures:
    vectorise_input_rows = [
        ("1-0001", 12.0, 0, 1, date(2024, 1, 1)),
        ("1-0002", 50.0, 1, 1, date(2024, 1, 1)),
    ]
    expected_vectorised_feature_rows = [
        ("1-0001", Vectors.dense([12.0, 0.0, 1.0])),
        ("1-0002", Vectors.dense([50.0, 1.0, 1.0])),
    ]

    expand_encode_and_extract_features_lookup_dict = {
        "has_A": "A",
        "has_B": "B",
        "has_C": "C",
    }
    expected_expand_encode_and_extract_features_feature_list = [
        "has_A",
        "has_B",
        "has_C",
    ]

    expand_encode_and_extract_features_when_not_array_rows = [
        ("1-0001", "A"),
        ("1-0002", "C"),
        ("1-0003", "B"),
        ("1-0004", "D"),
        ("1-0005", None),
    ]
    expected_expand_encode_and_extract_features_when_not_array_rows = [
        ("1-0001", "A", 1, 0, 0),
        ("1-0002", "C", 0, 0, 1),
        ("1-0003", "B", 0, 1, 0),
        ("1-0004", "D", 0, 0, 0),
        ("1-0005", None, None, None, None),
    ]

    expand_encode_and_extract_features_when_is_array_rows = [
        ("1-0001", ["A", "B"]),
        ("1-0002", ["B"]),
        ("1-0003", ["C", "A"]),
        ("1-0004", ["B", "D"]),
        ("1-0005", None),
    ]
    expected_expand_encode_and_extract_features_when_is_array_rows = [
        ("1-0001", ["A", "B"], 1, 1, 0),
        ("1-0002", ["B"], 0, 1, 0),
        ("1-0003", ["C", "A"], 1, 0, 1),
        ("1-0004", ["B", "D"], 0, 1, 0),
        ("1-0005", None, None, None, None),
    ]

    cap_integer_at_max_value_rows = [
        ("1-0001", 1),
        ("1-0002", 2),
        ("1-0003", 3),
        ("1-0004", None),
    ]
    expected_cap_integer_at_max_value_rows = [
        ("1-0001", 1, 1),
        ("1-0002", 2, 2),
        ("1-0003", 3, 2),
        ("1-0004", None, None),
    ]

    add_array_column_count_with_one_element_rows = [
        ("1-001", [{CQCL.name: "name", CQCL.description: "description"}]),
    ]
    expected_add_array_column_count_with_one_element_rows = [
        ("1-001", [{CQCL.name: "name", CQCL.description: "description"}], 1),
    ]

    add_array_column_count_with_multiple_elements_rows = [
        (
            "1-001",
            [
                {CQCL.name: "name_1", CQCL.description: "description_1"},
                {CQCL.name: "name_2", CQCL.description: "description_2"},
                {CQCL.name: "name_3", CQCL.description: "description_3"},
            ],
        ),
    ]
    expected_add_array_column_count_with_multiple_elements_rows = [
        (
            "1-001",
            [
                {CQCL.name: "name_1", CQCL.description: "description_1"},
                {CQCL.name: "name_2", CQCL.description: "description_2"},
                {CQCL.name: "name_3", CQCL.description: "description_3"},
            ],
            3,
        ),
    ]

    add_array_column_count_with_empty_array_rows = [
        ("1-001", []),
    ]
    expected_add_array_column_count_with_empty_array_rows = [
        ("1-001", [], 0),
    ]

    add_array_column_count_with_null_value_rows = [
        ("1-001", None),
    ]
    expected_add_array_column_count_with_null_value_rows = [
        ("1-001", None, 0),
    ]

    add_date_index_column_rows = [
        ("1-0001", CareHome.not_care_home, date(2024, 10, 1)),
        ("1-0002", CareHome.not_care_home, date(2024, 12, 1)),
        ("1-0003", CareHome.not_care_home, date(2024, 12, 1)),
        ("1-0004", CareHome.not_care_home, date(2025, 2, 1)),
        ("1-0005", CareHome.care_home, date(2025, 2, 1)),
    ]
    expected_add_date_index_column_rows = [
        ("1-0001", CareHome.not_care_home, date(2024, 10, 1), 1),
        ("1-0002", CareHome.not_care_home, date(2024, 12, 1), 2),
        ("1-0003", CareHome.not_care_home, date(2024, 12, 1), 2),
        ("1-0004", CareHome.not_care_home, date(2025, 2, 1), 3),
        ("1-0005", CareHome.care_home, date(2025, 2, 1), 1),
    ]

    group_rural_urban_sparse_categories_rows = [
        ("1-001", "Rural"),
        ("1-002", "Rural sparse"),
        ("1-003", "Another with sparse in it"),
        ("1-004", "Urban"),
        ("1-005", "Sparse with a capital S"),
    ]
    expected_group_rural_urban_sparse_categories_rows = [
        ("1-001", "Rural", "Rural"),
        ("1-002", "Rural sparse", "Sparse setting"),
        ("1-003", "Another with sparse in it", "Sparse setting"),
        ("1-004", "Urban", "Urban"),
        ("1-005", "Sparse with a capital S", "Sparse setting"),
    ]

    filter_without_dormancy_features_to_pre_2025_rows = [
        ("1-001", date(2024, 12, 31)),
        ("1-002", date(2025, 1, 1)),
        ("1-003", date(2025, 1, 2)),
    ]
    expected_filter_without_dormancy_features_to_pre_2025_rows = [
        ("1-001", date(2024, 12, 31)),
        ("1-002", date(2025, 1, 1)),
    ]


@dataclass
class ModelCareHomes:
    care_homes_cleaned_ind_cqc_rows = [
        (
            "1-000000001",
            "Care home with nursing",
            None,
            None,
            "Y",
            "South West",
            67,
            date(2022, 3, 29),
        ),
        (
            "1-000000002",
            "Care home without nursing",
            None,
            None,
            "N",
            "Merseyside",
            12,
            date(2022, 3, 29),
        ),
        (
            "1-000000003",
            "Care home with nursing",
            None,
            None,
            None,
            "Merseyside",
            34,
            date(2022, 3, 29),
        ),
        (
            "1-000000004",
            "non-residential",
            10.0,
            "already_populated",
            "N",
            None,
            0,
            date(2022, 3, 29),
        ),
        ("1-000000001", "non-residential", None, None, "N", None, 0, date(2022, 2, 20)),
    ]
    care_homes_features_rows = [
        (
            "1-000000001",
            date(2022, 3, 29),
            10,
            62.0,
            Vectors.sparse(39, {0: 1.0, 1: 1.2, 2: 1.0, 3: 50.0}),
        ),
        (
            "1-000000003",
            date(2022, 3, 29),
            15,
            45.0,
            None,
        ),
    ]


@dataclass
class ModelNonResWithDormancy:
    non_res_with_dormancy_cleaned_ind_cqc_rows = [
        (
            "1-000000001",
            PrimaryServiceType.non_residential,
            None,
            None,
            "Y",
            "South West",
            date(2022, 3, 29),
        ),
        (
            "1-000000002",
            PrimaryServiceType.non_residential,
            None,
            None,
            "N",
            "Merseyside",
            date(2022, 3, 29),
        ),
        (
            "1-000000003",
            PrimaryServiceType.non_residential,
            None,
            None,
            None,
            "Merseyside",
            date(2022, 3, 29),
        ),
    ]
    non_res_with_dormancy_features_rows = [
        (
            "1-000000001",
            date(2022, 3, 29),
            10.0,
            Vectors.sparse(
                32,
                {
                    0: 1.0,
                    1: 1.0,
                    4: 17.5,
                    10: 1.0,
                    18: 1.0,
                    31: 35.0,
                },
            ),
        ),
        (
            "1-000000003",
            date(2022, 3, 29),
            20.0,
            None,
        ),
    ]


@dataclass
class ModelNonResWithoutDormancy:
    non_res_without_dormancy_cleaned_ind_cqc_rows = [
        (
            "1-000000001",
            PrimaryServiceType.non_residential,
            None,
            None,
            "Y",
            "South West",
            date(2022, 3, 29),
        ),
        (
            "1-000000002",
            PrimaryServiceType.non_residential,
            None,
            None,
            "N",
            "Merseyside",
            date(2022, 3, 29),
        ),
        (
            "1-000000003",
            PrimaryServiceType.non_residential,
            None,
            None,
            None,
            "Merseyside",
            date(2022, 3, 29),
        ),
    ]
    non_res_without_dormancy_features_rows = [
        (
            "1-000000001",
            date(2022, 3, 29),
            10.0,
            Vectors.sparse(
                31,
                {
                    0: 1.0,
                    1: 1.0,
                    3: 17.5,
                    9: 1.0,
                    17: 1.0,
                    30: 35.0,
                },
            ),
        ),
        (
            "1-000000003",
            date(2022, 3, 29),
            20.0,
            None,
        ),
    ]


@dataclass
class ModelNonResWithAndWithoutDormancyCombinedRows:
    # fmt: off
    estimated_posts_rows = [
        ("1-001", "Y", date(2021, 1, 1), CareHome.not_care_home, "Y", 1, 1.0, None, False),
        ("1-001", "Y", date(2022, 2, 1), CareHome.not_care_home, "Y", 2, 3.0, None, False),
        ("1-001", "Y", date(2023, 3, 1), CareHome.not_care_home, "Y", 3, 4.0, 5.0, False),
        ("1-001", "Y", date(2024, 4, 1), CareHome.not_care_home, "Y", 4, 5.0, 5.5, False),
        ("1-001", "Y", date(2025, 5, 1), CareHome.not_care_home, "Y", 5, 6.0, 6.0, False),
        ("1-001", "Y", date(2025, 6, 1), CareHome.not_care_home, "Y", 6, 7.0, 6.5, False),
        ("1-002", "Y", date(2021, 1, 1), CareHome.not_care_home, "Y", 3, 8.0, None, False),
        ("1-002", "Y", date(2022, 2, 1), CareHome.not_care_home, "Y", 4, 8.0, None, False),
        ("1-002", "Y", date(2023, 3, 1), CareHome.not_care_home, "Y", 5, 8.0, 4.0, False),
        ("1-002", "Y", date(2024, 4, 1), CareHome.not_care_home, "Y", 6, 8.0, 4.5, False),
        ("1-002", "Y", date(2025, 5, 1), CareHome.not_care_home, "Y", 7, 8.0, 5.0, False),
        ("1-002", "Y", date(2025, 6, 1), CareHome.not_care_home, "Y", 8, 8.0, 5.5, False),
        ("1-003", "Y", date(2021, 1, 1), CareHome.not_care_home, "N", 1, 2.0, None, False),
        ("1-003", "Y", date(2022, 2, 1), CareHome.not_care_home, "N", 2, 2.0, None, False),
        ("1-003", "Y", date(2021, 3, 1), CareHome.not_care_home, "N", 3, 4.0, None, False),
        ("1-003", "Y", date(2022, 4, 1), CareHome.not_care_home, "N", 4, 4.0, None, False),
        ("1-003", "Y", date(2023, 5, 1), CareHome.not_care_home, "N", 5, 6.0, 8.0, False),
        ("1-003", "Y", date(2024, 6, 1), CareHome.not_care_home, "N", 6, 6.0, 9.0, False),
        ("1-004", "Y", date(2024, 4, 1), CareHome.care_home, "Y", 1, None, None, False),
        ("1-005", "Y", date(2024, 5, 1), CareHome.not_care_home, "Y", 1, 4.0, 2.0, False),
        ("1-005", "Y", date(2024, 6, 1), CareHome.not_care_home, "Y", 2, 5.0, 2.5, False),
        ("1-006", "Y", date(2024, 5, 1), CareHome.not_care_home, "N", 1, 3.0, 2.5, False),
        ("1-006", "Y", date(2024, 6, 1), CareHome.not_care_home, "N", 2, 3.0, 3.0, False),
        ("1-006", "Y", date(2024, 7, 1), CareHome.not_care_home, "N", 3, 3.0, 3.0, False),
        ("1-006", "Y", date(2024, 8, 1), CareHome.not_care_home, "N", 4, 3.0, 3.0, False),
    ]
    # fmt: on

    group_time_registered_to_six_month_bands_rows = [
        ("1-001", 6),
        ("1-002", 7),
        ("1-003", 200),
    ]
    expected_group_time_registered_to_six_month_bands_rows = [
        ("1-001", 6, 0),
        ("1-002", 7, 1),
        ("1-003", 200, 20),
    ]

    calculate_and_apply_model_ratios_rows = [
        ("1-001", date(2022, 2, 1), "Y", 2, 3.0, None),
        ("1-001", date(2023, 3, 1), "Y", 3, 4.0, 5.0),
        ("1-002", date(2022, 2, 1), "Y", 4, 8.0, None),
        ("1-002", date(2023, 3, 1), "Y", 5, 8.0, 4.0),
        ("1-003", date(2022, 2, 1), "N", 2, 2.0, None),
        ("1-003", date(2021, 3, 1), "N", 3, 4.0, None),
        ("1-003", date(2022, 4, 1), "N", 4, 4.0, None),
        ("1-003", date(2023, 5, 1), "N", 5, 6.0, 8.0),
        ("1-003", date(2024, 6, 1), "N", 6, 6.0, 9.0),
        ("1-004", date(2024, 5, 1), "Y", 1, 4.0, 2.0),
        ("1-004", date(2024, 6, 1), "Y", 2, 5.0, 2.5),
    ]

    average_models_by_related_location_and_time_registered_rows = [
        ("1-001", RelatedLocation.no_related_location, 1, 5.0, 14.0),
        ("1-002", RelatedLocation.no_related_location, 1, 6.0, 15.0),
        ("1-003", RelatedLocation.has_related_location, 1, 1.0, 10.0),
        ("1-004", RelatedLocation.has_related_location, 1, 2.0, 11.0),
        ("1-005", RelatedLocation.has_related_location, 2, 3.0, 12.0),
        ("1-006", RelatedLocation.has_related_location, 2, 4.0, 13.0),
        ("1-007", RelatedLocation.has_related_location, 2, 20.0, None),
        ("1-008", RelatedLocation.has_related_location, 2, None, 20.0),
    ]
    expected_average_models_by_related_location_and_time_registered_rows = [
        (RelatedLocation.no_related_location, 1, 5.5, 14.5),
        (RelatedLocation.has_related_location, 1, 1.5, 10.5),
        (RelatedLocation.has_related_location, 2, 3.5, 12.5),
    ]

    calculate_adjustment_ratios_rows = [
        (RelatedLocation.no_related_location, 1, 5.0, 10.0),
        (RelatedLocation.has_related_location, 1, 4.5, 1.5),
    ]
    expected_calculate_adjustment_ratios_rows = [
        (RelatedLocation.no_related_location, 1, 5.0, 10.0, 0.5),
        (RelatedLocation.has_related_location, 1, 4.5, 1.5, 3.0),
    ]

    calculate_adjustment_ratios_when_without_dormancy_is_zero_or_null_returns_one_rows = [
        (RelatedLocation.no_related_location, 1, 5.0, 0.0),
        (RelatedLocation.has_related_location, 1, 4.5, None),
    ]
    expected_calculate_adjustment_ratios_when_without_dormancy_is_zero_or_null_returns_one_rows = [
        (RelatedLocation.no_related_location, 1, 5.0, 0.0, 1.0),
        (RelatedLocation.has_related_location, 1, 4.5, None, 1.0),
    ]

    apply_model_ratios_returns_expected_values_when_all_values_known_rows = [
        ("1-001", 5.0, 14.0, 0.25),
        ("1-002", 6.0, 15.0, 2.0),
    ]
    expected_apply_model_ratios_returns_expected_values_when_all_values_known_rows = [
        ("1-001", 5.0, 14.0, 0.25, 3.5),
        ("1-002", 6.0, 15.0, 2.0, 30.0),
    ]

    apply_model_ratios_returns_none_when_none_values_present_rows = [
        ("1-001", 5.0, None, 0.2),
        ("1-002", 5.0, 10.0, None),
        ("1-003", 5.0, None, None),
    ]
    expected_apply_model_ratios_returns_none_when_none_values_present_rows = [
        ("1-001", 5.0, None, 0.2, None),
        ("1-002", 5.0, 10.0, None, None),
        ("1-003", 5.0, None, None, None),
    ]

    # fmt: off
    calculate_and_apply_residuals_rows = [
        ("1-001", date(2025, 2, 1), 20.0, 15.0),  # dates match, both models not null, residual calculated and applied
        ("1-002", date(2025, 1, 1), None, 16.0),  # "1-002" - with_dormancy is null, residual added from date(2025, 2, 1) but not applied
        ("1-002", date(2025, 2, 1), 10.0, 15.0),  # "1-002" - first period with both models present, take the residual
        ("1-002", date(2025, 3, 1), 11.0, 14.0),  # "1-002" - residual added from date(2025, 2, 1)
        ("1-002", date(2025, 4, 1), 12.0, None),  # "1-002" - without_dormancy is null, residual added from date(2025, 2, 1) but not applied
        ("1-003", date(2025, 2, 1), 30.0, None),  # doesn't pass filter, no residual, keep original model value
        ("1-004", date(2025, 2, 1), None, 15.0),  # doesn't pass filter, no residual, keep original model value
        ("1-005", date(2025, 2, 1), None, None),  # doesn't pass filter, no residual, keep original model value
    ]
    expected_calculate_and_apply_residuals_rows = [
        ("1-001", date(2025, 2, 1), 20.0, 15.0, 5.0, 20.0),  # dates match, both models not null, residual calculated
        ("1-002", date(2025, 1, 1), None, 16.0, -5.0, 11.0),  # "1-002" - with_dormancy is null, residual added from date(2025, 2, 1) but not applied
        ("1-002", date(2025, 2, 1), 10.0, 15.0, -5.0, 10.0),  # "1-002" - first period with both models present, take the residual
        ("1-002", date(2025, 3, 1), 11.0, 14.0, -5.0, 9.0),  # "1-002" - residual added from date(2025, 2, 1)
        ("1-002", date(2025, 4, 1), 12.0, None, -5.0, None),  # "1-002" - without_dormancy is null, residual added from date(2025, 2, 1) but not applied
        ("1-003", date(2025, 2, 1), 30.0, None, None, None),  # doesn't pass filter, no residual, keep original model value
        ("1-004", date(2025, 2, 1), None, 15.0, None, 15.0),  # doesn't pass filter, no residual, keep original model value
        ("1-005", date(2025, 2, 1), None, None, None, None),  # doesn't pass filter, no residual, keep original model value
    ]
    # fmt: on

    # fmt: off
    calculate_residuals_rows = [
        ("1-001", date(2025, 1, 1), date(2025, 2, 1), 10.0, 15.0),  # filtered out, dates not equal
        ("1-002", date(2025, 2, 1), date(2025, 2, 1), 10.0, 15.0),  # not filtered, negative residual
        ("1-003", date(2025, 2, 1), date(2025, 2, 1), 20.0, 15.0),  # not filtered, positive residual
        ("1-004", date(2025, 2, 1), date(2025, 2, 1), 30.0, None),  # filtered out, null model value
        ("1-005", date(2025, 2, 1), date(2025, 2, 1), None, 15.0),  # filtered out, null model value
        ("1-006", date(2025, 2, 1), date(2025, 2, 1), None, None),  # filtered out, null model value
    ]
    expected_calculate_residuals_rows = [
        ("1-002", -5.0),  # not filtered, negative residual
        ("1-003", 5.0),  # not filtered, positive residual
    ]
    # fmt: on

    apply_residuals_rows = [
        ("1-001", 7.0, 12.0),
        ("1-002", 5.0, -0.5),
        ("1-003", 1.0, -2.5),
        ("1-004", 10.0, None),
        ("1-005", None, -1.0),
        ("1-006", None, None),
    ]
    expected_apply_residuals_rows = [
        ("1-001", 7.0, 12.0, 19.0),
        ("1-002", 5.0, -0.5, 4.5),
        ("1-003", 1.0, -2.5, -1.5),
        ("1-004", 10.0, None, 10.0),
        ("1-005", None, -1.0, None),
        ("1-006", None, None, None),
    ]

    combine_model_predictions_rows = [
        ("1-001", 10.0, 15.0),
        ("1-002", 11.0, None),
        ("1-003", None, 16.0),
        ("1-004", None, None),
    ]
    expected_combine_model_predictions_rows = [
        ("1-001", 10.0, 15.0, 10.0),
        ("1-002", 11.0, None, 11.0),
        ("1-003", None, 16.0, 16.0),
        ("1-004", None, None, None),
    ]


@dataclass
class EstimateFilledPostsModelsUtils:
    cleaned_cqc_rows = ModelCareHomes.care_homes_cleaned_ind_cqc_rows

    predictions_rows = [
        (
            "1-000000001",
            "Care home with nursing",
            50.0,
            "Y",
            "South West",
            67,
            date(2022, 3, 29),
            56.89,
        ),
    ]

    set_min_value_when_below_minimum_rows = [
        ("1-001", 0.5, -7.5),
    ]
    expected_set_min_value_when_below_min_value_rows = [
        ("1-001", 0.5, 2.0),
    ]
    expected_set_min_value_when_below_minimum_and_default_not_set_rows = [
        ("1-001", 0.5, 1.0),
    ]
    expected_set_min_value_when_below_minimum_and_min_value_is_negative_rows = [
        ("1-001", 0.5, -5.0),
    ]

    set_min_value_when_above_minimum_rows = [
        ("1-001", 1.5, 1.5),
    ]

    set_min_value_when_null_rows = [
        ("1-001", None, None),
    ]

    combine_care_home_ratios_and_non_res_posts_rows = [
        ("1-001", CareHome.care_home, 20.0, 1.6),
        ("1-002", CareHome.care_home, 10.0, None),
        ("1-003", CareHome.care_home, None, 1.8),
        ("1-004", CareHome.care_home, None, None),
        ("1-005", CareHome.not_care_home, 20.0, 1.6),
        ("1-006", CareHome.not_care_home, 10.0, None),
        ("1-007", CareHome.not_care_home, None, 1.6),
        ("1-008", CareHome.not_care_home, None, None),
    ]
    expected_combine_care_home_ratios_and_non_res_posts_rows = [
        ("1-001", CareHome.care_home, 20.0, 1.6, 1.6),
        ("1-002", CareHome.care_home, 10.0, None, None),
        ("1-003", CareHome.care_home, None, 1.8, 1.8),
        ("1-004", CareHome.care_home, None, None, None),
        ("1-005", CareHome.not_care_home, 20.0, 1.6, 20.0),
        ("1-006", CareHome.not_care_home, 10.0, None, 10.0),
        ("1-007", CareHome.not_care_home, None, 1.8, None),
        ("1-008", CareHome.not_care_home, None, None, None),
    ]

    clean_number_of_beds_banded_rows = [
        ("1-001", PrimaryServiceType.care_home_only, 1.0),
        ("1-002", PrimaryServiceType.care_home_only, 2.0),
        ("1-003", PrimaryServiceType.care_home_only, 3.0),
        ("1-004", PrimaryServiceType.care_home_only, 4.0),
        ("1-005", PrimaryServiceType.care_home_only, None),
        ("1-006", PrimaryServiceType.care_home_with_nursing, 1.0),
        ("1-007", PrimaryServiceType.care_home_with_nursing, 2.0),
        ("1-008", PrimaryServiceType.care_home_with_nursing, 3.0),
        ("1-009", PrimaryServiceType.care_home_with_nursing, 4.0),
        ("1-010", PrimaryServiceType.care_home_with_nursing, None),
        ("1-011", PrimaryServiceType.non_residential, 1.0),
        ("1-012", PrimaryServiceType.non_residential, None),
    ]
    expected_clean_number_of_beds_banded_rows = [
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 2.0),
        ("1-002", PrimaryServiceType.care_home_only, 2.0, 2.0),
        ("1-003", PrimaryServiceType.care_home_only, 3.0, 3.0),
        ("1-004", PrimaryServiceType.care_home_only, 4.0, 4.0),
        ("1-005", PrimaryServiceType.care_home_only, None, None),
        ("1-006", PrimaryServiceType.care_home_with_nursing, 1.0, 3.0),
        ("1-007", PrimaryServiceType.care_home_with_nursing, 2.0, 3.0),
        ("1-008", PrimaryServiceType.care_home_with_nursing, 3.0, 3.0),
        ("1-009", PrimaryServiceType.care_home_with_nursing, 4.0, 4.0),
        ("1-010", PrimaryServiceType.care_home_with_nursing, None, None),
        ("1-011", PrimaryServiceType.non_residential, 1.0, 1.0),
        ("1-012", PrimaryServiceType.non_residential, None, None),
    ]

    convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_rows = [
        ("1-001", CareHome.care_home, 5, 1.6, 20.0),
        ("1-002", CareHome.care_home, 5, None, 10.0),
        ("1-003", CareHome.care_home, None, 1.6, 20.0),
        ("1-004", CareHome.care_home, None, None, 10.0),
        ("1-005", CareHome.care_home, 5, 1.8, None),
        ("1-006", CareHome.care_home, 5, None, None),
        ("1-007", CareHome.care_home, None, 1.8, None),
        ("1-008", CareHome.care_home, None, None, None),
        ("1-009", CareHome.not_care_home, 5, 1.6, 20.0),
        ("1-010", CareHome.not_care_home, None, None, 10.0),
        ("1-011", CareHome.not_care_home, 5, 1.6, None),
        ("1-012", CareHome.not_care_home, None, None, None),
    ]
    expected_convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_rows = [
        ("1-001", CareHome.care_home, 5, 1.6, 8.0),
        ("1-002", CareHome.care_home, 5, None, None),
        ("1-003", CareHome.care_home, None, 1.6, None),
        ("1-004", CareHome.care_home, None, None, None),
        ("1-005", CareHome.care_home, 5, 1.8, 9.0),
        ("1-006", CareHome.care_home, 5, None, None),
        ("1-007", CareHome.care_home, None, 1.8, None),
        ("1-008", CareHome.care_home, None, None, None),
        ("1-009", CareHome.not_care_home, 5, 1.6, 20.0),
        ("1-010", CareHome.not_care_home, None, None, 10.0),
        ("1-011", CareHome.not_care_home, 5, 1.6, None),
        ("1-012", CareHome.not_care_home, None, None, None),
    ]

    create_test_and_train_datasets_rows = [
        ("1-001", Vectors.dense([10.0, 0.0, 1.0])),
        ("1-002", Vectors.dense([20.0, 1.0, 1.0])),
        ("1-003", Vectors.dense([30.0, 0.0, 1.0])),
        ("1-004", Vectors.dense([40.0, 0.0, 1.0])),
        ("1-005", Vectors.dense([50.0, 1.0, 1.0])),
    ]

    train_lasso_regression_model_rows = [
        (Vectors.dense([1.0, 2.0]), 5.0),
        (Vectors.dense([2.0, 1.0]), 4.0),
    ]


@dataclass
class MLModelMetrics:
    ind_cqc_with_predictions_rows = [
        ("1-00001", "care home", 50.0, "Y", "South West", 67, date(2022, 3, 9), 56.89),
        ("1-00002", "non-res", 10.0, "N", "North East", 0, date(2022, 3, 9), 12.34),
    ]

    r2_metric_rows = [
        ("1-00001", 50.0, 56.89),
        ("1-00002", 10.0, 12.34),
    ]

    predictions_rows = [
        ("1-00001", 50.0, 56.89),
        ("1-00002", None, 46.80),
        ("1-00003", 10.0, 12.34),
    ]
    expected_predictions_with_dependent_rows = [
        ("1-00001", 50.0, 56.89),
        ("1-00003", 10.0, 12.34),
    ]


@dataclass
class ValidateMergedIndCqcData:
    # fmt: off
    cqc_locations_rows = [
        (date(2024, 1, 1), "1-000000001", "Independent", "Y", 10,),
        (date(2024, 1, 1), "1-000000002", "Independent", "N", None,),
        (date(2024, 2, 1), "1-000000001", "Independent", "Y", 10,),
        (date(2024, 2, 1), "1-000000002", "Independent", "N", None,),
    ]

    merged_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5),
    ]
    # fmt: on

    calculate_expected_size_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", Sector.local_authority),
        ("loc_3", None),
    ]


@dataclass
class ValidateMergedCoverageData:
    cqc_locations_rows = [
        (date(2024, 1, 1), "1-001", "Name", "AB1 2CD", "Y", 10, "2024", "01", "01"),
        (
            date(2024, 1, 1),
            "1-002",
            "Name",
            "EF3 4GH",
            "N",
            None,
            "2024",
            "01",
            "01",
        ),
        (date(2024, 2, 1), "1-001", "Name", "AB1 2CD", "Y", 10, "2024", "02", "01"),
        (
            date(2024, 2, 1),
            "1-002",
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
        ("1-001", date(2024, 1, 1), date(2024, 1, 1), "Name", "AB1 2CD", "Y"),
        ("1-002", date(2024, 1, 1), date(2024, 1, 1), "Name", "EF3 4GH", "N"),
        ("1-001", date(2024, 1, 9), date(2024, 1, 1), "Name", "AB1 2CD", "Y"),
        ("1-002", date(2024, 1, 9), date(2024, 1, 1), "Name", "EF3 4GH", "N"),
    ]
    calculate_expected_size_rows = [
        ("loc 1", date(2024, 1, 1), "name", "AB1 2CD", "Y", "2024", "01", "01"),
        ("loc 1", date(2024, 1, 8), "name", "AB1 2CD", "Y", "2024", "01", "08"),
        ("loc 2", date(2024, 1, 1), "name", "AB1 2CD", "Y", "2024", "01", "01"),
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
                    CQCLNew.use_of_resources: {
                        CQCL.organisation_id: None,
                        CQCLNew.summary: None,
                        CQCLNew.use_of_resources_rating: None,
                        CQCLNew.combined_quality_summary: None,
                        CQCLNew.combined_quality_rating: None,
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
                CQCLNew.service_ratings: [
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
                    CQCLNew.service_ratings: [
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
                        CQCLNew.use_of_resources: {
                            CQCLNew.combined_quality_rating: None,
                            CQCLNew.combined_quality_summary: None,
                            CQCLNew.use_of_resources_rating: None,
                            CQCLNew.use_of_resources_summary: None,
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

    add_rating_sequence_rows = [
        ("loc_1", "2024-01-01"),
        ("loc_1", "2024-01-02"),
        ("loc_2", "2024-01-01"),
        ("loc_2", "2024-02-01"),
        ("loc_3", "2023-01-01"),
        ("loc_3", "2024-01-01"),
    ]
    expected_add_rating_sequence_rows = [
        ("loc_1", "2024-01-02", 2),
        ("loc_2", "2024-01-01", 1),
        ("loc_2", "2024-02-01", 2),
        ("loc_1", "2024-01-01", 1),
        ("loc_3", "2023-01-01", 1),
        ("loc_3", "2024-01-01", 2),
    ]
    expected_reversed_add_rating_sequence_rows = [
        ("loc_1", "2024-01-02", 1),
        ("loc_2", "2024-01-01", 2),
        ("loc_2", "2024-02-01", 1),
        ("loc_1", "2024-01-01", 2),
        ("loc_3", "2023-01-01", 2),
        ("loc_3", "2024-01-01", 1),
    ]

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
            "2024-01-01",
            "Current",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
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
            "2024-01-01",
            "Historic",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            "Good",
            1,
            1,
            3,
            3,
            3,
            3,
            3,
            15,
        ),
    ]
    select_ratings_for_benchmarks_rows = [
        ("loc_1", RegistrationStatus.registered, CQCCurrentOrHistoricValues.current),
        ("loc_2", RegistrationStatus.registered, CQCCurrentOrHistoricValues.historic),
        ("loc_3", RegistrationStatus.deregistered, CQCCurrentOrHistoricValues.current),
        ("loc_4", RegistrationStatus.deregistered, CQCCurrentOrHistoricValues.historic),
    ]
    expected_select_ratings_for_benchmarks_rows = [
        ("loc_1", RegistrationStatus.registered, CQCCurrentOrHistoricValues.current),
    ]

    add_good_or_outstanding_flag_rows = [
        ("loc_1", CQCRatingsValues.outstanding),
        ("loc_2", CQCRatingsValues.good),
        ("loc_3", "other rating"),
        ("loc_1", None),
    ]
    expected_add_good_or_outstanding_flag_rows = [
        ("loc_1", CQCRatingsValues.outstanding, 1),
        ("loc_2", CQCRatingsValues.good, 1),
        ("loc_3", "other rating", 0),
        ("loc_1", None, 0),
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
    create_benchmark_ratings_dataset_rows = [
        ("loc_1", "estab_1", 1, "Good", "2024-01-01", ""),
        ("loc_2", "estab_2", 0, "Requires improvement", "2024-01-01", ""),
        ("loc_3", None, 1, "Good", "2024-01-01", ""),
        ("loc_4", "estab_2", 0, None, "2024-01-01", ""),
        ("loc_5", None, 0, None, "2024-01-01", ""),
    ]
    expected_create_benchmark_ratings_dataset_rows = [
        ("loc_1", "estab_1", 1, "Good", "2024-01-01"),
        ("loc_2", "estab_2", 0, "Requires improvement", "2024-01-01"),
    ]

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
class ValidationUtils:
    size_of_dataset_rule = {RuleName.size_of_dataset: 3}
    size_of_dataset_success_rows = [
        ("loc_1",),
        ("loc_2",),
        ("loc_3",),
    ]
    size_of_dataset_extra_rows = [
        ("loc_1",),
        ("loc_2",),
        ("loc_3",),
        ("loc_4",),
    ]
    size_of_dataset_missing_rows = [
        ("loc_1",),
        ("loc_2",),
    ]
    size_of_dataset_result_success_rows = [
        (
            "Size of dataset",
            "Warning",
            "Success",
            "SizeConstraint(Size(None))",
            "Success",
            "",
        )
    ]
    size_of_dataset_result_missing_rows = [
        (
            "Size of dataset",
            "Warning",
            "Warning",
            "SizeConstraint(Size(None))",
            "Failure",
            "Value: 2 does not meet the constraint requirement! DataFrame row count should be 3.",
        )
    ]
    size_of_dataset_result_extra_rows = [
        (
            "Size of dataset",
            "Warning",
            "Warning",
            "SizeConstraint(Size(None))",
            "Failure",
            "Value: 4 does not meet the constraint requirement! DataFrame row count should be 3.",
        )
    ]

    unique_index_columns_rule = {
        RuleName.index_columns: [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
        ]
    }
    unique_index_columns_success_rows = [
        (
            "loc_1",
            date(2024, 1, 1),
        ),
        (
            "loc_1",
            date(2024, 1, 2),
        ),
        (
            "loc_2",
            date(2024, 1, 1),
        ),
    ]
    unique_index_columns_not_unique_rows = [
        (
            "loc_1",
            date(2024, 1, 1),
        ),
        (
            "loc_1",
            date(2024, 1, 1),
        ),
    ]
    unique_index_columns_result_success_rows = [
        (
            "Index columns are unique",
            "Warning",
            "Success",
            "UniquenessConstraint(Uniqueness(Stream(locationId, ?),None,None))",
            "Success",
            "",
        ),
    ]
    unique_index_columns_result_not_unique_rows = [
        (
            "Index columns are unique",
            "Warning",
            "Warning",
            "UniquenessConstraint(Uniqueness(Stream(locationId, ?),None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Uniqueness should be 1.",
        ),
    ]

    one_complete_column_rule = {
        RuleName.complete_columns: [
            IndCQC.location_id,
        ]
    }
    two_complete_columns_rule = {
        RuleName.complete_columns: [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
        ]
    }
    one_complete_column_complete_rows = [
        ("loc_1",),
    ]
    one_complete_column_incomplete_rows = [
        (None,),
    ]
    two_complete_columns_both_complete_rows = [
        ("loc_1", date(2024, 1, 1)),
    ]
    two_complete_columns_one_incomplete_rows = [
        (None, date(2024, 1, 1)),
    ]
    two_complete_columns_both_incomplete_rows = [
        (None, None),
    ]

    one_complete_column_result_complete_rows = [
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Success",
            "",
        ),
    ]
    one_complete_column_result_incomplete_rows = [
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Completeness of locationId should be 1.",
        ),
    ]
    two_complete_columns_result_both_complete_rows = [
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Success",
            "",
        ),
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None,None))",
            "Success",
            "",
        ),
    ]
    two_complete_columns_result_one_incomplete_rows = [
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Completeness of locationId should be 1.",
        ),
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None,None))",
            "Success",
            "",
        ),
    ]
    two_complete_columns_result_both_incomplete_rows = [
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Completeness of locationId should be 1.",
        ),
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Completeness of cqc_location_import_date should be 1.",
        ),
    ]

    multiple_rules = {
        RuleName.size_of_dataset: 1,
        RuleName.index_columns: [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
        ],
        RuleName.complete_columns: [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
        ],
    }

    multiple_rules_rows = [
        ("loc_1", date(2024, 1, 1)),
    ]
    multiple_rules_results_rows = [
        (
            "Size of dataset",
            "Warning",
            "Success",
            "SizeConstraint(Size(None))",
            "Success",
            "",
        ),
        (
            "Index columns are unique",
            "Warning",
            "Success",
            "UniquenessConstraint(Uniqueness(Stream(locationId, ?),None,None))",
            "Success",
            "",
        ),
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(locationId,None,None))",
            "Success",
            "",
        ),
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None,None))",
            "Success",
            "",
        ),
    ]

    unknown_rules = {
        RuleName.size_of_dataset: 1,
        "unknown_rule": "some_value",
    }

    min_values_rule = {
        RuleName.min_values: {
            IndCQC.number_of_beds: 1,
        }
    }
    min_values_multiple_columns_rule = {
        RuleName.min_values: {
            IndCQC.number_of_beds: 1,
            IndCQC.pir_people_directly_employed_cleaned: 0,
        }
    }
    min_values_below_minimum_rows = [
        ("loc_1", 0),
    ]
    min_values_equal_minimum_rows = [
        ("loc_1", 1),
    ]
    min_values_above_minimum_rows = [
        ("loc_1", 2),
    ]
    min_values_multiple_columns_rows = [
        ("loc_1", 0, 0),
    ]

    min_values_result_success_rows = [
        (
            "Min value in column",
            "Warning",
            "Success",
            "MinimumConstraint(Minimum(numberOfBeds,None,None))",
            "Success",
            "",
        ),
    ]
    min_values_result_below_minimum_rows = [
        (
            "Min value in column",
            "Warning",
            "Warning",
            "MinimumConstraint(Minimum(numberOfBeds,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! The minimum value for numberOfBeds should be 1.",
        ),
    ]
    min_values_result_multiple_columns_rows = [
        (
            "Min value in column",
            "Warning",
            "Warning",
            "MinimumConstraint(Minimum(numberOfBeds,None,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! The minimum value for numberOfBeds should be 1.",
        ),
        (
            "Min value in column",
            "Warning",
            "Success",
            "MinimumConstraint(Minimum(pir_people_directly_employed_cleaned,None,None))",
            "Success",
            "",
        ),
    ]

    max_values_rule = {
        RuleName.max_values: {
            IndCQC.number_of_beds: 10,
        }
    }
    max_values_multiple_columns_rule = {
        RuleName.max_values: {
            IndCQC.number_of_beds: 10,
            IndCQC.pir_people_directly_employed_cleaned: 20,
        }
    }
    max_values_below_maximum_rows = [
        ("loc_1", 9),
    ]
    max_values_equal_maximum_rows = [
        ("loc_1", 10),
    ]
    max_values_above_maximum_rows = [
        ("loc_1", 11),
    ]
    max_values_multiple_columns_rows = [
        ("loc_1", 20, 20),
    ]

    max_values_result_success_rows = [
        (
            "Max value in column",
            "Warning",
            "Success",
            "MaximumConstraint(Maximum(numberOfBeds,None,None))",
            "Success",
            "",
        ),
    ]
    max_values_result_above_maximum_rows = [
        (
            "Max value in column",
            "Warning",
            "Warning",
            "MaximumConstraint(Maximum(numberOfBeds,None,None))",
            "Failure",
            "Value: 11.0 does not meet the constraint requirement! The maximum value for numberOfBeds should be 10.",
        ),
    ]
    max_values_result_multiple_columns_rows = [
        (
            "Max value in column",
            "Warning",
            "Warning",
            "MaximumConstraint(Maximum(numberOfBeds,None,None))",
            "Failure",
            "Value: 20.0 does not meet the constraint requirement! The maximum value for numberOfBeds should be 10.",
        ),
        (
            "Max value in column",
            "Warning",
            "Success",
            "MaximumConstraint(Maximum(pir_people_directly_employed_cleaned,None,None))",
            "Success",
            "",
        ),
    ]

    categorical_values_rule = {
        RuleName.categorical_values_in_columns: {
            IndCQC.cqc_sector: [Sector.independent, Sector.local_authority]
        }
    }
    categorical_values_all_present_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", Sector.local_authority),
    ]
    categorical_values_some_present_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", None),
    ]
    categorical_values_extra_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", Sector.local_authority),
        ("loc_3", "other value"),
    ]

    categorical_values_result_success_rows = [
        (
            "Categorical values are in list of expected values",
            "Warning",
            "Success",
            "ComplianceConstraint(Compliance(cqc_sector contained in Independent,Local authority,`cqc_sector` IS NULL OR `cqc_sector` IN ('Independent','Local authority'),None,List(cqc_sector),None))",
            "Success",
            "",
        ),
    ]
    categorical_values_result_failure_rows = [
        (
            "Categorical values are in list of expected values",
            "Warning",
            "Warning",
            "ComplianceConstraint(Compliance(cqc_sector contained in Independent,Local authority,`cqc_sector` IS NULL OR `cqc_sector` IN ('Independent','Local authority'),None,List(cqc_sector),None))",
            "Failure",
            "Value: 0.6666666666666666 does not meet the constraint requirement! Values in cqc_sector should be one of :['Independent', 'Local authority'].",
        ),
    ]

    distinct_values_rule = {
        RuleName.distinct_values: {
            IndCQC.cqc_sector: 2,
        }
    }

    distinct_values_multiple_columns_rule = {
        RuleName.distinct_values: {
            IndCQC.cqc_sector: 2,
            IndCQC.dormancy: 3,
        }
    }

    distinct_values_success_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", Sector.local_authority),
    ]
    fewer_distinct_values_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", Sector.independent),
    ]
    more_distinct_values_rows = [
        ("loc_1", Sector.independent),
        ("loc_2", Sector.local_authority),
        ("loc_3", None),
    ]
    distinct_values_multiple_columns_rows = [
        ("loc_1", Sector.independent, "Y"),
        ("loc_2", Sector.local_authority, "N"),
        ("loc_3", None, None),
    ]

    distinct_values_result_success_rows = [
        (
            "Column contains correct number of distinct values",
            "Warning",
            "Success",
            "HistogramBinConstraint(Histogram(cqc_sector,null,2,None,false,Count))",
            "Success",
            "",
        ),
    ]
    fewer_distinct_values_result_rows = [
        (
            "Column contains correct number of distinct values",
            "Warning",
            "Warning",
            "HistogramBinConstraint(Histogram(cqc_sector,null,2,None,false,Count))",
            "Failure",
            "Value: 1 does not meet the constraint requirement! The number of distinct values in cqc_sector should be 2.",
        ),
    ]
    more_distinct_values_result_rows = [
        (
            "Column contains correct number of distinct values",
            "Warning",
            "Warning",
            "HistogramBinConstraint(Histogram(cqc_sector,null,2,None,false,Count))",
            "Failure",
            "Value: 3 does not meet the constraint requirement! The number of distinct values in cqc_sector should be 2.",
        ),
    ]
    distinct_values_result_multiple_columns_rows = [
        (
            "Column contains correct number of distinct values",
            "Warning",
            "Warning",
            "HistogramBinConstraint(Histogram(cqc_sector,null,2,None,false,Count))",
            "Failure",
            "Value: 3 does not meet the constraint requirement! The number of distinct values in cqc_sector should be 2.",
        ),
        (
            "Column contains correct number of distinct values",
            "Warning",
            "Success",
            "HistogramBinConstraint(Histogram(dormancy,null,3,None,false,Count))",
            "Success",
            "",
        ),
    ]

    add_column_with_length_of_string_rows = [
        ("loc_1",),
    ]
    expected_add_column_with_length_of_string_rows = [
        ("loc_1", 5),
    ]

    check_rows = fewer_distinct_values_result_rows

    custom_type_rule = {
        RuleName.custom_type: CustomValidationRules.care_home_and_primary_service_type
    }

    custom_type_related_rows = [
        ("loc 1", CareHome.care_home, PrimaryServiceType.care_home_only),
        ("loc 2", CareHome.care_home, PrimaryServiceType.care_home_with_nursing),
        ("loc 3", CareHome.not_care_home, PrimaryServiceType.non_residential),
    ]
    expected_custom_type_related_rows = [
        (
            "custom type",
            "Warning",
            "Success",
            "ComplianceConstraint(Compliance(care_home_and_primary_service_type,(careHome = 'N' AND primary_service_type = 'non-residential') OR (careHome = 'Y' AND primary_service_type = 'Care home with nursing') OR (careHome = 'Y' AND primary_service_type = 'Care home without nursing'),None,List(),None))",
            "Success",
            "",
        ),
    ]

    custom_type_unrelated_rows = [
        ("loc 1", CareHome.care_home, PrimaryServiceType.non_residential),
        ("loc 2", CareHome.not_care_home, PrimaryServiceType.care_home_with_nursing),
        ("loc 3", CareHome.not_care_home, PrimaryServiceType.care_home_only),
    ]
    expected_custom_type_unrelated_rows = [
        (
            "custom type",
            "Warning",
            "Warning",
            "ComplianceConstraint(Compliance(care_home_and_primary_service_type,(careHome = 'N' AND primary_service_type = 'non-residential') OR (careHome = 'Y' AND primary_service_type = 'Care home with nursing') OR (careHome = 'Y' AND primary_service_type = 'Care home without nursing'),None,List(),None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! The data in carehome and primary_service_type should be related.",
        ),
    ]


@dataclass
class ValidateLocationsAPICleanedData:
    # fmt: off
    raw_cqc_locations_rows = [
        ("1-000000001", "1-001", "20240101", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000002", "1-001", "20240101", LocationType.social_care_identifier, RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000001", "1-001", "20240201", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000002", "1-001", "20240201", "not social care org", RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
    ]
    # fmt: on

    # fmt: off
    cleaned_cqc_locations_rows = [
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
    ]
    # fmt: on


    # fmt: off
    calculate_expected_size_rows = [
        ("loc_1", "prov_1", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_2", "prov_1", "non social care org", RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_3", "prov_1", None, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_4", "prov_1", LocationType.social_care_identifier, RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_5", "prov_1", "non social care org", RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_6", "prov_1", None, RegistrationStatus.deregistered,  [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_7", "prov_1", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_8", "prov_1", "non social care org", RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_9", "prov_1", None, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_10", "prov_1", LocationType.social_care_identifier, RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_11", "prov_1", "non social care org", RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_12", "prov_1", None, RegistrationStatus.deregistered,  [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        ("loc_13", "prov_1", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], None),
        ("loc_14", None, LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}], [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        (RecordsToRemoveInLocationsData.dental_practice, "prov_1", LocationType.social_care_identifier, RegistrationStatus.registered, None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
        (RecordsToRemoveInLocationsData.temp_registration, "prov_1", LocationType.social_care_identifier, RegistrationStatus.registered, None, [{CQCL.name: "name", CQCL.code: "A1", CQCL.contacts: []}]),
    ]
    # fmt: on

    identify_if_location_has_a_known_value_when_array_type_rows = [
        ("loc_1", []),
        ("loc_1", [{CQCL.name: "name", CQCL.code: "A1"}]),
        ("loc_1", None),
        ("loc_2", []),
        ("loc_3", None),
    ]
    expected_identify_if_location_has_a_known_value_when_array_type_rows = [
        ("loc_1", [], True),
        ("loc_1", [{CQCL.name: "name", CQCL.code: "A1"}], True),
        ("loc_1", None, True),
        ("loc_2", [], False),
        ("loc_3", None, False),
    ]

    identify_if_location_has_a_known_value_when_not_array_type_rows = [
        ("loc_1", None),
        ("loc_1", "prov_1"),
        ("loc_1", None),
        ("loc_2", None),
    ]
    expected_identify_if_location_has_a_known_value_when_not_array_type_rows = [
        ("loc_1", None, True),
        ("loc_1", "prov_1", True),
        ("loc_1", None, True),
        ("loc_2", None, False),
    ]


@dataclass
class ValidateProvidersAPICleanedData:
    raw_cqc_providers_rows = [
        ("1-000000001", "20240101"),
        ("1-000000002", "20240101"),
        ("1-000000001", "20240201"),
        ("1-000000002", "20240201"),
    ]
    cleaned_cqc_providers_rows = [
        ("1-000000001", date(2024, 1, 1), "name", Sector.independent),
        ("1-000000002", date(2024, 1, 1), "name", Sector.independent),
        ("1-000000001", date(2024, 1, 9), "name", Sector.independent),
        ("1-000000002", date(2024, 1, 9), "name", Sector.independent),
    ]

    calculate_expected_size_rows = raw_cqc_providers_rows


@dataclass
class ValidateCleanedIndCqcData:
    # fmt: off
    merged_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1), CareHome.care_home, "name", "postcode", "2024", "01", "01"),
        ("1-000000002", date(2024, 1, 1), CareHome.not_care_home, "name", "postcode", "2024", "01", "01"),
        ("1-000000001", date(2024, 2, 1), CareHome.care_home, "name", "postcode", "2024", "02", "01"),
        ("1-000000002", date(2024, 2, 1), CareHome.not_care_home, "name", "postcode", "2024", "02", "01"),
    ]

    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5, "source", 5.0, 5.0, 5, "2024", "01", "01"),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5, "source", 5.0, 5.0, 5, "2024", "01", "01"),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5, "source", 5.0, 5.0, 5, "2024", "01", "09"),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5, "source", 5.0, 5.0, 5, "2024", "01", "09"),
    ]
    # fmt: on

    calculate_expected_size_rows = [
        (
            "1-000000001",
            date(2024, 1, 1),
            CareHome.care_home,
            "name",
            "postcode",
            "2024",
            "01",
            "01",
        ),
    ]


@dataclass
class ValidateCareHomeIndCqcFeaturesData:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1), CareHome.care_home, [{"name": "Name"}]),
        ("1-000000002", date(2024, 1, 1), CareHome.care_home, [{"name": "Name"}]),
        ("1-000000001", date(2024, 1, 9), CareHome.care_home, [{"name": "Name"}]),
        ("1-000000002", date(2024, 1, 9), CareHome.care_home, [{"name": "Name"}]),
    ]

    care_home_ind_cqc_features_rows = [
        ("1-000000001", date(2024, 1, 1), "region", 5, 5, "Y", "features", 5.0),
        ("1-000000002", date(2024, 1, 1), "region", 5, 5, "Y", "features", 5.0),
        ("1-000000001", date(2024, 1, 9), "region", 5, 5, "Y", "features", 5.0),
        ("1-000000002", date(2024, 1, 9), "region", 5, 5, "Y", "features", 5.0),
    ]

    calculate_expected_size_rows = [
        ("1-001", date(2024, 1, 1), CareHome.care_home, [{"name": "Name"}]),
        ("1-002", date(2024, 1, 1), CareHome.care_home, None),
        ("1-003", date(2024, 1, 1), CareHome.not_care_home, [{"name": "Name"}]),
        ("1-004", date(2024, 1, 1), CareHome.not_care_home, None),
        ("1-005", date(2024, 1, 1), None, [{"name": "Name"}]),
        ("1-006", date(2024, 1, 1), None, None),
    ]
    # fmt: on


@dataclass
class ValidateFeaturesNonResASCWDSWithDormancyIndCqcData:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-001", date(2024, 1, 1), CareHome.not_care_home, Dormancy.dormant, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]),
        ("1-002", date(2024, 1, 1), CareHome.not_care_home, Dormancy.not_dormant, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]),
        ("1-001", date(2024, 1, 9), CareHome.not_care_home, Dormancy.dormant, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]),
        ("1-002", date(2024, 1, 9), CareHome.not_care_home, Dormancy.not_dormant, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]),
    ]
    # fmt: on

    non_res_ascwds_ind_cqc_features_rows = [
        ("1-001", date(2024, 1, 1)),
        ("1-002", date(2024, 1, 1)),
        ("1-001", date(2024, 1, 9)),
        ("1-002", date(2024, 1, 9)),
    ]

    # fmt: off
    calculate_expected_size_rows = [
        ("1-001", date(2024, 1, 1), CareHome.not_care_home, Dormancy.dormant, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]),
        ("1-002", date(2024, 1, 1), CareHome.not_care_home, Dormancy.dormant, [{"name": "Name", "description": "Desc"}], None), # filtered - null specialism
        ("1-003", date(2024, 1, 1), CareHome.not_care_home, Dormancy.dormant, None, [{"name": "Name"}]), # filtered - null service
        ("1-005", date(2024, 1, 1), CareHome.not_care_home, None, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]), # filtered - null dormancy
        ("1-004", date(2024, 1, 1), CareHome.care_home, Dormancy.dormant, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]), # filtered - care home
    ]


@dataclass
class ValidateFeaturesNonResASCWDSWithoutDormancyIndCqcData:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-001", date(2024, 1, 1), CareHome.not_care_home, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]),
        ("1-002", date(2024, 1, 1), CareHome.not_care_home, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]),
        ("1-001", date(2024, 1, 9), CareHome.not_care_home, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]),
        ("1-002", date(2024, 1, 9), CareHome.not_care_home, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]),
    ]
    # fmt: on

    non_res_ascwds_ind_cqc_features_rows = [
        ("1-001", date(2024, 1, 1)),
        ("1-002", date(2024, 1, 1)),
        ("1-001", date(2024, 1, 9)),
        ("1-002", date(2024, 1, 9)),
    ]

    # fmt: off
    calculate_expected_size_rows = [
        ("1-001", date(2024, 1, 1), CareHome.not_care_home, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]),
        ("1-002", date(2024, 1, 1), CareHome.not_care_home, [{"name": "Name", "description": "Desc"}], None), # filtered - null specialism
        ("1-003", date(2024, 1, 1), CareHome.not_care_home, None, [{"name": "Name"}]), # filtered - null service
        ("1-004", date(2024, 1, 1), CareHome.care_home, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]), # filtered - care home
        ("1-005", date(2025, 1, 2), CareHome.not_care_home, [{"name": "Name", "description": "Desc"}], [{"name": "Name"}]), # filtered - date after 1/1/2025
    ]
    # fmt: on


@dataclass
class ValidateEstimatedIndCqcFilledPostsData:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1),),
        ("1-000000002", date(2024, 1, 1),),
        ("1-000000001", date(2024, 2, 1),),
        ("1-000000002", date(2024, 2, 1),),
    ]

    estimated_ind_cqc_filled_posts_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0),
    ]
    # fmt: on

    calculate_expected_size_rows = [
        (
            "1-000000001",
            date(2024, 1, 1),
        ),
    ]


@dataclass
class ValidateEstimatedIndCqcFilledPostsByJobRoleData:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1),),
        ("1-000000002", date(2024, 1, 1),),
        ("1-000000001", date(2024, 2, 1),),
        ("1-000000002", date(2024, 2, 1),),
    ]

    estimated_ind_cqc_filled_posts_by_job_role_rows = [
        ("1-000000001", date(2024, 1, 1),),
        ("1-000000002", date(2024, 1, 1),),
        ("1-000000001", date(2024, 1, 9),),
        ("1-000000002", date(2024, 1, 9),),
    ]
    # fmt: on

    calculate_expected_size_rows = [
        (
            "1-000000001",
            date(2024, 1, 1),
        ),
    ]


@dataclass
class ValidateLocationsAPIRawData:
    raw_cqc_locations_rows = [
        ("1-00001", "20240101", "1-001", "name", LocationType.social_care_identifier),
        ("1-00002", "20240101", "1-001", "name", LocationType.social_care_identifier),
        ("1-00001", "20240201", "1-001", "name", LocationType.social_care_identifier),
        ("1-00002", "20240201", "1-001", "name", LocationType.social_care_identifier),
        ("1-00002", "20240201", "1-001", "name", LocationType.social_care_identifier),
    ]


@dataclass
class ValidateProvidersAPIRawData:
    # fmt: off
    raw_cqc_providers_rows = [
        ("1-000000001", "20240101", "name"),
        ("1-000000002", "20240101", "name"),
        ("1-000000001", "20240201", "name"),
        ("1-000000002", "20240201", "name"),
    ]


@dataclass
class RawDataAdjustments:
    expected_worker_data = [
        ("worker_1", "20240101", "estab_1", "other"),
        ("1737540", "20240101", "estab_1", "other"),
        ("1737540", "20230802", "estab_1", "other"),
        ("1737540", "20240101", "28208", "other"),
        ("worker_1", "20230802", "estab_1", "other"),
        ("worker_1", "20230802", "28208", "other"),
        ("worker_1", "20240101", "28208", "other"),
    ]
    worker_data_with_single_row_to_remove = [
        *expected_worker_data,
        ("1737540", "20230802", "28208", "other"),
    ]
    worker_data_with_multiple_rows_to_remove = [
        *expected_worker_data,
        ("1737540", "20230802", "28208", "other"),
        ("1737540", "20230802", "28208", "something else"),
    ]

    worker_data_without_rows_to_remove = expected_worker_data

    locations_data_with_multiple_rows_to_remove = [
        ("loc_1", "other"),
        (RecordsToRemoveInLocationsData.dental_practice, "other"),
        (RecordsToRemoveInLocationsData.dental_practice, "something else"),
        (RecordsToRemoveInLocationsData.temp_registration, "other"),
        (RecordsToRemoveInLocationsData.temp_registration, "something else"),
    ]

    locations_data_with_single_rows_to_remove = [
        ("loc_1", "other"),
        (RecordsToRemoveInLocationsData.dental_practice, "other"),
        (RecordsToRemoveInLocationsData.temp_registration, "other"),
    ]

    locations_data_without_rows_to_remove = [
        ("loc_1", "other"),
    ]

    expected_locations_data = locations_data_without_rows_to_remove


@dataclass
class DiagnosticsOnKnownFilledPostsData:
    estimate_filled_posts_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            10.0,
            10.0,
            PrimaryServiceType.care_home_only,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            None,
            None,
            None,
            10.0,
            "2024",
            "01",
            "01",
            "20240101",
        ),
    ]


@dataclass
class DiagnosticsOnCapacityTrackerData:
    estimate_filled_posts_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            CareHome.care_home,
            PrimaryServiceType.care_home_only,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            None,
            None,
            None,
            10.0,
            10,
            1704067200,
            "2024",
            "01",
            "01",
            "20240101",
        ),
        (
            "loc 1",
            date(2024, 2, 2),
            CareHome.care_home,
            PrimaryServiceType.care_home_only,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            None,
            None,
            None,
            10.0,
            10,
            1706832000,
            "2024",
            "01",
            "01",
            "20240101",
        ),
        (
            "loc 2",
            date(2024, 1, 1),
            CareHome.not_care_home,
            PrimaryServiceType.non_residential,
            10.0,
            None,
            None,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            1704067200,
            "2024",
            "01",
            "01",
            "20240101",
        ),
    ]

    capacity_tracker_care_home_rows = [
        ("loc 1", date(2024, 1, 1), 8, 3, 11, "2024", "01", "01", "20240101"),
        ("loc 1", date(2024, 2, 1), 8, 3, 11, "2024", "01", "01", "20240101"),
    ]
    capacity_tracker_non_res_rows = [
        ("loc 2", date(2024, 1, 1), 10, 80, "2024", "01", "01", "20240101"),
    ]

    join_capacity_tracker_care_home_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            CareHome.care_home,
            PrimaryServiceType.care_home_only,
            10.0,
            "2024",
            "01",
            "01",
            "20240101",
        ),
        (
            "loc 1",
            date(2024, 2, 2),
            CareHome.care_home,
            PrimaryServiceType.care_home_only,
            10.0,
            "2024",
            "01",
            "01",
            "20240101",
        ),
    ]

    expected_joined_care_home_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            CareHome.care_home,
            PrimaryServiceType.care_home_only,
            10.0,
            "2024",
            "01",
            "01",
            "20240101",
            date(2024, 1, 1),
            8,
            3,
            11,
        ),
        (
            "loc 1",
            date(2024, 2, 2),
            CareHome.care_home,
            PrimaryServiceType.care_home_only,
            10.0,
            "2024",
            "01",
            "01",
            "20240101",
            date(2024, 2, 1),
            8,
            3,
            11,
        ),
    ]

    join_capacity_tracker_non_res_rows = [
        (
            "loc 2",
            date(2024, 1, 1),
            CareHome.not_care_home,
            PrimaryServiceType.non_residential,
            10.0,
            None,
            None,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            1704067200,
            "2024",
            "01",
            "01",
            "20240101",
        ),
    ]
    expected_joined_non_res_rows = [
        (
            "loc 2",
            date(2024, 1, 1),
            CareHome.not_care_home,
            PrimaryServiceType.non_residential,
            10.0,
            None,
            None,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            10.0,
            None,
            1704067200,
            "2024",
            "01",
            "01",
            "20240101",
            date(2024, 1, 1),
            10,
            80,
        ),
    ]

    convert_to_all_posts_using_ratio_rows = [
        ("loc 1", 1.0),
        ("loc 2", 6.0),
        ("loc 3", None),
    ]
    expected_convert_to_all_posts_using_ratio_rows = [
        ("loc 1", 1.0, 1.25),
        ("loc 2", 6.0, 7.5),
        ("loc 3", None, None),
    ]

    calculate_care_worker_ratio_rows = [
        ("loc 1", 8.0, 10.0),
        ("loc 2", 16.0, 20.0),
        ("loc 3", 24.0, 30.0),
        ("loc 4", None, 40.0),
        ("loc 5", 40.0, None),
        ("loc 6", None, None),
    ]
    expected_care_worker_ratio = 0.8


@dataclass
class DiagnosticsUtilsData:
    filter_to_known_values_rows = [
        (
            "loc 1",
            1.0,
            1.0,
        ),
        (
            "loc 2",
            None,
            1.0,
        ),
        (
            "loc 3",
            2.0,
            None,
        ),
    ]

    expected_filter_to_known_values_rows = [
        (
            "loc 1",
            1.0,
            1.0,
        ),
        (
            "loc 3",
            2.0,
            None,
        ),
    ]
    list_of_models = ["model_type_one", "model_type_two"]
    expected_list_of_models = [
        *CatValues.estimate_filled_posts_source_column_values.categorical_values,
        IndCQC.estimate_filled_posts,
    ]
    restructure_dataframe_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            10.0,
            PrimaryServiceType.care_home_only,
            13.0,
            12.0,
            "2024",
            "01",
            "01",
            "20240101",
        ),
    ]
    expected_restructure_dataframe_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            "model_type_one",
            13.0,
            "2024",
            "01",
            "01",
            "20240101",
        ),
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            "model_type_two",
            12.0,
            "2024",
            "01",
            "01",
            "20240101",
        ),
    ]
    # fmt: off
    calculate_distribution_metrics_rows = [
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0),
    ]
    expected_calculate_distribution_mean_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 15.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 15.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 35.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 35.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 55.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 55.0),
    ]
    expected_calculate_distribution_standard_deviation_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 7.0710678118654755),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 7.0710678118654755),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 7.0710678118654755),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 7.0710678118654755),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 7.0710678118654755),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 7.0710678118654755),
    ]
    expected_calculate_distribution_kurtosis_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, -2.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, -2.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, -2.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, -2.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, -2.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, -2.0),
    ]
    expected_calculate_distribution_skewness_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 0.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 0.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 0.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 0.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 0.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 0.0),
    ]
    expected_calculate_distribution_metrics_rows =[
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 15.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 20.0, 15.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 35.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 40.0, 35.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 55.0, 7.0710678118654755, -2.0, 0.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 60.0, 55.0, 7.0710678118654755, -2.0, 0.0),
    ]
    # fmt: on

    calculate_residuals_rows = [
        ("loc 1", 10.0, 5.0),
        ("loc 2", 5.0, 10.0),
    ]
    expected_calculate_residual_rows = [
        ("loc 1", 10.0, 5.0, -5.0),
        ("loc 2", 5.0, 10.0, 5.0),
    ]
    expected_calculate_absolute_residual_rows = [
        ("loc 1", 10.0, 5.0, -5.0, 5.0),
        ("loc 2", 5.0, 10.0, 5.0, 5.0),
    ]
    expected_calculate_percentage_residual_rows = [
        ("loc 1", 10.0, 5.0, -1.0),
        ("loc 2", 5.0, 10.0, 0.5),
    ]
    expected_calculate_standardised_residual_rows = [
        ("loc 1", 10.0, 5.0, -5.0, -1.58113883),
        ("loc 2", 5.0, 10.0, 5.0, 2.23606798),
    ]
    expected_calculate_residuals_rows = [
        ("loc 1", 10.0, 5.0, -5.0, 5.0, -1.0, -1.58113883),
        ("loc 2", 5.0, 10.0, 5.0, 5.0, 0.5, 2.23606798),
    ]
    # fmt: off
    calculate_aggregate_residuals_rows = [
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 10.0, 0.1, 0.9),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, -20.0, 20.0, 0.2, 1.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 30.0, 0.3, 1.1),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, -40.0, 40.0, 0.4, 1.2),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 50.0, 0.5, 1.3),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, -60.0, 60.0, 0.6, 1.4),
    ]
    expected_calculate_aggregate_residuals_rows = [
        ("loc 1", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 10.0, 10.0, 0.1, 0.9, 15.0, 0.15, 10.0, -20.0, 0.5, 1.0, 1.0, 1.0),
        ("loc 2", PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, -20.0, 20.0, 0.2, 1.0, 15.0, 0.15, 10.0, -20.0, 0.5, 1.0, 1.0, 1.0),
        ("loc 3", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 30.0, 30.0, 0.3, 1.1, 35.0, 0.35, 30.0, -40.0, 0.0, 0.5, 0.0, 0.0),
        ("loc 4", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, -40.0, 40.0, 0.4, 1.2,  35.0, 0.35, 30.0, -40.0, 0.0, 0.5, 0.0, 0.0),
        ("loc 5", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 50.0, 50.0, 0.5, 1.3, 55.0, 0.55, 50.0, -60.0, 0.0, 0.0, 0.0, 0.0),
        ("loc 6", PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, -60.0, 60.0, 0.6, 1.4, 55.0, 0.55, 50.0, -60.0, 0.0, 0.0, 0.0, 0.0),
    ]

    create_summary_dataframe_rows = [
        ("loc 1", date(2024, 1, 1), PrimaryServiceType.care_home_only, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, "2024", "01", "01", "20240101", 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        ("loc 2", date(2024, 1, 1), PrimaryServiceType.care_home_only, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, "2024", "01", "01", "20240101", 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        ("loc 3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, "2024", "01", "01", "20240101", 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        ("loc 4", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, "2024", "01", "01", "20240101", 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        ("loc 5", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.imputed_posts_care_home_model, 100.0, "2024", "01", "01", "20240101", 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
        ("loc 6", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.imputed_posts_care_home_model, 100.0, "2024", "01", "01", "20240101", 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
    ]
    expected_create_summary_dataframe_rows = [
        (PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 1.0, 2.0, 3.0, 4.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        (PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 2.0, 3.0, 4.0, 5.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        (PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 3.0, 4.0, 5.0, 6.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
    ]

    # fmt: on


@dataclass
class ASCWDSFilteringUtilsData:
    add_filtering_column_rows = [
        ("loc 1", 10.0),
        ("loc 2", None),
    ]
    expected_add_filtering_column_rows = [
        ("loc 1", 10.0, AscwdsFilteringRule.populated),
        ("loc 2", None, AscwdsFilteringRule.missing_data),
    ]
    update_filtering_rule_populated_to_nulled_rows = [
        (
            "loc 1",
            10.0,
            10.0,
            AscwdsFilteringRule.populated,
        ),
        (
            "loc 2",
            10.0,
            None,
            AscwdsFilteringRule.populated,
        ),
        (
            "loc 3",
            10.0,
            None,
            AscwdsFilteringRule.missing_data,
        ),
    ]
    update_filtering_rule_populated_to_winsorized_rows = [
        (
            "loc 1",
            10.0,
            9.0,
            AscwdsFilteringRule.populated,
        ),
        (
            "loc 2",
            10.0,
            11.0,
            AscwdsFilteringRule.populated,
        ),
        (
            "loc 3",
            10.0,
            10.0,
            AscwdsFilteringRule.populated,
        ),
    ]
    update_filtering_rule_winsorized_to_nulled_rows = [
        (
            "loc 1",
            10.0,
            9.0,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        ),
        (
            "loc 2",
            10.0,
            None,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        ),
    ]
    expected_update_filtering_rule_populated_to_nulled_rows = [
        (
            "loc 1",
            10.0,
            10.0,
            AscwdsFilteringRule.populated,
        ),
        (
            "loc 2",
            10.0,
            None,
            AscwdsFilteringRule.contained_invalid_missing_data_code,
        ),
        (
            "loc 3",
            10.0,
            None,
            AscwdsFilteringRule.missing_data,
        ),
    ]
    expected_update_filtering_rule_populated_to_winsorized_rows = [
        (
            "loc 1",
            10.0,
            9.0,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        ),
        (
            "loc 2",
            10.0,
            11.0,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        ),
        (
            "loc 3",
            10.0,
            10.0,
            AscwdsFilteringRule.populated,
        ),
    ]
    expected_update_filtering_rule_winsorized_to_nulled_rows = [
        (
            "loc 1",
            10.0,
            9.0,
            AscwdsFilteringRule.winsorized_beds_ratio_outlier,
        ),
        (
            "loc 2",
            10.0,
            None,
            AscwdsFilteringRule.contained_invalid_missing_data_code,
        ),
    ]


@dataclass
class NullFilledPostsUsingInvalidMissingDataCodeData:
    null_filled_posts_using_invalid_missing_data_code_rows = [
        ("loc 1", 20.0, 20.0, AscwdsFilteringRule.populated),
        ("loc 2", 999.0, 999.0, AscwdsFilteringRule.populated),
        ("loc 3", None, None, AscwdsFilteringRule.missing_data),
    ]
    expected_null_filled_posts_using_invalid_missing_data_code_rows = [
        ("loc 1", 20.0, 20.0, AscwdsFilteringRule.populated),
        ("loc 2", 999.0, None, AscwdsFilteringRule.contained_invalid_missing_data_code),
        ("loc 3", None, None, AscwdsFilteringRule.missing_data),
    ]


@dataclass
class NullGroupedProvidersData:
    # fmt: off
    null_grouped_providers_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 13.0, 4, 3.25, AscwdsFilteringRule.populated),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", None, None,  None, 4, None, AscwdsFilteringRule.missing_data),
        ("loc 3", "prov 1", date(2024, 1, 1), "Y", None, None, None, 4, None, AscwdsFilteringRule.missing_data),
        ("loc 1", "prov 1", date(2024, 1, 8), "Y", "estab 1", 12.0, 12.0, 4, 3.0, AscwdsFilteringRule.populated),
        ("loc 2", "prov 1", date(2024, 1, 8), "Y", None, None, None, 4, None, AscwdsFilteringRule.missing_data),
    ]

    calculate_data_for_grouped_provider_identification_where_provider_has_one_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", None, 4),
        ("loc 2", "prov 2", date(2024, 1, 1), "Y", None, None, 5),
        ("loc 3", "prov 3", date(2024, 1, 1), "N", "estab 3", 10.0, None),
    ]
    expected_calculate_data_for_grouped_provider_identification_where_provider_has_one_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 1, 1, 1, 4),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", None, 4, 1, 1, 0, 4),
        ("loc 2", "prov 2", date(2024, 1, 1), "Y", None, None, 5, 1, 0, 0, 5),
        ("loc 3", "prov 3", date(2024, 1, 1), "N", "estab 3", 10.0, None, 1, 1, 1, None),
    ]

    calculate_data_for_grouped_provider_identification_where_provider_has_multiple_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", "estab 2", 14.0, 3),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", 13.0, 4),
        ("loc 2", "prov 1", date(2024, 2, 1), "Y", None, None, 5),
        ("loc 3", "prov 2", date(2024, 1, 1), "Y", None, None, 6),
        ("loc 4", "prov 2", date(2024, 1, 1), "N", "estab 3", None, None),
        ("loc 5", "prov 3", date(2024, 1, 1), "N", None, None, None),
        ("loc 6", "prov 3", date(2024, 1, 1), "N", None, None, None),
    ]
    expected_calculate_data_for_grouped_provider_identification_where_provider_has_multiple_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 2, 2, 2, 7),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", "estab 2", 14.0, 3, 2, 2, 2, 7),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", 13.0, 4, 2, 1, 1, 9),
        ("loc 2", "prov 1", date(2024, 2, 1), "Y", None, None, 5, 2, 1, 1, 9),
        ("loc 3", "prov 2", date(2024, 1, 1), "Y", None, None, 6, 2, 1, 0, 6),
        ("loc 4", "prov 2", date(2024, 1, 1), "N", "estab 3", None, None, 2, 1, 0, 6),
        ("loc 5", "prov 3", date(2024, 1, 1), "N", None, None, None, 2, 0, 0, None),
        ("loc 6", "prov 3", date(2024, 1, 1), "N", None, None, None, 2, 0, 0, None),
    ]

    identify_potential_grouped_providers_rows = [
        ("1", 1, 1, 1),
        ("2", 1, 1, 0),
        ("3", 1, 0, 0),
        ("4", 2, 2, 2),
        ("5", 2, 2, 1),
        ("6", 2, 2, 0),
        ("7", 2, 1, 1),
        ("8", 2, 1, 0),
        ("9", 5, 1, 1),
    ]
    expected_identify_potential_grouped_providers_rows = [
        ("1", 1, 1, 1, False),
        ("2", 1, 1, 0, False),
        ("3", 1, 0, 0, False),
        ("4", 2, 2, 2, False),
        ("5", 2, 2, 1, False),
        ("6", 2, 2, 0, False),
        ("7", 2, 1, 1, True),
        ("8", 2, 1, 0, False),
        ("9", 5, 1, 1, True),
    ]

    null_care_home_grouped_providers_where_location_is_not_care_home = [
        ("loc 1", CareHome.not_care_home, 50.0, 50.0, None, 2, None, True, AscwdsFilteringRule.populated),
    ]

    null_care_home_grouped_providers_where_location_is_not_potential_grouped_provider = [
        ("loc 1", CareHome.care_home, 50.0, 50.0, 2, 3, 25.0, False, AscwdsFilteringRule.populated),
    ]

    null_care_home_grouped_providers_where_filled_posts_below_cutoffs = [
        ("loc 1", CareHome.care_home, 4.0, 4.0, 2, 3, 2.0, True, AscwdsFilteringRule.populated),
    ]

    null_care_home_grouped_providers_where_filled_posts_on_or_above_cutoffs = [
        ("loc 1", CareHome.care_home, 6.0, 6.0, 2, 2, 3.0, True, AscwdsFilteringRule.populated),
        ("loc 2", CareHome.care_home, 7.0, 7.0, 2, 2, 3.0, True, AscwdsFilteringRule.populated),
        ("loc 3", CareHome.care_home, 8.0, 8.0, 2, 6, 4.0, True, AscwdsFilteringRule.populated),
        ("loc 4", CareHome.care_home, 9.0, 9.0, 2, 6, 4.5, True, AscwdsFilteringRule.populated),
        ("loc 5", CareHome.care_home, 9.0, 9.0, 2, 2, 4.5, True, AscwdsFilteringRule.populated),
    ]
    expected_null_care_home_grouped_providers_where_filled_posts_on_or_above_cutoffs = [
        ("loc 1", CareHome.care_home, 6.0, None, 2, 2, None, True, AscwdsFilteringRule.care_home_location_was_grouped_provider),
        ("loc 2", CareHome.care_home, 7.0, None, 2, 2, None, True, AscwdsFilteringRule.care_home_location_was_grouped_provider),
        ("loc 3", CareHome.care_home, 8.0, None, 2, 6, None, True, AscwdsFilteringRule.care_home_location_was_grouped_provider),
        ("loc 4", CareHome.care_home, 9.0, None, 2, 6, None, True, AscwdsFilteringRule.care_home_location_was_grouped_provider),
        ("loc 5", CareHome.care_home, 9.0, None, 2, 2, None, True, AscwdsFilteringRule.care_home_location_was_grouped_provider),
    ]
    # fmt: on


@dataclass
class ArchiveFilledPostsEstimates:
    filled_posts_rows = [
        ("loc 1", date(2024, 1, 1)),
    ]

    select_import_dates_to_archive_rows = [
        ("loc 1", date(2024, 6, 8)),
        ("loc 1", date(2024, 5, 1)),
        ("loc 1", date(2024, 4, 1)),
        ("loc 1", date(2024, 3, 1)),
        ("loc 1", date(2023, 4, 1)),
        ("loc 1", date(2023, 3, 1)),
    ]
    expected_select_import_dates_to_archive_rows = [
        ("loc 1", date(2024, 6, 8)),
        ("loc 1", date(2024, 5, 1)),
        ("loc 1", date(2024, 4, 1)),
        ("loc 1", date(2023, 4, 1)),
    ]

    create_archive_date_partitions_rows = [
        ("loc 1", date(2024, 1, 2)),
    ]
    expected_create_archive_date_partitions_rows = [
        ("loc 1", date(2024, 1, 2), "02", "01", "2024", "2024-01-02 12:00"),
    ]

    single_digit_number = 9
    expected_single_digit_number_as_string = "09"
    double_digit_number = 10
    expected_double_digit_number_as_string = "10"


@dataclass
class ValidateCleanedCapacityTrackerCareHomeData:
    # fmt: off
    ct_care_home_rows = [
        ("1-000000001", "1", "2", "3", "4", "5", "6", "2024", "01", "01"),
        ("1-000000002", "1", "2", "3", "4", "5", "6", "2024", "01", "01"),
        ("1-000000001", "1", "2", "3", "4", "5", "6", "2024", "02", "01"),
        ("1-000000002", "1", "2", "3", "4", "5", "6", "2024", "02", "01"),
    ]

    cleaned_ct_care_home_rows = [
        ("1-000000001", "1", "2", "3", "4", "5", "6", "2024", "01", "01", date(2024, 1, 1), 6, 15, 21),
        ("1-000000002", "1", "2", "3", "4", "5", "6", "2024", "01", "01", date(2024, 1, 1), 6, 15, 21),
        ("1-000000001", "1", "2", "3", "4", "5", "6", "2024", "02", "01", date(2024, 2, 1), 6, 15, 21),
        ("1-000000002", "1", "2", "3", "4", "5", "6", "2024", "02", "01", date(2024, 2, 1), 6, 15, 21),
    ]
    # fmt: on

    calculate_expected_size_rows = [
        (
            "1-000000001",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "2024",
            "01",
            "01",
        ),
    ]


@dataclass
class ValidateCleanedCapacityTrackerNonResData:
    # fmt: off
    ct_non_res_rows = [
        ("1-000000001", "1", "2", "2024", "01", "01"),
        ("1-000000002", "1", "2", "2024", "01", "01"),
        ("1-000000001", "1", "2", "2024", "02", "01"),
        ("1-000000002", "1", "2", "2024", "02", "01"),
    ]

    cleaned_ct_non_res_rows = [
        ("1-000000001", "1", "2", "2024", "01", "01", date(2024, 1, 1)),
        ("1-000000002", "1", "2", "2024", "01", "01", date(2024, 1, 1)),
        ("1-000000001", "1", "2", "2024", "02", "01", date(2024, 2, 1)),
        ("1-000000002", "1", "2", "2024", "02", "01", date(2024, 2, 1)),
    ]
    # fmt: on

    calculate_expected_size_rows = [
        (
            "1-000000001",
            "1",
            "2",
            "2024",
            "01",
            "01",
        ),
    ]


@dataclass
class ReconciliationUtilsData:
    # fmt: off
    input_ascwds_workplace_rows = ReconciliationData.input_ascwds_workplace_rows
    input_cqc_location_api_rows = ReconciliationData.input_cqc_location_api_rows
    # fmt: on
    dates_to_use_mid_month_rows = [
        ("1-001", date(2024, 3, 28)),
        ("1-002", date(2023, 1, 1)),
    ]
    dates_to_use_first_month_rows = [
        ("1-001", date(2024, 4, 1)),
        ("1-002", date(2023, 1, 1)),
    ]

    expected_prepared_most_recent_cqc_location_rows = [
        ("1-001", "Registered", None, date(2024, 4, 1)),
        ("1-002", "Registered", None, date(2024, 4, 1)),
        ("1-003", "Registered", None, date(2024, 4, 1)),
        ("1-004", "Registered", None, date(2024, 4, 1)),
        ("1-902", "Deregistered", date(2024, 1, 1), date(2024, 4, 1)),
        ("1-903", "Deregistered", date(2024, 3, 1), date(2024, 4, 1)),
        ("1-904", "Deregistered", date(2024, 3, 1), date(2024, 4, 1)),
        ("1-912", "Deregistered", date(2024, 1, 1), date(2024, 4, 1)),
        ("1-913", "Deregistered", date(2024, 3, 1), date(2024, 4, 1)),
        ("1-922", "Deregistered", date(2024, 1, 1), date(2024, 4, 1)),
        ("1-923", "Deregistered", date(2024, 3, 1), date(2024, 4, 1)),
        ("1-995", "Deregistered", date(2024, 4, 1), date(2024, 4, 1)),
    ]

    dates_to_use_rows = [
        ("1-001", date(2024, 3, 28)),
        ("1-002", date(2023, 1, 1)),
    ]

    regtype_rows = [
        ("1", "Not regulated"),
        ("2", "CQC regulated"),
        ("3", None),
    ]

    remove_head_office_accounts_rows = [
        ("1", "1-001", "Head office services"),
        ("2", "1-002", "any non-head office service"),
        ("3", None, "any non-head office service"),
        ("4", None, "Head office services"),
    ]

    first_of_most_recent_month = date(2024, 4, 1)
    first_of_previous_month = date(2024, 3, 1)
    # fmt: off
    filter_to_relevant_rows = [
        ("loc_1", None, date(2024, 3, 31), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_2", None, date(2024, 3, 31), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_3", None, date(2024, 3, 1), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_4", None, date(2024, 3, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_5", None, date(2024, 2, 29), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_6", None, date(2024, 2, 29), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_7", None, date(2024, 4, 1), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_8", None, date(2024, 4, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_9", RegistrationStatus.registered, date(2024, 3, 31), ParentsOrSinglesAndSubs.parents),  # remove
        ("loc_10", RegistrationStatus.registered, date(2024, 3, 31), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
        ("loc_11", RegistrationStatus.registered, date(2024, 3, 1), ParentsOrSinglesAndSubs.parents),  # remove
        ("loc_12", RegistrationStatus.registered, date(2024, 3, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
        ("loc_13", RegistrationStatus.registered, date(2024, 2, 29), ParentsOrSinglesAndSubs.parents),  # remove
        ("loc_14", RegistrationStatus.registered, date(2024, 2, 29), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
        ("loc_15", RegistrationStatus.registered, date(2024, 4, 1), ParentsOrSinglesAndSubs.parents),  # remove
        ("loc_16", RegistrationStatus.registered, date(2024, 4, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
        ("loc_17", RegistrationStatus.deregistered, date(2024, 3, 31), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_18", RegistrationStatus.deregistered, date(2024, 3, 31), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_19", RegistrationStatus.deregistered, date(2024, 3, 1), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_20", RegistrationStatus.deregistered, date(2024, 3, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # keep
        ("loc_21", RegistrationStatus.deregistered, date(2024, 2, 29), ParentsOrSinglesAndSubs.parents),  # keep
        ("loc_22", RegistrationStatus.deregistered, date(2024, 2, 29), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
        ("loc_23", RegistrationStatus.deregistered, date(2024, 4, 1), ParentsOrSinglesAndSubs.parents),  # remove
        ("loc_24", RegistrationStatus.deregistered, date(2024, 4, 1), ParentsOrSinglesAndSubs.singles_and_subs),  # remove
    ]
    # fmt: on

    parents_or_singles_and_subs_rows = [
        ("1", "Yes", "Parent has ownership"),
        ("2", "Yes", "Workplace has ownership"),
        ("3", "No", "Workplace has ownership"),
        ("4", "No", "Parent has ownership"),
    ]
    expected_parents_or_singles_and_subs_rows = [
        ("1", "Yes", "Parent has ownership", ParentsOrSinglesAndSubs.parents),
        ("2", "Yes", "Workplace has ownership", ParentsOrSinglesAndSubs.parents),
        (
            "3",
            "No",
            "Workplace has ownership",
            ParentsOrSinglesAndSubs.singles_and_subs,
        ),
        ("4", "No", "Parent has ownership", ParentsOrSinglesAndSubs.parents),
    ]

    add_singles_and_subs_description_rows = [
        ("loc_1", date(2024, 3, 28)),
        ("loc_2", None),
    ]

    expected_singles_and_subs_description_rows = [
        (
            "loc_1",
            date(2024, 3, 28),
            SingleSubDescription.single_sub_deregistered_description,
        ),
        ("loc_2", None, SingleSubDescription.single_sub_reg_type_description),
    ]

    create_missing_columns_rows = [
        ("id_1", "care_home", "region", "Care Home Name"),
    ]

    expected_create_missing_columns_rows = [
        (
            "id_1",
            "care_home",
            "region",
            "Care Home Name",
            "id_1",
            "id_1",
            "id_1 Care Home Name",
            "id_1 Care Home Name",
            "Open",
            "_",
            "No",
            "Internal",
            "Priority 5",
            "CQC work",
            "CQC work",
            "Yes",
            "N/A",
            "ASC-WDS",
            "CQC work",
            0,
        ),
    ]
    # fmt: off
    final_column_selection_rows = [
        (
            "extra_col", "", "", "", "", "", "", "", "", 0, "", "", "nmds_1", "", "desc_a", "", "", "", "", "", "", "",
        ),
        (
            "extra_col", "", "", "", "", "", "", "", "", 0, "", "", "nmds_2", "", "desc_b", "", "", "", "", "", "", "",
        ),
        (
            "extra_col", "", "", "", "", "", "", "", "", 0, "", "", "nmds_2", "", "desc_a", "", "", "", "", "", "", "",
        ),
        (
            "extra_col", "", "", "", "", "", "", "", "", 0, "", "", "nmds_1", "", "desc_b", "", "", "", "", "", "", "",
        ),
    ]

    expected_final_column_selection_rows = [
        (
             "", "nmds_1", "", "desc_a", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, "",
        ),
        (
             "", "nmds_2", "", "desc_a", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, "",
        ),
        (
             "", "nmds_1", "", "desc_b", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, "",
        ),
        (
             "", "nmds_2", "", "desc_b", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 0, "",
        ),
    ]
    # fmt: on
    add_subject_column_rows = [
        ("loc_1",),
    ]

    expected_add_subject_column_rows = [
        ("loc_1", "test_subject"),
    ]

    new_issues_rows = [
        ("org 1", "loc 1", ""),
        ("org 1", "loc 2", ""),
        ("org 1", "loc 3", ""),
        ("org 2", "loc 4", ""),
        ("org 2", "loc 5", ""),
        ("org 3", "loc 6", ""),
        ("org 5", "loc 7", ""),
    ]

    unique_rows = [
        ("org 1", ""),
        ("org 2", ""),
        ("org 3", ""),
        ("org 6", ""),
    ]

    expected_join_array_of_nmdsids_rows = [
        ("org 1", "", "new_column: loc 2, loc 3, loc 1"),
        ("org 2", "", "new_column: loc 5, loc 4"),
        ("org 3", "", "new_column: loc 6"),
        ("org 6", "", None),
    ]

    new_column = "new_column"

    create_parents_description_rows = [
        ("org 1", None, None, None),
        ("org 2", None, None, "missing"),
        ("org 3", None, "old", None),
        ("org 4", None, "old", "missing"),
        ("org 5", "new", None, None),
        ("org 6", "new", None, "missing"),
        ("org 7", "new", "old", None),
        ("org 8", "new", "old", "missing"),
    ]

    expected_create_parents_description_rows = [
        ("org 1", None, None, None, ""),
        ("org 2", None, None, "missing", "missing "),
        ("org 3", None, "old", None, "old "),
        ("org 4", None, "old", "missing", "old missing "),
        ("org 5", "new", None, None, "new "),
        ("org 6", "new", None, "missing", "new missing "),
        ("org 7", "new", "old", None, "new old "),
        ("org 8", "new", "old", "missing", "new old missing "),
    ]

    get_ascwds_parent_accounts_rows = [
        (
            "nmds_1",
            "estab_1",
            "name",
            "org_1",
            "type",
            "region_id",
            IsParent.is_parent,
            "other",
        ),
        (
            "nmds_2",
            "estab_2",
            "name",
            "org_2",
            "type",
            "region_id",
            IsParent.is_not_parent,
            "other",
        ),
        ("nmds_3", "estab_3", "name", "org_3", "type", "region_id", None, "other"),
    ]
    expected_get_ascwds_parent_accounts_rows = [
        ("nmds_1", "estab_1", "name", "org_1", "type", "region_id"),
    ]

    cqc_data_for_join_rows = [
        ("loc_1", "name"),
        ("loc_2", "name"),
    ]
    ascwds_data_for_join_rows = [
        ("loc_1", "estab_1"),
        ("loc_3", "estab_2"),
    ]
    expected_data_for_join_rows = [
        ("loc_1", "estab_1", "name"),
        ("loc_3", "estab_2", None),
    ]


@dataclass
class EstimateIndCQCFilledPostsByJobRoleData:
    estimated_ind_cqc_filled_posts_rows = [
        (
            "1-001",
            date(2024, 1, 1),
            "Service A",
            "101",
            date(2024, 1, 1),
            3.0,
            ["John Doe"],
        ),
        (
            "1-002",
            date(2025, 1, 1),
            "Service A",
            "101",
            date(2025, 1, 1),
            3.0,
            ["John Doe"],
        ),
        (
            "1-003",
            date(2025, 1, 1),
            "Service B",
            "103",
            date(2025, 1, 1),
            3.0,
            ["John Doe"],
        ),
        (
            "1-004",
            date(2025, 1, 1),
            "Service A",
            "104",
            date(2025, 1, 1),
            3.0,
            ["John Doe"],
        ),
    ]
    cleaned_ascwds_worker_rows = [
        ("101", date(2024, 1, 1), MainJobRoleLabels.senior_management),
        ("101", date(2024, 1, 1), MainJobRoleLabels.care_worker),
        ("101", date(2024, 1, 1), MainJobRoleLabels.care_worker),
        ("101", date(2025, 1, 1), MainJobRoleLabels.care_worker),
        ("101", date(2025, 1, 1), MainJobRoleLabels.care_worker),
        ("103", date(2025, 1, 1), MainJobRoleLabels.senior_management),
        ("103", date(2025, 1, 1), MainJobRoleLabels.registered_nurse),
        ("103", date(2025, 1, 1), MainJobRoleLabels.care_worker),
        ("103", date(2025, 1, 1), MainJobRoleLabels.care_worker),
        ("103", date(2025, 1, 1), MainJobRoleLabels.care_worker),
        ("103", date(2025, 1, 1), MainJobRoleLabels.care_worker),
        ("111", date(2025, 1, 1), MainJobRoleLabels.care_worker),
    ]


@dataclass
class EstimateIndCQCFilledPostsByJobRoleUtilsData:
    list_of_job_roles_for_tests = [
        MainJobRoleLabels.care_worker,
        MainJobRoleLabels.registered_nurse,
        MainJobRoleLabels.senior_care_worker,
        MainJobRoleLabels.senior_management,
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_rows = [
        ("101", date(2024, 1, 1), "1-001", MainJobRoleLabels.care_worker),
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_when_all_job_roles_present_rows = [
        ("101", date(2024, 1, 1), MainJobRoleLabels.care_worker),
        ("101", date(2024, 1, 1), MainJobRoleLabels.care_worker),
        ("101", date(2024, 1, 1), MainJobRoleLabels.registered_nurse),
        ("102", date(2024, 1, 1), MainJobRoleLabels.senior_care_worker),
        ("102", date(2024, 1, 1), MainJobRoleLabels.senior_management),
        ("102", date(2024, 1, 2), MainJobRoleLabels.care_worker),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_all_job_roles_present_rows = [
        (
            "101",
            date(2024, 1, 1),
            {
                MainJobRoleLabels.care_worker: 2,
                MainJobRoleLabels.registered_nurse: 1,
                MainJobRoleLabels.senior_care_worker: 0,
                MainJobRoleLabels.senior_management: 0,
            },
        ),
        (
            "102",
            date(2024, 1, 1),
            {
                MainJobRoleLabels.care_worker: 0,
                MainJobRoleLabels.registered_nurse: 0,
                MainJobRoleLabels.senior_care_worker: 1,
                MainJobRoleLabels.senior_management: 1,
            },
        ),
        (
            "102",
            date(2024, 1, 2),
            {
                MainJobRoleLabels.care_worker: 1,
                MainJobRoleLabels.registered_nurse: 0,
                MainJobRoleLabels.senior_care_worker: 0,
                MainJobRoleLabels.senior_management: 0,
            },
        ),
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_when_some_job_roles_never_present_rows = [
        ("101", date(2024, 1, 1), MainJobRoleLabels.senior_management),
        ("101", date(2024, 1, 1), MainJobRoleLabels.registered_nurse),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_some_job_roles_never_present_rows = [
        (
            "101",
            date(2024, 1, 1),
            {
                MainJobRoleLabels.care_worker: 0,
                MainJobRoleLabels.registered_nurse: 1,
                MainJobRoleLabels.senior_care_worker: 0,
                MainJobRoleLabels.senior_management: 1,
            },
        ),
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_missing_roles_replaced_with_zero_rows = [
        ("101", date(2024, 1, 1), MainJobRoleLabels.registered_nurse),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_missing_roles_replaced_with_zero_rows = [
        (
            "101",
            date(2024, 1, 1),
            {
                MainJobRoleLabels.care_worker: 0,
                MainJobRoleLabels.registered_nurse: 1,
                MainJobRoleLabels.senior_care_worker: 0,
                MainJobRoleLabels.senior_management: 0,
            },
        ),
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_when_single_establishment_has_multiple_dates_rows = [
        ("101", date(2024, 1, 1), MainJobRoleLabels.senior_care_worker),
        ("101", date(2024, 1, 1), MainJobRoleLabels.senior_management),
        ("101", date(2024, 1, 2), MainJobRoleLabels.care_worker),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_single_establishment_has_multiple_dates_rows = [
        (
            "101",
            date(2024, 1, 1),
            {
                MainJobRoleLabels.care_worker: 0,
                MainJobRoleLabels.registered_nurse: 0,
                MainJobRoleLabels.senior_care_worker: 1,
                MainJobRoleLabels.senior_management: 1,
            },
        ),
        (
            "101",
            date(2024, 1, 2),
            {
                MainJobRoleLabels.care_worker: 1,
                MainJobRoleLabels.registered_nurse: 0,
                MainJobRoleLabels.senior_care_worker: 0,
                MainJobRoleLabels.senior_management: 0,
            },
        ),
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_when_multiple_establishments_on_the_same_date_rows = [
        ("101", date(2024, 1, 1), MainJobRoleLabels.senior_care_worker),
        ("101", date(2024, 1, 1), MainJobRoleLabels.senior_management),
        ("102", date(2024, 1, 1), MainJobRoleLabels.care_worker),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_multiple_establishments_on_the_same_date_rows = [
        (
            "101",
            date(2024, 1, 1),
            {
                MainJobRoleLabels.care_worker: 0,
                MainJobRoleLabels.registered_nurse: 0,
                MainJobRoleLabels.senior_care_worker: 1,
                MainJobRoleLabels.senior_management: 1,
            },
        ),
        (
            "101",
            date(2024, 1, 2),
            {
                MainJobRoleLabels.care_worker: 1,
                MainJobRoleLabels.registered_nurse: 0,
                MainJobRoleLabels.senior_care_worker: 0,
                MainJobRoleLabels.senior_management: 0,
            },
        ),
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_when_unrecognised_role_present_rows = [
        ("101", date(2024, 1, 1), MainJobRoleLabels.senior_care_worker),
        ("101", date(2024, 1, 1), "unrecognised_role"),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_unrecognised_role_present_rows = [
        (
            "101",
            date(2024, 1, 1),
            {
                MainJobRoleLabels.care_worker: 0,
                MainJobRoleLabels.registered_nurse: 0,
                MainJobRoleLabels.senior_care_worker: 1,
                MainJobRoleLabels.senior_management: 0,
            },
        ),
    ]

    create_map_column_when_all_columns_populated_rows = [("123", 0, 10, 20, 30)]
    expected_create_map_column_when_all_columns_populated_rows = [
        (
            "123",
            0,
            10,
            20,
            30,
            {
                MainJobRoleLabels.care_worker: 0,
                MainJobRoleLabels.registered_nurse: 10,
                MainJobRoleLabels.senior_care_worker: 20,
                MainJobRoleLabels.senior_management: 30,
            },
        )
    ]
    expected_create_map_column_when_all_columns_populated_and_drop_columns_is_true_rows = [
        (
            "123",
            {
                MainJobRoleLabels.care_worker: 0,
                MainJobRoleLabels.registered_nurse: 10,
                MainJobRoleLabels.senior_care_worker: 20,
                MainJobRoleLabels.senior_management: 30,
            },
        )
    ]
    create_map_column_when_some_columns_populated_rows = [("123", 0, None, 20, None)]
    expected_create_map_column_when_some_columns_populated_rows = [
        (
            "123",
            0,
            None,
            20,
            None,
            {
                MainJobRoleLabels.care_worker: 0,
                MainJobRoleLabels.registered_nurse: None,
                MainJobRoleLabels.senior_care_worker: 20,
                MainJobRoleLabels.senior_management: None,
            },
        )
    ]
    create_map_column_when_no_columns_populated_rows = [("123", None, None, None, None)]
    expected_create_map_column_when_no_columns_populated_rows = [
        (
            "123",
            None,
            None,
            None,
            None,
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
                MainJobRoleLabels.senior_care_worker: None,
                MainJobRoleLabels.senior_management: None,
            },
        )
    ]

    # fmt: off
    estimated_filled_posts_when_single_establishment_has_multiple_dates_rows = [
        ("1-1001", CareHome.care_home, 3, date(2025, 1, 1), "1"),
        ("1-1001", CareHome.care_home, 5, date(2025, 2, 1), "1"),
        ("1-1001", CareHome.care_home, 7, date(2025, 3, 1), "1"),
    ]
    aggregated_job_role_breakdown_when_single_establishment_has_multiple_dates_rows = [
        ("1", date(2025, 1, 1), {MainJobRoleLabels.care_worker: 0, MainJobRoleLabels.registered_nurse: 1}),
        ("1", date(2025, 3, 1), {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2}),
        ("1", date(2025, 5, 1), {MainJobRoleLabels.care_worker: 2, MainJobRoleLabels.registered_nurse: 3}),
    ]
    expected_merge_dataframse_when_single_establishment_has_multiple_dates_rows = [
        ("1-1001", CareHome.care_home, 3, date(2025, 1, 1), "1", {MainJobRoleLabels.care_worker: 0, MainJobRoleLabels.registered_nurse: 1}),
        ("1-1001", CareHome.care_home, 5, date(2025, 2, 1), "1", None),
        ("1-1001", CareHome.care_home, 7, date(2025, 3, 1), "1", {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2}),
    ]
    # fmt: on

    # fmt: off
    estimated_filled_posts_when_multiple_establishments_on_the_same_date_rows = [
        ("1-1001", CareHome.care_home, 3, date(2025, 1, 1), "1"),
        ("1-1002", CareHome.care_home, 5, date(2025, 1, 1), "2"),
        ("1-1003", CareHome.care_home, 7, date(2025, 1, 1), "3"),
    ]
    aggregated_job_role_breakdown_when_multiple_establishments_on_the_same_date_rows = [
        ("1", date(2025, 1, 1), {MainJobRoleLabels.care_worker: 0, MainJobRoleLabels.registered_nurse: 1}),
        ("2", date(2025, 1, 1), {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2}),
    ]
    expected_merge_dataframse_when_multiple_establishments_on_the_same_date_rows = [
        ("1-1001", CareHome.care_home, 3, date(2025, 1, 1), "1", {MainJobRoleLabels.care_worker: 0, MainJobRoleLabels.registered_nurse: 1}),
        ("1-1002", CareHome.care_home, 5, date(2025, 1, 1), "2", {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2}),
        ("1-1003", CareHome.care_home, 7, date(2025, 1, 1), "3", None),
    ]
    # fmt: on

    # fmt: off
    estimated_filled_posts_when_establishments_do_not_match_rows = [
        ("1-1001", CareHome.care_home, 3, date(2025, 1, 1), "1"),
    ]
    aggregated_job_role_breakdown_when_establishments_do_not_match_rows = [
        ("2", date(2025, 1, 1), {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2}),
    ]
    expected_merge_dataframse_when_establishments_do_not_match_rows = [
        ("1-1001", CareHome.care_home, 3, date(2025, 1, 1), "1", None),
    ]
    # fmt: on

    temp_total_count_of_worker_records = "temp_total_count_of_worker_records"

    create_total_from_values_in_map_column_when_counts_are_longs_rows = [
        (
            "1-001",
            {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2},
        ),
        (
            "1-002",
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
            },
        ),
        (
            "1-003",
            None,
        ),
    ]
    expected_create_total_from_values_in_map_column_when_counts_are_longs_rows = [
        (
            "1-001",
            {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2},
            3,
        ),
        (
            "1-002",
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
            },
            None,
        ),
        (
            "1-003",
            None,
            None,
        ),
    ]

    create_total_from_values_in_map_column_when_counts_are_doubles_rows = [
        (
            "1-001",
            {
                MainJobRoleLabels.care_worker: 2.0,
                MainJobRoleLabels.registered_nurse: 3.0,
            },
        ),
    ]
    expected_create_total_from_values_in_map_column_when_counts_are_doubles_rows = [
        (
            "1-001",
            {
                MainJobRoleLabels.care_worker: 2.0,
                MainJobRoleLabels.registered_nurse: 3.0,
            },
            5.0,
        ),
    ]

    create_ratios_from_counts_when_counts_are_longs_rows = (
        expected_create_total_from_values_in_map_column_when_counts_are_longs_rows
    )
    expected_create_ratios_from_counts_when_counts_are_longs_rows = [
        (
            "1-001",
            {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2},
            3,
            {
                MainJobRoleLabels.care_worker: 0.333,
                MainJobRoleLabels.registered_nurse: 0.667,
            },
        ),
        (
            "1-002",
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
            },
            None,
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
            },
        ),
        (
            "1-003",
            None,
            None,
            None,
        ),
    ]

    create_ratios_from_counts_when_counts_are_doubles_rows = (
        expected_create_total_from_values_in_map_column_when_counts_are_doubles_rows
    )
    expected_create_ratios_from_counts_when_counts_are_doubles_rows = [
        (
            "1-001",
            {
                MainJobRoleLabels.care_worker: 2.0,
                MainJobRoleLabels.registered_nurse: 3.0,
            },
            5.0,
            {
                MainJobRoleLabels.care_worker: 0.4,
                MainJobRoleLabels.registered_nurse: 0.6,
            },
        ),
    ]

    # fmt: off
    create_estimate_filled_posts_by_job_role_map_column_when_all_job_role_ratios_populated_rows = [
        ("1-001",
         100.0,
        {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5})
    ]

    expected_create_estimate_filled_posts_by_job_role_map_column_when_all_job_role_ratios_populated_rows = [
        ("1-001",
         100.0,
        {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
        {MainJobRoleLabels.care_worker: 50.0, MainJobRoleLabels.registered_nurse: 50.0})
    ]
    # fmt: on

    create_estimate_filled_posts_by_job_role_map_column_when_job_role_ratio_column_is_null_rows = [
        ("1-001", 100.0, None)
    ]

    expected_create_estimate_filled_posts_by_job_role_map_column_when_job_role_ratio_column_is_null_rows = [
        ("1-001", 100.0, None, None)
    ]

    # fmt: off
    create_estimate_filled_posts_by_job_role_map_column_when_estimate_filled_posts_is_null_rows = [
        (
            "1-001",
            None,
            {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
        )
    ]

    expected_create_estimate_filled_posts_by_job_role_map_column_when_estimate_filled_posts_is_null_rows = [
        (
            "1-001",
            None,
            {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
            {MainJobRoleLabels.care_worker: None, MainJobRoleLabels.registered_nurse: None},
        )
    ]
    # fmt: on

    # fmt: off
    remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds_rows = [
        ("1-001",
         10.0,
         10.0,
         EstimateFilledPostsSource.ascwds_pir_merged,
         {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2}),
        ("1-002",
         None,
         20.0,
         EstimateFilledPostsSource.ascwds_pir_merged,
         {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2}),
        ("1-003",
         10.0,
         10.0,
         EstimateFilledPostsSource.care_home_model,
         {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2}),
    ]
    # fmt: on

    # fmt: off
    expected_remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds_rows = [
        ("1-001",
         10.0,
         10.0,
         EstimateFilledPostsSource.ascwds_pir_merged,
         {MainJobRoleLabels.care_worker: 1, MainJobRoleLabels.registered_nurse: 2}),
        ("1-002",
         None,
         20.0,
         EstimateFilledPostsSource.ascwds_pir_merged,
         None),
        ("1-003",
         10.0,
         10.0,
         EstimateFilledPostsSource.care_home_model,
         None),
    ]
    # fmt: on

    count_registered_manager_names_when_location_has_one_registered_manager_rows = [
        ("1-0000000001", date(2025, 1, 1), ["John Doe"])
    ]
    expected_count_registered_manager_names_when_location_has_one_registered_manager_rows = [
        ("1-0000000001", date(2025, 1, 1), ["John Doe"], 1)
    ]

    count_registered_manager_names_when_location_has_two_registered_managers_rows = [
        ("1-0000000001", date(2025, 1, 1), ["John Doe", "Jane Doe"])
    ]
    expected_count_registered_manager_names_when_location_has_two_registered_managers_rows = [
        ("1-0000000001", date(2025, 1, 1), ["John Doe", "Jane Doe"], 1)
    ]

    count_registered_manager_names_when_location_has_null_registered_manager_rows = [
        ("1-0000000001", date(2025, 1, 1), None)
    ]
    expected_count_registered_manager_names_when_location_has_null_registered_manager_rows = [
        ("1-0000000001", date(2025, 1, 1), None, 0)
    ]

    count_registered_manager_names_when_location_has_empty_list_rows = [
        ("1-0000000001", date(2025, 1, 1), [])
    ]
    expected_count_registered_manager_names_when_location_has_empty_list_rows = [
        ("1-0000000001", date(2025, 1, 1), [], 0)
    ]

    count_registered_manager_names_when_two_locations_have_different_number_of_registered_managers_rows = [
        ("1-0000000001", date(2025, 1, 1), ["John Doe"]),
        ("1-0000000002", date(2025, 1, 1), ["John Doe", "Jane Doe"]),
    ]
    expected_count_registered_manager_names_when_two_locations_have_different_number_of_registered_managers_rows = [
        ("1-0000000001", date(2025, 1, 1), ["John Doe"], 1),
        ("1-0000000002", date(2025, 1, 1), ["John Doe", "Jane Doe"], 1),
    ]

    count_registered_manager_names_when_a_location_has_different_number_of_registered_managers_at_different_import_dates_rows = [
        ("1-0000000001", date(2025, 1, 1), ["John Doe"]),
        ("1-0000000001", date(2025, 2, 1), ["John Doe", "Jane Doe"]),
    ]
    expected_count_registered_manager_names_when_a_location_has_different_number_of_registered_managers_at_different_import_dates_rows = [
        ("1-0000000001", date(2025, 1, 1), ["John Doe"], 1),
        ("1-0000000001", date(2025, 2, 1), ["John Doe", "Jane Doe"], 1),
    ]

    unpacked_mapped_column_with_one_record_data = [
        (
            "1-001",
            date(2025, 1, 1),
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
        )
    ]
    expected_unpacked_mapped_column_with_one_record_data = [
        (
            "1-001",
            date(2025, 1, 1),
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
            1.0,
            2.0,
            3.0,
            4.0,
        )
    ]

    unpacked_mapped_column_with_map_items_in_different_orders_data = [
        (
            "1-001",
            date(2025, 1, 1),
            {
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.senior_management: 4.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
            },
        ),
        (
            "1-002",
            date(2025, 1, 1),
            {
                MainJobRoleLabels.senior_management: 40.0,
                MainJobRoleLabels.registered_nurse: 20.0,
                MainJobRoleLabels.care_worker: 10.0,
                MainJobRoleLabels.senior_care_worker: 30.0,
            },
        ),
    ]
    expected_unpacked_mapped_column_with_map_items_in_different_orders_data = [
        (
            "1-001",
            date(2025, 1, 1),
            {
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.senior_management: 4.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
            },
            1.0,
            2.0,
            3.0,
            4.0,
        ),
        (
            "1-002",
            date(2025, 1, 1),
            {
                MainJobRoleLabels.senior_management: 40.0,
                MainJobRoleLabels.registered_nurse: 20.0,
                MainJobRoleLabels.care_worker: 10.0,
                MainJobRoleLabels.senior_care_worker: 30.0,
            },
            10.0,
            20.0,
            30.0,
            40.0,
        ),
    ]

    unpacked_mapped_column_with_null_values_data = [
        (
            "1-001",
            date(2025, 1, 1),
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
                MainJobRoleLabels.senior_care_worker: None,
                MainJobRoleLabels.senior_management: None,
            },
        ),
        (
            "1-002",
            date(2025, 2, 1),
            None,
        ),
    ]
    expected_unpacked_mapped_column_with_null_values_data = [
        (
            "1-001",
            date(2025, 1, 1),
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
                MainJobRoleLabels.senior_care_worker: None,
                MainJobRoleLabels.senior_management: None,
            },
            None,
            None,
            None,
            None,
        ),
        (
            "1-002",
            date(2025, 2, 1),
            None,
            None,
            None,
            None,
            None,
        ),
    ]

    # fmt: off
    non_rm_managerial_estimate_filled_posts_rows = [
        ("1-001", 9.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
        ("1-002", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
    ]

    expected_non_rm_managerial_estimate_filled_posts_rows = [
        ("1-001", 9.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
         20.0,
         {MainJobRoleLabels.senior_management: 0.45,
          MainJobRoleLabels.middle_management: 0.15,
          MainJobRoleLabels.first_line_manager: 0.05,
          MainJobRoleLabels.supervisor: 0.05,
          MainJobRoleLabels.other_managerial_staff: 0.05,
          MainJobRoleLabels.deputy_manager: 0.05,
          MainJobRoleLabels.team_leader: 0.05,
          MainJobRoleLabels.data_governance_manager: 0.05,
          MainJobRoleLabels.it_manager: 0.05,
          MainJobRoleLabels.it_service_desk_manager: 0.05},
         ),
        ("1-002", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
         0.0,
         {MainJobRoleLabels.senior_management: 0.1,
          MainJobRoleLabels.middle_management: 0.1,
          MainJobRoleLabels.first_line_manager: 0.1,
          MainJobRoleLabels.supervisor: 0.1,
          MainJobRoleLabels.other_managerial_staff: 0.1,
          MainJobRoleLabels.deputy_manager: 0.1,
          MainJobRoleLabels.team_leader: 0.1,
          MainJobRoleLabels.data_governance_manager: 0.1,
          MainJobRoleLabels.it_manager: 0.1,
          MainJobRoleLabels.it_service_desk_manager: 0.1}
         ),
    ]

    # fmt: on
    interpolate_job_role_ratios_data = [
        (
            "1000",
            1000,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 1.0,
                MainJobRoleLabels.senior_care_worker: 1.0,
                MainJobRoleLabels.senior_management: 1.0,
            },
        ),
        (
            "1000",
            1001,
            None,
        ),
        (
            "1000",
            1002,
            {
                MainJobRoleLabels.care_worker: 3.0,
                MainJobRoleLabels.registered_nurse: 3.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 3.0,
            },
        ),
    ]

    expected_interpolate_job_role_ratios_data = [
        (
            "1000",
            1000,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 1.0,
                MainJobRoleLabels.senior_care_worker: 1.0,
                MainJobRoleLabels.senior_management: 1.0,
            },
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 1.0,
                MainJobRoleLabels.senior_care_worker: 1.0,
                MainJobRoleLabels.senior_management: 1.0,
            },
        ),
        (
            "1000",
            1001,
            None,
            {
                MainJobRoleLabels.care_worker: 2.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 2.0,
                MainJobRoleLabels.senior_management: 2.0,
            },
        ),
        (
            "1000",
            1002,
            {
                MainJobRoleLabels.care_worker: 3.0,
                MainJobRoleLabels.registered_nurse: 3.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 3.0,
            },
            {
                MainJobRoleLabels.care_worker: 3.0,
                MainJobRoleLabels.registered_nurse: 3.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 3.0,
            },
        ),
    ]

    interpolate_job_role_ratios_with_null_records_which_cannot_be_interpolated_data = [
        (
            "1000",
            1000,
            None,
        ),
        (
            "1000",
            1001,
            {
                MainJobRoleLabels.care_worker: 2.0,
                MainJobRoleLabels.registered_nurse: 4.0,
                MainJobRoleLabels.senior_care_worker: 6.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
        ),
        (
            "1000",
            1002,
            None,
        ),
        (
            "1000",
            1003,
            {
                MainJobRoleLabels.care_worker: 0.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 4.0,
                MainJobRoleLabels.senior_management: 5.0,
            },
        ),
        (
            "1000",
            1004,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 1.0,
                MainJobRoleLabels.senior_care_worker: 1.0,
                MainJobRoleLabels.senior_management: 1.0,
            },
        ),
        (
            "1000",
            1005,
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 4.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 2.0,
            },
        ),
        (
            "1000",
            1006,
            None,
        ),
    ]

    expected_interpolate_job_role_ratios_with_null_records_which_cannot_be_interpolated_data = [
        (
            "1000",
            1000,
            None,
            None,
        ),
        (
            "1000",
            1001,
            {
                MainJobRoleLabels.care_worker: 2.0,
                MainJobRoleLabels.registered_nurse: 4.0,
                MainJobRoleLabels.senior_care_worker: 6.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
            {
                MainJobRoleLabels.care_worker: 2.0,
                MainJobRoleLabels.registered_nurse: 4.0,
                MainJobRoleLabels.senior_care_worker: 6.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
        ),
        (
            "1000",
            1002,
            None,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 3.0,
                MainJobRoleLabels.senior_care_worker: 5.0,
                MainJobRoleLabels.senior_management: 6.5,
            },
        ),
        (
            "1000",
            1003,
            {
                MainJobRoleLabels.care_worker: 0.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 4.0,
                MainJobRoleLabels.senior_management: 5.0,
            },
            {
                MainJobRoleLabels.care_worker: 0.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 4.0,
                MainJobRoleLabels.senior_management: 5.0,
            },
        ),
        (
            "1000",
            1004,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 1.0,
                MainJobRoleLabels.senior_care_worker: 1.0,
                MainJobRoleLabels.senior_management: 1.0,
            },
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 1.0,
                MainJobRoleLabels.senior_care_worker: 1.0,
                MainJobRoleLabels.senior_management: 1.0,
            },
        ),
        (
            "1000",
            1005,
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 4.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 2.0,
            },
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 4.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 2.0,
            },
        ),
        (
            "1000",
            1006,
            None,
            None,
        ),
    ]

    # fmt: off
    pivot_job_role_column_returns_expected_pivot_rows = [
        (1000, PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 1.0),
        (1000, PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 2.0),
        (1000, PrimaryServiceType.care_home_only, MainJobRoleLabels.senior_care_worker, 3.0),
        (1000, PrimaryServiceType.care_home_only, MainJobRoleLabels.senior_management, 4.0),
    ]
    expected_pivot_job_role_column_returns_expected_pivot_rows = [
        (1000,PrimaryServiceType.care_home_only, 1.0, 2.0, 3.0, 4.0),
    ]
    # fmt: on

    # fmt: off
    pivot_job_role_column_with_multiple_grouping_column_options_rows = [
        (1000, PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 1.0),
        (1000, PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 6.0),
        (1001, PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 2.0),
        (1001, PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 5.0),
        (1000, PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 3.0),
        (1000, PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 4.0),
        (1001, PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 4.0),
        (1001, PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 3.0),
        (1000, PrimaryServiceType.non_residential, MainJobRoleLabels.care_worker, 5.0),
        (1000, PrimaryServiceType.non_residential, MainJobRoleLabels.registered_nurse, 2.0),
        (1001, PrimaryServiceType.non_residential, MainJobRoleLabels.care_worker, 6.0),
        (1001, PrimaryServiceType.non_residential, MainJobRoleLabels.registered_nurse, 1.0),
        (1002, PrimaryServiceType.non_residential, MainJobRoleLabels.care_worker, None),
        (1002, PrimaryServiceType.non_residential, MainJobRoleLabels.registered_nurse, None),
    ]
    expected_pivot_job_role_column_with_multiple_grouping_column_options_rows = [
        (1000, PrimaryServiceType.care_home_with_nursing, 1.0, 6.0),
        (1000, PrimaryServiceType.care_home_only, 3.0, 4.0),
        (1000, PrimaryServiceType.non_residential, 5.0, 2.0),
        (1001, PrimaryServiceType.care_home_with_nursing, 2.0, 5.0),
        (1001, PrimaryServiceType.care_home_only, 4.0, 3.0),
        (1001, PrimaryServiceType.non_residential, 6.0, 1.0),
        (1002, PrimaryServiceType.non_residential, None, None),
    ]
    # fmt: on

    # fmt: off
    pivot_job_role_column_returns_first_aggregation_column_value_rows = [
        (1000, PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 1.0),
        (1000, PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 2.0),
        (1000, PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 3.0),
        (1000, PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 4.0),
        (1001, PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 5.0),
        (1001, PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 6.0),
        (1002, PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 7.0),
        (1002, PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 8.0),
    ]
    expected_pivot_job_role_column_returns_first_aggregation_column_value_rows = [
        (1000, PrimaryServiceType.care_home_with_nursing, 1.0, 3.0),
        (1001, PrimaryServiceType.care_home_only, 5.0, None),
        (1002, PrimaryServiceType.care_home_only, None, 7.0),
    ]
    # fmt: on

    convert_map_with_all_null_values_to_null_map_has_no_nulls_data = [
        (
            "1000",
            1,
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
                MainJobRoleLabels.senior_care_worker: None,
                MainJobRoleLabels.senior_management: None,
            },
        )
    ]

    expected_convert_map_with_all_null_values_to_null_map_has_no_nulls_data = [
        (
            "1000",
            1,
            None,
        )
    ]

    convert_map_with_all_null_values_to_null_map_has_all_nulls = [
        (
            "1001",
            1,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
        )
    ]

    expected_convert_map_with_all_null_values_to_null_map_has_all_nulls = [
        (
            "1001",
            1,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
        )
    ]

    convert_map_with_all_null_values_to_null_when_map_has_all_null_and_all_non_null_records_data = [
        (
            "2000",
            1,
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
                MainJobRoleLabels.senior_care_worker: None,
                MainJobRoleLabels.senior_management: None,
            },
        ),
        (
            "2001",
            1,
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 6.0,
                MainJobRoleLabels.senior_care_worker: 7.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
        ),
    ]

    expected_convert_map_with_all_null_values_to_null_when_map_has_all_null_and_all_non_null_records_data = [
        (
            "2000",
            1,
            None,
        ),
        (
            "2001",
            1,
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 6.0,
                MainJobRoleLabels.senior_care_worker: 7.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
        ),
    ]

    convert_map_with_all_null_values_to_null_when_map_has_some_nulls_data = [
        (
            "1002",
            1,
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: None,
            },
        )
    ]

    expected_convert_map_with_all_null_values_to_null_when_map_has_some_nulls_data = [
        (
            "1002",
            1,
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: None,
            },
        )
    ]

    estimate_and_cqc_registered_manager_rows = [
        ("1-001", 0.0, 1),
        ("1-002", 10.0, 1),
        ("1-003", None, 1),
        ("1-004", 10.0, None),
    ]
    expected_estimate_and_cqc_registered_manager_rows = [
        ("1-001", 0.0, 1, -1.0),
        ("1-002", 10.0, 1, 9.0),
        ("1-003", None, 1, None),
        ("1-004", 10.0, None, None),
    ]

    sum_job_group_counts_from_job_role_count_map_rows = [
        (
            "1-001",
            1000,
            {
                MainJobRoleLabels.care_worker: 1,
                MainJobRoleLabels.senior_care_worker: 1,
                MainJobRoleLabels.senior_management: 2,
                MainJobRoleLabels.first_line_manager: 2,
                MainJobRoleLabels.registered_nurse: 3,
                MainJobRoleLabels.social_worker: 3,
                MainJobRoleLabels.admin_staff: 4,
                MainJobRoleLabels.ancillary_staff: 4,
            },
        ),
        (
            "1-001",
            1001,
            {
                MainJobRoleLabels.care_worker: 10,
                MainJobRoleLabels.senior_care_worker: 10,
                MainJobRoleLabels.senior_management: 20,
                MainJobRoleLabels.first_line_manager: 20,
                MainJobRoleLabels.registered_nurse: 30,
                MainJobRoleLabels.social_worker: 30,
                MainJobRoleLabels.admin_staff: 40,
                MainJobRoleLabels.ancillary_staff: 40,
            },
        ),
        (
            "1-002",
            1000,
            {
                MainJobRoleLabels.care_worker: 0,
                MainJobRoleLabels.senior_care_worker: 0,
                MainJobRoleLabels.registered_nurse: None,
            },
        ),
        (
            "1-003",
            1000,
            None,
        ),
    ]
    expected_sum_job_group_counts_from_job_role_count_map_rows = [
        (
            "1-001",
            1000,
            {
                MainJobRoleLabels.care_worker: 1,
                MainJobRoleLabels.senior_care_worker: 1,
                MainJobRoleLabels.senior_management: 2,
                MainJobRoleLabels.first_line_manager: 2,
                MainJobRoleLabels.registered_nurse: 3,
                MainJobRoleLabels.social_worker: 3,
                MainJobRoleLabels.admin_staff: 4,
                MainJobRoleLabels.ancillary_staff: 4,
            },
            {
                JobGroupLabels.direct_care: 2,
                JobGroupLabels.managers: 4,
                JobGroupLabels.regulated_professions: 6,
                JobGroupLabels.other: 8,
            },
        ),
        (
            "1-001",
            1001,
            {
                MainJobRoleLabels.care_worker: 10,
                MainJobRoleLabels.senior_care_worker: 10,
                MainJobRoleLabels.senior_management: 20,
                MainJobRoleLabels.first_line_manager: 20,
                MainJobRoleLabels.registered_nurse: 30,
                MainJobRoleLabels.social_worker: 30,
                MainJobRoleLabels.admin_staff: 40,
                MainJobRoleLabels.ancillary_staff: 40,
            },
            {
                JobGroupLabels.direct_care: 20,
                JobGroupLabels.managers: 40,
                JobGroupLabels.regulated_professions: 60,
                JobGroupLabels.other: 80,
            },
        ),
        (
            "1-002",
            1000,
            {
                MainJobRoleLabels.care_worker: 0,
                MainJobRoleLabels.senior_care_worker: 0,
                MainJobRoleLabels.registered_nurse: None,
            },
            {
                JobGroupLabels.direct_care: 0,
                JobGroupLabels.managers: 0,
                JobGroupLabels.regulated_professions: 0,
                JobGroupLabels.other: 0,
            },
        ),
        (
            "1-003",
            1000,
            None,
            None,
        ),
    ]
    sum_job_group_counts_from_job_role_count_map_for_patching_create_map_column_rows = [
        (
            "1-001",
            1001,
            {
                JobGroupLabels.direct_care: 20,
                JobGroupLabels.managers: 40,
                JobGroupLabels.regulated_professions: 60,
                JobGroupLabels.other: 80,
            },
        ),
        (
            "1-002",
            1000,
            {
                JobGroupLabels.direct_care: 0,
                JobGroupLabels.managers: 0,
                JobGroupLabels.regulated_professions: 0,
                JobGroupLabels.other: 0,
            },
        ),
        (
            "1-001",
            1000,
            {
                JobGroupLabels.direct_care: 2,
                JobGroupLabels.managers: 4,
                JobGroupLabels.regulated_professions: 6,
                JobGroupLabels.other: 8,
            },
        ),
    ]

    filter_ascwds_job_role_map_when_dc_or_manregprof_1_or_more_rows = [
        (
            "1-001",
            None,
            None,
            None,
        ),
        (
            "1-002",
            0,
            None,
            None,
        ),
        (
            "1-003",
            1,
            {
                MainJobRoleLabels.admin_staff: 0,
            },
            {
                JobGroupLabels.direct_care: 0,
                JobGroupLabels.managers: 0,
                JobGroupLabels.regulated_professions: 0,
                JobGroupLabels.other: 1,
            },
        ),
        (
            "1-004",
            1,
            {
                MainJobRoleLabels.care_worker: 1,
            },
            {
                JobGroupLabels.direct_care: 1,
                JobGroupLabels.managers: 0,
                JobGroupLabels.regulated_professions: 0,
                JobGroupLabels.other: 0,
            },
        ),
        (
            "1-005",
            1,
            {
                MainJobRoleLabels.senior_management: 1,
            },
            {
                JobGroupLabels.direct_care: 0,
                JobGroupLabels.managers: 1,
                JobGroupLabels.regulated_professions: 0,
                JobGroupLabels.other: 0,
            },
        ),
        (
            "1-006",
            1,
            {
                MainJobRoleLabels.social_worker: 1,
            },
            {
                JobGroupLabels.direct_care: 0,
                JobGroupLabels.managers: 0,
                JobGroupLabels.regulated_professions: 1,
                JobGroupLabels.other: 0,
            },
        ),
    ]
    expected_filter_ascwds_job_role_map_when_dc_or_manregprof_1_or_more_rows = [
        (
            "1-001",
            None,
            None,
            None,
            None,
        ),
        (
            "1-002",
            0,
            None,
            None,
            None,
        ),
        (
            "1-003",
            1,
            {
                MainJobRoleLabels.admin_staff: 0,
            },
            {
                JobGroupLabels.direct_care: 0,
                JobGroupLabels.managers: 0,
                JobGroupLabels.regulated_professions: 0,
                JobGroupLabels.other: 1,
            },
            None,
        ),
        (
            "1-004",
            1,
            {
                MainJobRoleLabels.care_worker: 1,
            },
            {
                JobGroupLabels.direct_care: 1,
                JobGroupLabels.managers: 0,
                JobGroupLabels.regulated_professions: 0,
                JobGroupLabels.other: 0,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-005",
            1,
            {
                MainJobRoleLabels.senior_management: 1,
            },
            {
                JobGroupLabels.direct_care: 0,
                JobGroupLabels.managers: 1,
                JobGroupLabels.regulated_professions: 0,
                JobGroupLabels.other: 0,
            },
            {
                MainJobRoleLabels.senior_management: 1,
            },
        ),
        (
            "1-006",
            1,
            {
                MainJobRoleLabels.social_worker: 1,
            },
            {
                JobGroupLabels.direct_care: 0,
                JobGroupLabels.managers: 0,
                JobGroupLabels.regulated_professions: 1,
                JobGroupLabels.other: 0,
            },
            {
                MainJobRoleLabels.social_worker: 1,
            },
        ),
    ]

    filter_ascwds_job_role_count_map_when_job_group_ratios_outside_percentile_boundaries_rows = [
        (
            "1-001",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 10.0,
                JobGroupLabels.managers: 0.9,
                JobGroupLabels.regulated_professions: 0.9,
                JobGroupLabels.other: 0.9,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-002",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 0.9,
                JobGroupLabels.managers: 10.0,
                JobGroupLabels.regulated_professions: 0.8,
                JobGroupLabels.other: 0.8,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-003",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 0.8,
                JobGroupLabels.managers: 0.8,
                JobGroupLabels.regulated_professions: 10.0,
                JobGroupLabels.other: 0.7,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-004",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 0.7,
                JobGroupLabels.managers: 0.7,
                JobGroupLabels.regulated_professions: 0.7,
                JobGroupLabels.other: 10.0,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-005",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 0.0,
                JobGroupLabels.managers: 0.6,
                JobGroupLabels.regulated_professions: 0.6,
                JobGroupLabels.other: 0.6,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-006",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 0.6,
                JobGroupLabels.managers: 0.5,
                JobGroupLabels.regulated_professions: 0.5,
                JobGroupLabels.other: 0.5,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-007",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 10.0,
                JobGroupLabels.managers: 0.9,
                JobGroupLabels.regulated_professions: 0.9,
                JobGroupLabels.other: 0.9,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-008",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 0.9,
                JobGroupLabels.managers: 10.0,
                JobGroupLabels.regulated_professions: 0.8,
                JobGroupLabels.other: 0.8,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-009",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 0.8,
                JobGroupLabels.managers: 0.8,
                JobGroupLabels.regulated_professions: 10.0,
                JobGroupLabels.other: 0.7,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-010",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 0.7,
                JobGroupLabels.managers: 0.7,
                JobGroupLabels.regulated_professions: 0.7,
                JobGroupLabels.other: 10.0,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-011",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 0.0,
                JobGroupLabels.managers: 0.6,
                JobGroupLabels.regulated_professions: 0.6,
                JobGroupLabels.other: 0.6,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-012",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 0.6,
                JobGroupLabels.managers: 0.5,
                JobGroupLabels.regulated_professions: 0.5,
                JobGroupLabels.other: 0.5,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
    ]
    expected_filter_ascwds_job_role_count_map_when_job_group_ratios_outside_percentile_boundaries_rows = [
        (
            "1-001",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 10.0,
                JobGroupLabels.managers: 0.9,
                JobGroupLabels.regulated_professions: 0.9,
                JobGroupLabels.other: 0.9,
            },
            None,
        ),
        (
            "1-002",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 0.9,
                JobGroupLabels.managers: 10.0,
                JobGroupLabels.regulated_professions: 0.8,
                JobGroupLabels.other: 0.8,
            },
            None,
        ),
        (
            "1-003",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 0.8,
                JobGroupLabels.managers: 0.8,
                JobGroupLabels.regulated_professions: 10.0,
                JobGroupLabels.other: 0.7,
            },
            None,
        ),
        (
            "1-004",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 0.7,
                JobGroupLabels.managers: 0.7,
                JobGroupLabels.regulated_professions: 0.7,
                JobGroupLabels.other: 10.0,
            },
            None,
        ),
        (
            "1-005",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 0.0,
                JobGroupLabels.managers: 0.6,
                JobGroupLabels.regulated_professions: 0.6,
                JobGroupLabels.other: 0.6,
            },
            None,
        ),
        (
            "1-006",
            PrimaryServiceType.care_home_with_nursing,
            {
                JobGroupLabels.direct_care: 0.6,
                JobGroupLabels.managers: 0.5,
                JobGroupLabels.regulated_professions: 0.5,
                JobGroupLabels.other: 0.5,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
        (
            "1-007",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 10.0,
                JobGroupLabels.managers: 0.9,
                JobGroupLabels.regulated_professions: 0.9,
                JobGroupLabels.other: 0.9,
            },
            None,
        ),
        (
            "1-008",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 0.9,
                JobGroupLabels.managers: 10.0,
                JobGroupLabels.regulated_professions: 0.8,
                JobGroupLabels.other: 0.8,
            },
            None,
        ),
        (
            "1-009",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 0.8,
                JobGroupLabels.managers: 0.8,
                JobGroupLabels.regulated_professions: 10.0,
                JobGroupLabels.other: 0.7,
            },
            None,
        ),
        (
            "1-010",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 0.7,
                JobGroupLabels.managers: 0.7,
                JobGroupLabels.regulated_professions: 0.7,
                JobGroupLabels.other: 10.0,
            },
            None,
        ),
        (
            "1-011",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 0.0,
                JobGroupLabels.managers: 0.6,
                JobGroupLabels.regulated_professions: 0.6,
                JobGroupLabels.other: 0.6,
            },
            None,
        ),
        (
            "1-012",
            PrimaryServiceType.care_home_only,
            {
                JobGroupLabels.direct_care: 0.6,
                JobGroupLabels.managers: 0.5,
                JobGroupLabels.regulated_professions: 0.5,
                JobGroupLabels.other: 0.5,
            },
            {
                MainJobRoleLabels.care_worker: 1,
            },
        ),
    ]

    transform_imputed_job_role_ratios_to_counts_rows = [
        (
            "1-001",
            100.0,
            {
                MainJobRoleLabels.care_worker: 0.10,
                MainJobRoleLabels.registered_nurse: 0.20,
                MainJobRoleLabels.senior_care_worker: 0.30,
                MainJobRoleLabels.senior_management: 0.40,
            },
        ),
        (
            "1-002",
            None,
            {
                MainJobRoleLabels.care_worker: 0.10,
                MainJobRoleLabels.registered_nurse: 0.20,
                MainJobRoleLabels.senior_care_worker: 0.30,
                MainJobRoleLabels.senior_management: 0.40,
            },
        ),
        (
            "1-003",
            100.0,
            None,
        ),
    ]
    expected_transform_imputed_job_role_ratios_to_counts_rows = [
        (
            "1-001",
            100.0,
            {
                MainJobRoleLabels.care_worker: 0.10,
                MainJobRoleLabels.registered_nurse: 0.20,
                MainJobRoleLabels.senior_care_worker: 0.30,
                MainJobRoleLabels.senior_management: 0.40,
            },
            {
                MainJobRoleLabels.care_worker: 10.0,
                MainJobRoleLabels.registered_nurse: 20.0,
                MainJobRoleLabels.senior_care_worker: 30.0,
                MainJobRoleLabels.senior_management: 40.0,
            },
        ),
        (
            "1-002",
            None,
            {
                MainJobRoleLabels.care_worker: 0.10,
                MainJobRoleLabels.registered_nurse: 0.20,
                MainJobRoleLabels.senior_care_worker: 0.30,
                MainJobRoleLabels.senior_management: 0.40,
            },
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
                MainJobRoleLabels.senior_care_worker: None,
                MainJobRoleLabels.senior_management: None,
            },
        ),
        (
            "1-003",
            100.0,
            None,
            None,
        ),
    ]

    # fmt: off
    job_role_ratios_extrapolation_rows = [
        (
            "1-001",
            1000000200,
            None
        ),
        (
            "1-001",
            1000000300,
            {
                MainJobRoleLabels.care_worker: 0.1,
                MainJobRoleLabels.registered_nurse: 0.1,
            },
        ),
        (
            "1-001",
            1000000400,
            {
                MainJobRoleLabels.care_worker: 0.2,
                MainJobRoleLabels.registered_nurse: 0.2,
            }
        ),
        (
            "1-001",
            1000000500,
            {
                MainJobRoleLabels.care_worker: 0.3,
                MainJobRoleLabels.registered_nurse: 0.3,
            },
        ),
        (
            "1-001",
            1000000600,
            None,
        ),
        (
            "1-002",
            1000000200,
            {
                MainJobRoleLabels.care_worker: 0.1,
                MainJobRoleLabels.registered_nurse: 0.1,
            }
        ),
        (
            "1-002",
            1000000300,
            None
        ),
        (
            "1-002",
            1000000400,
            {
                MainJobRoleLabels.care_worker: 0.2,
                MainJobRoleLabels.registered_nurse: 0.2,
            },
        ),
        ("1-003", 1000000200, None),
        ("1-003", 1000000300, None),
        ("1-003", 1000000400, None),
        ("1-003", 1000000500, None),
    ]
    expected_job_role_ratios_extrapolation_rows = [
        (
            "1-001",
            1000000200,
            None,
            {
                MainJobRoleLabels.care_worker: 0.1,
                MainJobRoleLabels.registered_nurse: 0.1,
            },
        ),
        (
            "1-001",
            1000000300,
            {
                MainJobRoleLabels.care_worker: 0.1,
                MainJobRoleLabels.registered_nurse: 0.1,
            },
            {
                MainJobRoleLabels.care_worker: 0.1,
                MainJobRoleLabels.registered_nurse: 0.1,
            },
        ),
        (
            "1-001",
            1000000400,
            {
                MainJobRoleLabels.care_worker: 0.2,
                MainJobRoleLabels.registered_nurse: 0.2,
            },
            {
                MainJobRoleLabels.care_worker: 0.2,
                MainJobRoleLabels.registered_nurse: 0.2,
            },
        ),
        (
            "1-001",
            1000000500,
            {
                MainJobRoleLabels.care_worker: 0.3,
                MainJobRoleLabels.registered_nurse: 0.3,
            },
            {
                MainJobRoleLabels.care_worker: 0.3,
                MainJobRoleLabels.registered_nurse: 0.3,
            },
        ),
        (
            "1-001",
            1000000600,
            None,
            {
                MainJobRoleLabels.care_worker: 0.3,
                MainJobRoleLabels.registered_nurse: 0.3,
            },
        ),
        (
            "1-002",
            1000000200,
            {
                MainJobRoleLabels.care_worker: 0.1,
                MainJobRoleLabels.registered_nurse: 0.1,
            },
            {
                MainJobRoleLabels.care_worker: 0.1,
                MainJobRoleLabels.registered_nurse: 0.1,
            },
        ),
        (
            "1-002",
            1000000300,
            None,
            None,
        ),
        (
            "1-002",
            1000000400,
            {
                MainJobRoleLabels.care_worker: 0.2,
                MainJobRoleLabels.registered_nurse: 0.2,
            },
            {
                MainJobRoleLabels.care_worker: 0.2,
                MainJobRoleLabels.registered_nurse: 0.2,
            },
        ),
        ("1-003", 1000000200, None, None),
        ("1-003", 1000000300, None, None),
        ("1-003", 1000000400, None, None),
        ("1-003", 1000000500, None, None),
    ]
    # fmt: on

    recalculate_managerial_filled_posts_rows = [
        (
            "1-001",
            0.0,
            1.0,
            4.0,
            5.0,
            {
                "managerial_role_1": 0.0,
                "managerial_role_2": 0.1,
                "managerial_role_3": 0.4,
                "managerial_role_4": 0.5,
            },
            1.0,
        ),
        (
            "1-002",
            1.0,
            1.0,
            1.0,
            1.0,
            {
                "managerial_role_1": 0.25,
                "managerial_role_2": 0.25,
                "managerial_role_3": 0.25,
                "managerial_role_4": 0.25,
            },
            0.0,
        ),
        (
            "1-003",
            0.0,
            0.0,
            0.0,
            0.0,
            {
                "managerial_role_1": 0.25,
                "managerial_role_2": 0.25,
                "managerial_role_3": 0.25,
                "managerial_role_4": 0.25,
            },
            -1.0,
        ),
        (
            "1-004",
            0.0,
            1.0,
            4.0,
            5.0,
            {
                "managerial_role_1": 0.0,
                "managerial_role_2": 0.1,
                "managerial_role_3": 0.4,
                "managerial_role_4": 0.5,
            },
            -1.0,
        ),
    ]
    expected_recalculate_managerial_filled_posts_rows = [
        (
            "1-001",
            0.0,
            1.1,
            4.4,
            5.5,
            {
                "managerial_role_1": 0.0,
                "managerial_role_2": 0.1,
                "managerial_role_3": 0.4,
                "managerial_role_4": 0.5,
            },
            1.0,
        ),
        (
            "1-002",
            1.0,
            1.0,
            1.0,
            1.0,
            {
                "managerial_role_1": 0.25,
                "managerial_role_2": 0.25,
                "managerial_role_3": 0.25,
                "managerial_role_4": 0.25,
            },
            0.0,
        ),
        (
            "1-003",
            0.0,
            0.0,
            0.0,
            0.0,
            {
                "managerial_role_1": 0.25,
                "managerial_role_2": 0.25,
                "managerial_role_3": 0.25,
                "managerial_role_4": 0.25,
            },
            -1.0,
        ),
        (
            "1-004",
            0.0,
            0.9,
            3.6,
            4.5,
            {
                "managerial_role_1": 0.0,
                "managerial_role_2": 0.1,
                "managerial_role_3": 0.4,
                "managerial_role_4": 0.5,
            },
            -1.0,
        ),
    ]

    recalculate_total_filled_posts_rows = [
        ("1-001", 0.0, 0.0, 0.0, 0.0),
        ("1-002", 2.0, 1.0, 2.0, 1.0),
    ]
    expected_recalculate_total_filled_posts_rows = [
        ("1-001", 0.0, 0.0, 0.0, 0.0, 0.0),
        ("1-002", 2.0, 1.0, 2.0, 1.0, 6.0),
    ]

    combine_interpolated_and_extrapolated_job_role_ratios_rows = [
        (
            "1-001",
            {
                MainJobRoleLabels.care_worker: 0.1,
                MainJobRoleLabels.registered_nurse: 0.1,
            },
            {
                MainJobRoleLabels.care_worker: 0.2,
                MainJobRoleLabels.registered_nurse: 0.2,
            },
            {
                MainJobRoleLabels.care_worker: 0.3,
                MainJobRoleLabels.registered_nurse: 0.3,
            },
        ),
        (
            "1-002",
            None,
            {
                MainJobRoleLabels.care_worker: 0.2,
                MainJobRoleLabels.registered_nurse: 0.2,
            },
            {
                MainJobRoleLabels.care_worker: 0.3,
                MainJobRoleLabels.registered_nurse: 0.3,
            },
        ),
        (
            "1-003",
            None,
            None,
            {
                MainJobRoleLabels.care_worker: 0.3,
                MainJobRoleLabels.registered_nurse: 0.3,
            },
        ),
        (
            "1-004",
            None,
            None,
            None,
        ),
    ]
    expected_combine_interpolated_and_extrapolated_job_role_ratios_rows = [
        (
            "1-001",
            {
                MainJobRoleLabels.care_worker: 0.1,
                MainJobRoleLabels.registered_nurse: 0.1,
            },
            {
                MainJobRoleLabels.care_worker: 0.2,
                MainJobRoleLabels.registered_nurse: 0.2,
            },
            {
                MainJobRoleLabels.care_worker: 0.3,
                MainJobRoleLabels.registered_nurse: 0.3,
            },
            {
                MainJobRoleLabels.care_worker: 0.1,
                MainJobRoleLabels.registered_nurse: 0.1,
            },
        ),
        (
            "1-002",
            None,
            {
                MainJobRoleLabels.care_worker: 0.2,
                MainJobRoleLabels.registered_nurse: 0.2,
            },
            {
                MainJobRoleLabels.care_worker: 0.3,
                MainJobRoleLabels.registered_nurse: 0.3,
            },
            {
                MainJobRoleLabels.care_worker: 0.2,
                MainJobRoleLabels.registered_nurse: 0.2,
            },
        ),
        (
            "1-003",
            None,
            None,
            {
                MainJobRoleLabels.care_worker: 0.3,
                MainJobRoleLabels.registered_nurse: 0.3,
            },
            {
                MainJobRoleLabels.care_worker: 0.3,
                MainJobRoleLabels.registered_nurse: 0.3,
            },
        ),
        (
            "1-004",
            None,
            None,
            None,
            None,
        ),
    ]

    overwrite_registered_manager_estimate_with_cqc_count_rows = [
        (10.0, 1),
        (10.0, 0),
    ]
    expected_overwrite_registered_manager_estimate_with_cqc_count_rows = [
        (1.0, 1),
        (0.0, 0),
    ]

    calculate_difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles_rows = [
        (10.0, 10.0),
        (10.0, 9.0),
        (9.0, 10.0),
    ]
    expected_calculate_difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles_rows = [
        (10.0, 10.0, 0.0),
        (10.0, 9.0, -1.0),
        (9.0, 10.0, 1.0),
    ]


@dataclass
class EstimateJobRolesPrimaryServiceRollingSumData:
    list_of_job_roles_for_tests = [
        MainJobRoleLabels.care_worker,
        MainJobRoleLabels.registered_nurse,
        MainJobRoleLabels.senior_care_worker,
        MainJobRoleLabels.senior_management,
    ]

    add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled_data = [
        (
            0,
            PrimaryServiceType.care_home_with_nursing,
            MainJobRoleLabels.care_worker,
            1.0,
        ),
        (
            86401,
            PrimaryServiceType.care_home_with_nursing,
            MainJobRoleLabels.care_worker,
            1.0,
        ),
        (
            86402,
            PrimaryServiceType.care_home_with_nursing,
            MainJobRoleLabels.care_worker,
            1.0,
        ),
        (
            86403,
            PrimaryServiceType.care_home_with_nursing,
            MainJobRoleLabels.care_worker,
            None,
        ),
        (
            86404,
            PrimaryServiceType.care_home_with_nursing,
            MainJobRoleLabels.senior_management,
            1.0,
        ),
        (
            0,
            PrimaryServiceType.care_home_only,
            MainJobRoleLabels.care_worker,
            None,
        ),
        (
            86401,
            PrimaryServiceType.care_home_only,
            MainJobRoleLabels.care_worker,
            1.0,
        ),
        (
            0,
            PrimaryServiceType.non_residential,
            MainJobRoleLabels.care_worker,
            10.0,
        ),
        (
            86400,
            PrimaryServiceType.non_residential,
            MainJobRoleLabels.care_worker,
            2.0,
        ),
        (
            86401,
            PrimaryServiceType.non_residential,
            MainJobRoleLabels.care_worker,
            8.0,
        ),
    ]

    expected_add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled_data = [
        (
            0,
            PrimaryServiceType.care_home_with_nursing,
            MainJobRoleLabels.care_worker,
            1.0,
            1.0,
        ),
        (
            86401,
            PrimaryServiceType.care_home_with_nursing,
            MainJobRoleLabels.care_worker,
            1.0,
            1.0,
        ),
        (
            86402,
            PrimaryServiceType.care_home_with_nursing,
            MainJobRoleLabels.care_worker,
            1.0,
            2.0,
        ),
        (
            86403,
            PrimaryServiceType.care_home_with_nursing,
            MainJobRoleLabels.care_worker,
            None,
            2.0,
        ),
        (
            86404,
            PrimaryServiceType.care_home_with_nursing,
            MainJobRoleLabels.senior_management,
            1.0,
            1.0,
        ),
        (
            0,
            PrimaryServiceType.care_home_only,
            MainJobRoleLabels.care_worker,
            None,
            None,
        ),
        (
            86401,
            PrimaryServiceType.care_home_only,
            MainJobRoleLabels.care_worker,
            1.0,
            1.0,
        ),
        (
            0,
            PrimaryServiceType.non_residential,
            MainJobRoleLabels.care_worker,
            10.0,
            10.0,
        ),
        (
            86400,
            PrimaryServiceType.non_residential,
            MainJobRoleLabels.care_worker,
            2.0,
            12.0,
        ),
        (
            86401,
            PrimaryServiceType.non_residential,
            MainJobRoleLabels.care_worker,
            8.0,
            10.0,
        ),
    ]

    primary_service_rolling_sum_when_one_primary_service_present_rows = [
        (
            "1000",
            1,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
        ),
        (
            "1000",
            2,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
                MainJobRoleLabels.senior_care_worker: None,
                MainJobRoleLabels.senior_management: None,
            },
        ),
        (
            "1000",
            3,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 6.0,
                MainJobRoleLabels.senior_care_worker: 7.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
        ),
    ]
    expected_primary_service_rolling_sum_when_one_primary_service_present_rows = [
        (
            "1000",
            1,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
        ),
        (
            "1000",
            2,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: None,
                MainJobRoleLabels.registered_nurse: None,
                MainJobRoleLabels.senior_care_worker: None,
                MainJobRoleLabels.senior_management: None,
            },
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
        ),
        (
            "1000",
            3,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 6.0,
                MainJobRoleLabels.senior_care_worker: 7.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
            {
                MainJobRoleLabels.care_worker: 6.0,
                MainJobRoleLabels.registered_nurse: 8.0,
                MainJobRoleLabels.senior_care_worker: 10.0,
                MainJobRoleLabels.senior_management: 12.0,
            },
        ),
    ]

    primary_service_rolling_sum_when_multiple_primary_services_present_rows = [
        (
            "1000",
            1,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
        ),
        (
            "1000",
            2,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 6.0,
                MainJobRoleLabels.senior_care_worker: 7.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
        ),
        (
            "1000",
            1,
            PrimaryServiceType.care_home_only,
            {
                MainJobRoleLabels.care_worker: 11.0,
                MainJobRoleLabels.registered_nurse: 12.0,
                MainJobRoleLabels.senior_care_worker: 13.0,
                MainJobRoleLabels.senior_management: 14.0,
            },
        ),
        (
            "1000",
            2,
            PrimaryServiceType.care_home_only,
            {
                MainJobRoleLabels.care_worker: 15.0,
                MainJobRoleLabels.registered_nurse: 16.0,
                MainJobRoleLabels.senior_care_worker: 17.0,
                MainJobRoleLabels.senior_management: 18.0,
            },
        ),
    ]
    expected_primary_service_rolling_sum_when_multiple_primary_services_present_rows = [
        (
            "1000",
            1,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
        ),
        (
            "1000",
            2,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 6.0,
                MainJobRoleLabels.senior_care_worker: 7.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
            {
                MainJobRoleLabels.care_worker: 6.0,
                MainJobRoleLabels.registered_nurse: 8.0,
                MainJobRoleLabels.senior_care_worker: 10.0,
                MainJobRoleLabels.senior_management: 12.0,
            },
        ),
        (
            "1000",
            1,
            PrimaryServiceType.care_home_only,
            {
                MainJobRoleLabels.care_worker: 11.0,
                MainJobRoleLabels.registered_nurse: 12.0,
                MainJobRoleLabels.senior_care_worker: 13.0,
                MainJobRoleLabels.senior_management: 14.0,
            },
            {
                MainJobRoleLabels.care_worker: 11.0,
                MainJobRoleLabels.registered_nurse: 12.0,
                MainJobRoleLabels.senior_care_worker: 13.0,
                MainJobRoleLabels.senior_management: 14.0,
            },
        ),
        (
            "1000",
            2,
            PrimaryServiceType.care_home_only,
            {
                MainJobRoleLabels.care_worker: 15.0,
                MainJobRoleLabels.registered_nurse: 16.0,
                MainJobRoleLabels.senior_care_worker: 17.0,
                MainJobRoleLabels.senior_management: 18.0,
            },
            {
                MainJobRoleLabels.care_worker: 26.0,
                MainJobRoleLabels.registered_nurse: 28.0,
                MainJobRoleLabels.senior_care_worker: 30.0,
                MainJobRoleLabels.senior_management: 32.0,
            },
        ),
    ]

    primary_service_rolling_sum_when_days_not_within_rolling_window_rows = [
        (
            "1000",
            1704067200,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
        ),
        (
            "1000",
            1720137600,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 6.0,
                MainJobRoleLabels.senior_care_worker: 7.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
        ),
    ]
    expected_primary_service_rolling_sum_when_days_not_within_rolling_window_rows = [
        (
            "1000",
            1704067200,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
            {
                MainJobRoleLabels.care_worker: 1.0,
                MainJobRoleLabels.registered_nurse: 2.0,
                MainJobRoleLabels.senior_care_worker: 3.0,
                MainJobRoleLabels.senior_management: 4.0,
            },
        ),
        (
            "1000",
            1720137600,
            PrimaryServiceType.care_home_with_nursing,
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 6.0,
                MainJobRoleLabels.senior_care_worker: 7.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
            {
                MainJobRoleLabels.care_worker: 5.0,
                MainJobRoleLabels.registered_nurse: 6.0,
                MainJobRoleLabels.senior_care_worker: 7.0,
                MainJobRoleLabels.senior_management: 8.0,
            },
        ),
    ]
