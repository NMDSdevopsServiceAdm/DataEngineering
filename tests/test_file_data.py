from dataclasses import dataclass
from datetime import date

from utils.diagnostics_utils.diagnostics_meta_data import (
    Variables as Values,
)

from utils.column_names.cleaned_data_files.cqc_provider_data_columns_values import (
    CqcProviderCleanedValues as CQCPValues,
)


@dataclass
class CreateJobEstimatesDiagnosticsData:
    # fmt: off
    estimate_jobs_rows = [
        ("location_1", "20230401", 40.0, 40.0, Values.care_home_with_nursing, 60.9, 23.4, 45.1, None, None, 40.0, 45,),
    ]
    capacity_tracker_care_home_rows = [
        ("location_1", 8.0, 12.0, 15.0, 1.0, 3.0, 2.0),
    ]
    capacity_tracker_non_residential_rows = [
        ("location_2", 67.0),
    ]
    prepare_capacity_tracker_care_home_rows = [
        ("location_1", Values.care_home_with_nursing, 8.0, 12.0, 15.0, 1.0, 3.0, 2.0, None,),
        ("location_2", Values.non_residential, None, None, None, None, None, None, 30.0,),
    ]
    prepare_capacity_tracker_non_residential_rows = [
        ("location_1", Values.care_home_with_nursing, 8.0, 12.0, 15.0, 1.0, 3.0, 2.0, None,),
        ("location_2", Values.non_residential, None, None, None, None, None, None, 75.0,),
    ]
    calculate_residuals_rows = [
        ("location_2", 40.0, 40.0, Values.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, 40, None, 40.0,),
        ("location_3", 40.0, 40.0, Values.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, 45, 41.0, None,),
        ("location_4", None, None, Values.non_residential, 60.9, 23.4, None, None, None, 60.0, 45, None, None,),
        ("location_5", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, 50.0, None,),
    ]
    run_residuals_rows = [
        ("location_1", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, None, None,),
        ("location_2", 40.0, 40.0, Values.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, None, None, 40.0,),
        ("location_3", 40.0, 40.0, Values.care_home_without_nursing, 60.9, 23.4, 45.1, None, None, 40.0, 45, 41.0, None,),
        ("location_4", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, 45, None, None,),
        ("location_5", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, 50.0, None,),
        ("location_6", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, 45, 50.0, None,),
        ("location_7", None, None, Values.non_residential, 60.9, None, None, None, 40.0, 60.9, 45, None, None,),
        ("location_8", None, None, Values.non_residential, 60.9, None, None, None, 40.0, 60.9, None, None, 40.0,),
        ("location_9", None, None, Values.non_residential, 60.9, None, None, None, 40.0, 60.9, 45, None, 40.0,),
        ("location_10", None, None, Values.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, None, None,),
    ]
    residuals_rows = [
        ("location_1", 0.0, 0.0,),
        ("location_2", -1.0, 0.0,),
        ("location_3", 3.0, 0.0,),
        ("location_4", None, 0.0,),
        ("location_5", 10.5, 0.0,),
        ("location_6", -2.5, 0.0,),
        ("location_7", None, None,),
        ("location_8", None, None,),
    ]
    add_timestamps_rows = [
        ("location_1", 0.0, 0.0,),
        ("location_2", -1.0, 0.0,),
    ]
    # fmt: on
    description_of_change: str = "test"
    run_timestamp: str = "12/24/2018, 04:59:31"


@dataclass
class CalculatePaRatioData:
    calculate_pa_ratio_rows = [
        (2021, 1.0),
        (2021, 2.0),
        (2021, 2.0),
        (2021, 1.0),
        (2021, 1.0),
    ]

    exclude_outliers_rows = [
        (2021, 10.0),
        (2021, 20.0),
        (2021, 0.0),
        (2021, 9.0),
        (2021, 1.0),
        (2021, -1.0),
    ]

    calculate_average_ratio_rows = [
        (2021, 1.0),
        (2021, 2.0),
        (2020, 1.0),
        (2020, 1.0),
        (2019, 2.0),
        (2019, 2.0),
    ]

    add_historic_rows = [
        (2011, None),
        (2012, None),
        (2013, None),
        (2014, 1.0),
        (2015, None),
        (2016, None),
        (2017, 1.0),
        (2018, None),
        (2019, 1.0),
        (2020, 1.0),
        (2021, 1.0),
        (2022, 1.6),
        (2023, 2.2),
    ]

    apply_rolling_average_rows = [
        (2019, 1.0),
        (2020, 1.0),
        (2021, 1.0),
        (2022, 1.6),
        (2023, 2.2),
    ]


@dataclass
class ASCWDSWorkerData:
    worker_rows = [
        ("1-000000001", "100", "1", "20220101"),
        ("1-000000001", "101", "1", "20220101"),
        ("1-000000001", "102", "1", "20220101"),
        ("1-000000001", "103", "1", "20220101"),
        ("1-000000001", "104", "2", "20220101"),
        ("1-000000001", "105", "3", "20220101"),
        ("1-000000002", "106", "1", "20220101"),
    ]


@dataclass
class ASCWDSWorkplaceData:
    workplace_rows = rows = [
        (
            "1-000000001",
            "101",
            14,
            16,
            "20200101",
            "1",
            date(2021, 2, 1),
            0,
            "201",
            "01/02/2021",
        ),
        (
            "1-000000002",
            "102",
            76,
            65,
            "20200101",
            "1",
            date(2021, 4, 1),
            1,
            None,
            "01/02/2021",
        ),
        (
            "1-000000003",
            "103",
            34,
            34,
            "20200101",
            "2",
            date(2021, 3, 1),
            0,
            "203",
            "01/02/2021",
        ),
        (
            "1-000000004",
            "104",
            234,
            265,
            "20190101",
            "2",
            date(2021, 4, 1),
            0,
            None,
            "01/02/2021",
        ),
        (
            "1-000000005",
            "105",
            62,
            65,
            "20190101",
            "3",
            date(2021, 10, 1),
            0,
            None,
            "01/02/2021",
        ),
        (
            "1-000000006",
            "106",
            77,
            77,
            "20190101",
            "3",
            date(2020, 3, 1),
            1,
            None,
            "01/02/2021",
        ),
        (
            "1-000000007",
            "107",
            51,
            42,
            "20190101",
            " 3",
            date(2021, 5, 1),
            0,
            None,
            "01/05/2021",
        ),
        (
            "1-000000008",
            "108",
            36,
            34,
            "20190101",
            "4",
            date(2021, 7, 1),
            0,
            None,
            "01/05/2021",
        ),
        (
            "1-000000009",
            "109",
            34,
            32,
            "20190101",
            "5",
            date(2021, 12, 1),
            0,
            None,
            "01/05/2021",
        ),
        (
            "1-0000000010",
            "110",
            14,
            20,
            "20190101",
            "6",
            date(2021, 3, 1),
            0,
            None,
            "01/05/2021",
        ),
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
        ),
    ]

    sector_rows = [
        "1-10000000002",
        "1-10000000003",
        "1-10000000004",
        "1-10000000005",
    ]

    expected_rows_with_cqc_sector = [
        ("1-10000000001", CQCPValues.independent),
        ("1-10000000002", CQCPValues.local_authority),
        ("1-10000000003", CQCPValues.local_authority),
    ]


@dataclass
class CQCpirData:
    sample_rows_full = [
        (
            "1-10000000001",
            "Location 1",
            "Community",
            "2024-01-01",
            1,
            0,
            0,
            None,
            None,
            "Community based adult social care services",
            "ASC North",
            "Wakefield",
            0,
            "Y",
            "Active",
        ),
        (
            "1-10000000002",
            "Location 2",
            "Residential",
            "2024-01-01",
            86,
            8,
            3,
            None,
            None,
            "Residential social care",
            "ASC London",
            "Islington",
            53,
            None,
            "Active",
        ),
        (
            "1-10000000003",
            "Location 3",
            "Residential",
            "2024-01-01",
            37,
            5,
            5,
            None,
            None,
            "Residential social care",
            "ASC Central",
            "Nottingham",
            50,
            None,
            "Active",
        ),
    ]


@dataclass
class CQCLocationsData:
    sample_rows_full = [
        (
            "location1",
            "provider1",
            "Location",
            "Social Care Org",
            "name of location",
            "E00000001",
            "NHS West Sussex CCG",
            "ABC1D",
            "123456789",
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
            50.123455,
            -5.6789,
            "Y",
            "Adult social care",
            "01234567891",
            "Woking",
            "Trafford",
            {"date": "2021-01-15"},
            {"publicationdate": "2021-02-15"},
            [
                {
                    "relatedlocationid": "1-23456",
                    "relatedlocationname": "CarePlace",
                    "type": "HSCA Predecessor",
                    "reason": "Location Move",
                }
            ],
            [
                {
                    "name": "Personal care",
                    "code": "RA1",
                    "contacts": [
                        {
                            "persontitle": "Mr",
                            "persongivenname": "John",
                            "personfamilyname": "Doe",
                            "personroles": ["Registered Manager"],
                        }
                    ],
                }
            ],
            [{"name": "Homecare agencies", "description": "Domiciliary care service"}],
            [
                {
                    "code": "S2",
                    "primary": "true",
                    "name": "Community based adult social care services",
                }
            ],
            [{"name": "Services for everyone"}],
            {
                "overall": {
                    "rating": "Good",
                    "reportdate": "2020-01-01",
                    "reportlinkid": "1234abcd5678",
                    "keyquestionratings": [
                        {
                            "name": "Safe",
                            "rating": "Good",
                            "reportdate": "2020-01-01",
                            "reportlinkid": "1234abcd5678",
                        },
                        {
                            "name": "Well-led",
                            "rating": "Good",
                            "reportdate": "2020-01-01",
                            "reportlinkid": "1234abcd5678",
                        },
                        {
                            "name": "Caring",
                            "rating": "Good",
                            "reportdate": "2020-01-01",
                            "reportlinkid": "1234abcd5678",
                        },
                        {
                            "name": "Responsive",
                            "rating": "Good",
                            "reportdate": "2020-01-01",
                            "reportlinkid": "1234abcd5678",
                        },
                        {
                            "name": "Effective",
                            "rating": "Good",
                            "reportdate": "2020-01-01",
                            "reportlinkid": "1234abcd5678",
                        },
                    ],
                }
            },
            None,
            [
                {
                    "linkid": "zyx123",
                    "reportdate": "2021-02-10",
                    "reporturi": "/reports/zyx123",
                    "firstvisitdate": "2020-01-01",
                    "reporttype": "Location",
                }
            ],
            "2020-01-01"
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
    ]

    small_location_rows = [
        (
            "loc-1",
            "prov-1",
            "2020-01-01",
        ),
        (
            "loc-2",
            "prov-1",
            "2020-01-01",
        ),
        (
            "loc-3",
            "prov-2",
            "2020-01-01",
        ),
        (
            "loc-4",
            "prov-2",
            "2021-01-01",
        ),
    ]

    join_provider_rows = [
        (
            "prov-1",
            "Apple Tree Care Homes",
            "Local authority",
            "North East",
            "2020-01-01",
        ),
        (
            "prov-2",
            "Sunshine Domestic Care",
            "Independent",
            "North West",
            "2020-01-01",
        ),
        (
            "prov-2",
            "Sunny Days Domestic Care",
            "Independent",
            "North East",
            "2021-01-01",
        ),
    ]

    expected_joined_rows = [
        (
            "loc-1",
            "prov-1",
            "Apple Tree Care Homes",
            "Local authority",
            "2020-01-01",
        ),
        (
            "loc-2",
            "prov-1",
            "Apple Tree Care Homes",
            "Local authority",
            "2020-01-01",
        ),
        (
            "loc-3",
            "prov-2",
            "Sunshine Domestic Care",
            "Independent",
            "2020-01-01",
        ),
        (
            "loc-4",
            "prov-2",
            "Sunny Days Domestic Care",
            "Independent",
            "2021-01-01",
        ),
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

    gender = [
        ("1", "male"),
        ("2", "female"),
    ]

    nationality = [
        ("100", "British"),
        ("101", "French"),
        ("102", "Spanish"),
        ("103", "Portuguese"),
    ]

    replace_labels_rows = [
        ("1", "1"),
        ("2", "2"),
        ("3", None),
        ("4", None),
        ("5", "2"),
    ]

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

    expected_rows_replace_labels_in_situe = [
        ("1", "male"),
        ("2", "female"),
        ("3", None),
        ("4", None),
        ("5", "female"),
    ]

    expected_rows_replace_labels_with_new_column = [
        ("1", "1", "male"),
        ("2", "2", "female"),
        ("3", None, None),
        ("4", None, None),
        ("5", "2", "female"),
    ]
