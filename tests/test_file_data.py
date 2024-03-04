from dataclasses import dataclass
from datetime import date

from utils.diagnostics_utils.diagnostics_meta_data import (
    Variables as Values,
)

from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
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
        ("1-000000001", "101", "100", "1", "20200101", "2020", "01", "01"),
        ("1-000000002", "102", "101", "1", "20200101", "2020", "01", "01"),
        ("1-000000003", "103", "102", "1", "20200101", "2020", "01", "01"),
        ("1-000000004", "104", "103", "1", "20190101", "2019", "01", "01"),
        ("1-000000005", "104", "104", "2", "19000101", "1900", "01", "01"),
        ("1-000000006", "invalid", "105", "3", "20200101", "2020", "01", "01"),
        ("1-000000007", "999", "106", "1", "20200101", "2020", "01", "01"),
    ]

    expected_worker_rows = [
        ("1-000000001", "101", "100", "1", "20200101", "2020", "01", "01"),
        ("1-000000002", "102", "101", "1", "20200101", "2020", "01", "01"),
        ("1-000000003", "103", "102", "1", "20200101", "2020", "01", "01"),
        ("1-000000004", "104", "103", "1", "20190101", "2019", "01", "01"),
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

    small_location_rows = [
        (
            "loc-1",
            "2020-01-01",
            "1",
        ),
        (
            "loc-2",
            "2020-01-01",
            "1",
        ),
        (
            "loc-3",
            "2020-01-01",
            "2",
        ),
        (
            "loc-4",
            "2021-01-01",
            "2",
        ),
    ]

    location_rows_with_duplicates = [
        *small_location_rows,
        (
            "loc-3",
            "2020-01-01",
            "10",
        ),
        (
            "loc-4",
            "2021-01-01",
            "10",
        ),
    ]

    location_rows_with_different_import_dates = [
        *small_location_rows,
        (
            "loc-3",
            "2021-01-01",
            "10",
        ),
        (
            "loc-4",
            "2022-01-01",
            "10",
        ),
    ]

    expected_filtered_location_rows = [
        (
            "loc-1",
            "2020-01-01",
            "1",
        ),
        (
            "loc-2",
            "2020-01-01",
            "1",
        ),
    ]

    purge_outdated_data = [
        (
            "1-000000001",
            "20200101",
            "1",
            date(2020, 1, 1),
            0,
        ),
        (
            "1-000000002",
            "20200101",
            "2",
            date(2000, 1, 1),
            0,
        ),
        (
            "1-000000003",
            "20200101",
            "1",
            date(2018, 1, 1),
            0,
        ),
        (
            "1-000000004",
            "20200101",
            "2",
            date(2017, 12, 31),
            0,
        ),
        (
            "1-000000005",
            "20200101",
            "1",
            date(2015, 1, 1),
            1,
        ),
        (
            "1-000000006",
            "20200101",
            "2",
            date(2013, 1, 1),
            1,
        ),
    ]

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

    expected_rows_with_cqc_sector = [
        ("1-10000000001", CQCPValues.independent),
        ("1-10000000002", CQCPValues.local_authority),
        ("1-10000000003", CQCPValues.local_authority),
    ]


@dataclass
class ONSData:
    sample_rows = [
        ("Yorkshire & Humber", "Leeds", "50.10101"),
        ("Yorkshire & Humber", "York", "52.10101"),
        ("Yorkshire & Humber", "Hull", "53.10101"),
    ]

    expected_rows = [
        ("Yorkshire & Humber", "Leeds", "50.10101"),
        ("Yorkshire & Humber", "York", "52.10101"),
        ("Yorkshire & Humber", "Hull", "53.10101"),
    ]

    # fmt: off
    ons_sample_rows_full = [
        ("AB10AA", "cssr1", "region1", "subicb1", "icb1", "icb_region1", "ccg1", "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "20220101"),
        ("AB10AB", "cssr1", "region1", "subicb1", "icb1", "icb_region1", "ccg1", "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "20220101"),
        ("AB10AA", "cssr2", "region1", "subicb2", "icb2", "icb_region2", None, "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "20230101"),
        ("AB10AB", "cssr2", "region1", "subicb2", "icb2", "icb_region2", None, "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "20230101"),
        ("AB10AC", "cssr2", "region1", "subicb2", "icb2", "icb_region2", None, "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "20230101"),
    ]
    # fmt: on


@dataclass
class CQCpirData:
    sample_rows_full = [
        (
            "1-1000000001",
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
            "20230201",
        ),
        (
            "1-1000000002",
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
            "20230201",
        ),
        (
            "1-1000000003",
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
            "20230201",
        ),
    ]

    add_care_home_column_rows = [
        ("loc 1", "Residential"),
        ("loc 2", "Shared Lives"),
        ("loc 3", None),
        ("loc 4", "Community"),
    ]
    expected_care_home_column_rows = [
        ("loc 1", "Residential", "Y"),
        ("loc 2", "Shared Lives", None),
        ("loc 3", None, None),
        ("loc 4", "Community", "N"),
    ]


@dataclass
class CQCPirCleanedData:
    subset_for_latest_submission_date_before_filter = [
        ("1-1199876096", "Y", date(2022, 2, 1), date(2021, 5, 7)),
        ("1-1199876096", "Y", date(2022, 7, 1), date(2022, 5, 20)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 12)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 24)),
        ("1-1199876096", "N", date(2023, 6, 1), date(2023, 5, 24)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 24)),
    ]
    subset_for_latest_submission_date_after_filter_deduplication = [
        ("1-1199876096", "Y", date(2022, 2, 1), date(2021, 5, 7)),
        ("1-1199876096", "Y", date(2022, 7, 1), date(2022, 5, 20)),
        ("1-1199876096", "N", date(2023, 6, 1), date(2023, 5, 24)),
        ("1-1199876096", "Y", date(2023, 6, 1), date(2023, 5, 24)),
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
            "2020-01-01",
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

    expected_deregistered_rows = [
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
            "Yorkshire & Humber",
            "Hertfordshire",
            "Leeds",
            "Urban city and town",
            "20210101",
            "LS1 2AB",
        ),
        (
            "Yorkshire & Humber",
            "Hertfordshire",
            "York",
            "Urban city and town",
            "20210101",
            "B69 3EG",
        ),
        (
            "Yorkshire & Humber",
            "Hertfordshire",
            "Hull",
            "Urban city and town",
            "20200101",
            "PR1 9HL",
        ),
    ]

    expected_processed_ons_rows = [
        (
            "Yorkshire & Humber",
            "Hertfordshire",
            "Leeds",
            "Urban city and town",
            "LS1 2AB",
            date(2021, 1, 1),
        ),
        (
            "Yorkshire & Humber",
            "Hertfordshire",
            "York",
            "Urban city and town",
            "B69 3EG",
            date(2021, 1, 1),
        ),
    ]

    locations_for_contemporary_ons_join_rows = [
        (
            "loc-1",
            "prov-1",
            date(2020, 1, 1),
            "PR1 9AB",
        ),
        (
            "loc-2",
            "prov-1",
            date(2018, 1, 1),
            "B69 3EG",
        ),
        (
            "loc-3",
            "prov-2",
            date(2020, 1, 1),
            "PR1 9HL",
        ),
        (
            "loc-4",
            "prov-2",
            date(2021, 1, 1),
            "LS1 2AB",
        ),
    ]

    ons_for_contemporary_ons_join_rows = [
        (
            "Yorkshire & Humber",
            "Hertfordshire",
            "Leeds",
            "Urban city and town",
            date(2021, 1, 1),
            "LS1 2AB",
        ),
        (
            "Yorkshire & Humber",
            "Hertfordshire",
            "York",
            "Urban city and town",
            date(2021, 1, 1),
            "B69 3EG",
        ),
        (
            "Yorkshire & Humber",
            "Hertfordshire",
            "Hull",
            "Urban city and town",
            date(2019, 1, 1),
            "PR1 9HL",
        ),
    ]

    expected_contemporary_ons_join_rows = [
        (
            "loc-1",
            "prov-1",
            date(2020, 1, 1),
            "PR1 9AB",
            None,
            None,
            None,
            None,
            date(2019, 1, 1),
        ),
        (
            "loc-2",
            "prov-1",
            date(2018, 1, 1),
            "B69 3EG",
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "loc-3",
            "prov-2",
            date(2020, 1, 1),
            "PR1 9HL",
            "Yorkshire & Humber",
            "Hertfordshire",
            "Hull",
            "Urban city and town",
            date(2019, 1, 1),
        ),
        (
            "loc-4",
            "prov-2",
            date(2021, 1, 1),
            "LS1 2AB",
            "Yorkshire & Humber",
            "Hertfordshire",
            "Leeds",
            "Urban city and town",
            date(2021, 1, 1),
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


@dataclass
class MergeIndCQCData:
    clean_cqc_pir_rows = [
        ("1-000000001", "Y", date(2024, 1, 1), 10),
        ("1-000000002", "N", date(2024, 1, 1), 20),
        ("1-000000003", "Y", date(2024, 1, 1), 30),
        ("1-000000001", "Y", date(2024, 2, 1), 1),
        ("1-000000002", "N", date(2024, 2, 1), 4),
    ]

    # fmt: off
    clean_cqc_location_for_merge_rows = [
        (date(2024, 1, 1), "1-000000001", "Independent", "Y", 10,),
        (date(2024, 1, 1), "1-000000002", "Independent", "N", None,),
        (date(2024, 1, 1), "1-000000003", "Independent", "N", None,),
        (date(2024, 2, 1), "1-000000001", "Independent", "Y", 10,),
        (date(2024, 2, 1), "1-000000002", "Independent", "N", None,),
        (date(2024, 2, 1), "1-000000003", "Independent", "N", None,),
        (date(2024, 3, 1), "1-000000001", "Independent", "Y", 10,),
        (date(2024, 3, 1), "1-000000002", "Independent", "N", None,),
        (date(2024, 3, 1), "1-000000003", "Independent", "N", None,),
    ]
    # fmt: on

    # fmt: off
    clean_ascwds_workplace_for_merge_rows = [
        (date(2024, 1, 1), "1-000000001", "1", 1,),
        (date(2024, 1, 1), "1-000000003", "3", 2,),
        (date(2024, 1, 5), "1-000000001", "1", 3,),
        (date(2024, 1, 9), "1-000000001", "1", 4,),
        (date(2024, 1, 9), "1-000000003", "3", 5,),
        (date(2024, 3, 1), "1-000000003", "4", 6,),
    ]
    # fmt: on

    # fmt: off
    expected_merged_cqc_and_pir = [
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

    # fmt: off
    expected_cqc_and_ascwds_merged_rows = [
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

    # fmt: off
    cqc_sector_rows = [
        ("loc-1", "Local Authority",),
        ("loc-2", None,),
        ("loc-3", "Independent",),
    ]
    expected_cqc_sector_rows = [
        ("loc-3", "Independent",),
    ]
    # fmt: on
