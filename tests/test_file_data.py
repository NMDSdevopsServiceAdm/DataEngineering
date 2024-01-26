from dataclasses import dataclass

from utils.diagnostics_utils.diagnostics_meta_data import (
    Variables as Values,
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
        ("location_2", Values.non_residential,  None, None, None, None, None, None, 30.0,),
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
        ),
    ]
