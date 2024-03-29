from dataclasses import dataclass
from datetime import date

from pyspark.ml.linalg import Vectors


from utils.diagnostics_utils.diagnostics_meta_data import (
    Variables as Values,
)

from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedValues as CQCPValues,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedValues as CQCLValues,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_absolute_difference_within_range import (
    ascwds_filled_posts_absolute_difference_within_range_source_description,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_return_only_permitted_value import (
    ascwds_filled_posts_select_only_value_source_description,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_return_worker_record_count_if_equal_to_total_staff import (
    ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description,
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
        ("AB10AA", "cssr1", "region1", "subicb1", "icb1", "icb_region1", "ccg1", "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "2022", "01", "01", "20220101"),
        ("AB10AB", "cssr1", "region1", "subicb1", "icb1", "icb_region1", "ccg1", "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "2022", "01", "01", "20220101"),
        ("AB10AA", "cssr2", "region1", "subicb2", "icb2", "icb_region2", None, "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "2023", "01", "01", "20230101"),
        ("AB10AB", "cssr2", "region1", "subicb2", "icb2", "icb_region2", None, "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "2023", "01", "01", "20230101"),
        ("AB10AC", "cssr2", "region1", "subicb2", "icb2", "icb_region2", None, "51.23456", "-.12345", "123", "E010123", "E020123", "Rural village", "E010123", "E020123", "pcon1", "2023", "01", "01", "20230101"),
    ]
    # fmt: on


@dataclass
class CapacityTrackerCareHomeData:
    sample_rows = [
        (
            "Barnsley Metropolitan Borough Council",
            "Woodways",
            "Bespoke Care and Support Ltd",
            "South Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "North East and Yorkshire",
            "NHS South Yorkshire ICB",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "1-10192918971",
            "VNJ4V",
            "0",
            "No",
            "0",
            "0",
            "0",
            "61",
            "0",
            "0",
            "8",
            "0",
            "0",
            "0",
            "0",
            "0",
            "9483",
            "1623",
            "432",
            "444",
            "0",
            "45330.3840277778",
            "45330.3840277778",
        ),
        (
            "Barnsley Metropolitan Borough Council",
            "Woodlands Lodge Care Home",
            "Mr Dhanus Dharry Ramdharry, Mrs Sooba Devi Mootyen, Mr Dhanraz Danny Ramdharry",
            "South Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "North East and Yorkshire",
            "NHS South Yorkshire ICB",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "1-933054479",
            "VLEH4",
            "2",
            "Yes",
            "0",
            "0",
            "0",
            "28",
            "0",
            "0",
            "14",
            "0",
            "0",
            "0",
            "0",
            "0",
            "4658",
            "0",
            "18",
            "0",
            "24",
            "45330.4958333333",
            "45330.4958333333",
        ),
        (
            "Barnsley Metropolitan Borough Council",
            "Water Royd Nursing Home",
            "Maria Mallaband Limited",
            "South Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "North East and Yorkshire",
            "NHS South Yorkshire ICB",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "1-124000082",
            "VLNVC",
            "0",
            "Yes",
            "11",
            "3",
            "0",
            "46",
            "5",
            "0",
            "14",
            "0",
            "0",
            "0",
            "0",
            "0",
            "9334",
            "1",
            "0",
            "37",
            "0",
            "45351.3625",
            "45351.3625",
        ),
    ]

    expected_rows = sample_rows


@dataclass
class CapacityTrackerDomCareData:
    sample_rows = [
        (
            "Barnsley Metropolitan Borough Council",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "NHS South Yorkshire ICB",
            "North East and Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "South Yorkshire",
            "Yorkshire and The Humber",
            "AJB Care Ltd",
            "1-1140582998",
            "VN5A8",
            "45330.3854166667",
            "45330.3854166667",
            "57",
            "",
            "",
            "20",
            "0",
            "TRUE",
            "40",
            "16",
            "2",
            "4",
            "Yes",
            "2228",
            "0",
            "0",
            "0",
            "14",
            "0",
            "0",
            "0",
        ),
        (
            "Barnsley Metropolitan Borough Council",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "NHS South Yorkshire ICB",
            "North East and Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "South Yorkshire",
            "Yorkshire and The Humber",
            "Barnsley Disability Services Limited",
            "1-1002692043",
            "VN1N0",
            "45331.4673611111",
            "45331.4673611111",
            "12",
            "",
            "",
            "10",
            "0",
            "FALSE",
            "0",
            "10",
            "1",
            "3",
            "Yes",
            "1428",
            "7",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
        ),
        (
            "Barnsley Metropolitan Borough Council",
            "NHS South Yorkshire ICB - 02P Barnsley",
            "NHS South Yorkshire ICB",
            "North East and Yorkshire",
            "Barnsley Metropolitan Borough Council",
            "South Yorkshire",
            "Yorkshire and The Humber",
            "Barnsley Mencap",
            "1-119187505",
            "VN3L9",
            "45331.4597222222",
            "45331.4597222222",
            "102",
            "",
            "",
            "165",
            "0",
            "FALSE",
            "0",
            "161",
            "28",
            "37",
            "Yes",
            "18015",
            "3113",
            "567",
            "0",
            "171",
            "0",
            "0",
            "0",
        ),
    ]

    expected_rows = sample_rows


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

    expected_services_offered_rows = [
        (
            "location1",
            "provider1",
            [
                {
                    "name": "Homecare agencies",
                    "description": "Domiciliary care service",
                },
            ],
            ["Domiciliary care service"],
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
            ["Care home service with nursing"],
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
            ["Care home service without nursing"],
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
            ["Care home service with nursing", "Care home service without nursing"],
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
            ["Care home service without nursing", "Fake service"],
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


@dataclass
class CleanIndCQCData:
    # fmt: off
    merged_rows_for_cleaning_job = [
        ("1-1000001", "20220201", date(2020, 2, 1), "South East", "Surrey", "Rural", "Y", 0, 5, 82, None, "Care home without nursing"),
        ("1-1000001", "20220101", date(2022, 1, 1), "South East", "Surrey", "Rural", "Y", 5, 5, None, 67, "Care home without nursing"),
        ("1-1000002", "20220101", date(2022, 1, 1), "South East", "Surrey", "Rural", "N", 0, 17, None, None, "non-residential"),
        ("1-1000002", "20220201", date(2022, 2, 1), "South East", "Surrey", "Rural", "N", 0, 34, None, None, "non-residential"),
        ("1-1000003", "20220301", date(2022, 3, 1), "North West", "Bolton", "Urban", "N", 0, 34, None, None, "non-residential"),
        ("1-1000003", "20220308", date(2022, 3, 8), "North West", "Bolton", "Rural", "N", 0, 15, None, None, "non-residential"),
        ("1-1000004", "20220308", date(2022, 3, 8), "South West", "Dorset", "Urban", "Y", 9, 0, 25, 25, "Care home with nursing"),
    ]
    # fmt: on

    # fmt: off
    calculate_ascwds_filled_posts_rows = [
        # Both 0: Return None
        ("1-000001", 0, None, None, None,),
        # Both 500: Return 500
        ("1-000002", 500, 500, None, None,),
        # Only know total_staff: Return totalstaff (10)
        ("1-000003", 10, None, None, None,),
        # worker_record_count below min permitted: return totalstaff (23)
        ("1-000004", 23, 1, None, None,),
        # Only know worker_records: Return worker_record_count (100)
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
        # Only know total_staff: Return totalstaff (10)
        ("1-000003", 10, None, 10.0, ascwds_filled_posts_select_only_value_source_description(IndCQC.total_staff_bounded),),
        # worker_record_count below min permitted: return totalstaff (23)
        ("1-000004", 23, 1, 23.0, ascwds_filled_posts_select_only_value_source_description(IndCQC.total_staff_bounded),),
        # Only know worker_records: Return worker_record_count (100)
        ("1-000005", None, 100, 100.0, ascwds_filled_posts_select_only_value_source_description(IndCQC.worker_records_bounded),),
        # None of the rules apply: Return None
        ("1-000006", 900, 600, None, None,),
        # Absolute difference is within absolute bounds: Return Average
        ("1-000007", 12, 11, 11.5, ascwds_filled_posts_absolute_difference_within_range_source_description,),
        # Absolute difference is within percentage bounds: Return Average
        ("1-000008", 500, 475, 487.5, ascwds_filled_posts_absolute_difference_within_range_source_description,),
        # Already populated, shouldn't change it
        ("1-000009", 10, 10, 10.0, ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description),
    ]
    # fmt: on

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
class FilterAscwdsFilledPostsData:
    input_rows = [
        ("01", date(2023, 1, 1), "Y", 25, 1.0),
        ("02", date(2023, 1, 1), "Y", 25, 2.0),
        ("03", date(2023, 1, 1), "Y", 25, 3.0),
    ]

    # fmt: off
    care_home_filled_posts_per_bed_rows = [
        ("01", date(2023, 1, 1), "Y", 25, 1.0),
        ("02", date(2023, 1, 1), "Y", 25, 2.0),
        ("03", date(2023, 1, 1), "Y", 25, 3.0),
        ("04", date(2023, 1, 1), "Y", 25, 4.0),
        ("05", date(2023, 1, 1), "Y", 25, 5.0),
        ("06", date(2023, 1, 1), "Y", 25, 6.0),
        ("07", date(2023, 1, 1), "Y", 25, 7.0),
        ("08", date(2023, 1, 1), "Y", 25, 8.0),
        ("09", date(2023, 1, 1), "Y", 25, 9.0),
        ("10", date(2023, 1, 1), "Y", 25, 10.0),
        ("11", date(2023, 1, 1), "Y", 25, 11.0),
        ("12", date(2023, 1, 1), "Y", 25, 12.0),
        ("13", date(2023, 1, 1), "Y", 25, 13.0),
        ("14", date(2023, 1, 1), "Y", 25, 14.0),
        ("15", date(2023, 1, 1), "Y", 25, 15.0),
        ("16", date(2023, 1, 1), "Y", 25, 16.0),
        ("17", date(2023, 1, 1), "Y", 25, 17.0),
        ("18", date(2023, 1, 1), "Y", 25, 18.0),
        ("19", date(2023, 1, 1), "Y", 25, 19.0),
        ("20", date(2023, 1, 1), "Y", 25, 20.0),
        ("21", date(2023, 1, 1), "Y", 25, 21.0),
        ("22", date(2023, 1, 1), "Y", 25, 22.0),
        ("23", date(2023, 1, 1), "Y", 25, 23.0),
        ("24", date(2023, 1, 1), "Y", 25, 24.0),
        ("25", date(2023, 1, 1), "Y", 25, 25.0),
        ("26", date(2023, 1, 1), "Y", 25, 26.0),
        ("27", date(2023, 1, 1), "Y", 25, 27.0),
        ("28", date(2023, 1, 1), "Y", 25, 28.0),
        ("29", date(2023, 1, 1), "Y", 25, 29.0),
        ("30", date(2023, 1, 1), "Y", 25, 30.0),
        ("31", date(2023, 1, 1), "Y", 25, 31.0),
        ("32", date(2023, 1, 1), "Y", 25, 32.0),
        ("33", date(2023, 1, 1), "Y", 25, 33.0),
        ("34", date(2023, 1, 1), "Y", 25, 34.0),
        ("35", date(2023, 1, 1), "Y", 25, 35.0),
        ("36", date(2023, 1, 1), "Y", 25, 36.0),
        ("37", date(2023, 1, 1), "Y", 25, 37.0),
        ("38", date(2023, 1, 1), "Y", 25, 38.0),
        ("39", date(2023, 1, 1), "Y", 25, 39.0),
        ("40", date(2023, 1, 1), "Y", 25, 40.0),
        ("41", date(2023, 1, 1), "Y", 25, None),
        ("42", date(2023, 1, 1), "Y", None, 42.0),
        ("43", date(2023, 1, 1), "N", 25, 43.0),
        ("44", date(2023, 1, 1), "N", None, 44.0),
    ]
    # fmt: on


@dataclass
class NonResFeaturesData(object):
    # fmt: off
    rows = [
        ("1-1783948", date(2022, 2, 1), "South East", 0, ["Domiciliary care service"], "non-residential", 5, None, "Surrey", "N", "Independent", "Rural hamlet and isolated dwellings in a sparse setting", "rule_1", "Registered", '2022', '02', '01', '20220201'),
        ("1-1783948", date(2022, 1, 1), "South East", 0, ["Domiciliary care service"], "non-residential", 5, 67.0, "Surrey", "N", "Independent", "Rural hamlet and isolated dwellings in a sparse setting", "rule_2", "Registered", '2022', '01', '01', '20220101'),
        ("1-10235302415", date(2022, 1, 12), "South West", 0, ["Urgent care services", "Supported living service"], "non-residential", 17, None, "Surrey", "N", "Independent", "Rural hamlet and isolated dwellings", "rule_3", "Registered", '2022', '01', '12', '20220112'),
        ("1-1060912125", date(2022, 1, 12), "Yorkshire and the Humber", 0, ["Hospice services at home"], "non-residential", 34, None, "Surrey", "N", "Independent", "Rural hamlet and isolated dwellings", "rule_2", "Registered", '2022', '01', '12', '20220212'),
        ("1-107095666", date(2022, 3, 1), "Yorkshire and the Humber", 0, ["Specialist college service", "Community based services for people who misuse substances", "Urgent care services'"], "non-residential", 34, None, "Lewisham", "N", "Independent", "Urban city and town", "rule_3", "Registered", '2022', '03', '01', '20220301'),
        ("1-108369587", date(2022, 3, 8), "South West", 0, ["Specialist college service"], "non-residential", 15, None, "Lewisham", "N", "Independent", "Rural town and fringe in a sparse setting", "rule_1", "Registered", '2022', '03', '08', '20220308'),
        ("1-000000001", date(2022, 3, 8), "Yorkshire and the Humber", 67, ["Care home service with nursing"], "Care home with nursing", None, None, "Lewisham", "Y", "Local authority", "Urban city and town", "rule_1", "Registered", '2022', '03', '08', '20220308'),
        ("1-10894414510", date(2022, 3, 8), "Yorkshire and the Humber", 10, ["Care home service with nursing"], "Care home with nursing", 0, 25.0, "Lewisham", "Y", "Independent", "Urban city and town", "rule_3", "Registered", '2022', '03', '08', '20220308'),
        ("1-108950835", date(2022, 3, 15), "Merseyside", 20, ["Care home service without nursing"], "Care home without nursing", 23, None, "Lewisham", "Y", "", "Urban city and town", "rule_1", "Registered", '2022', '03', '15', '20220315'),
        ("1-108967195", date(2022, 4, 22), "North West", 0, ["Supported living service", "Acute services with overnight beds"], "non-residential", 11, None, "Lewisham", "N", "Independent", "Urban city and town", "rule_3", "Registered", '2022', '04', '22', '20220422'),
    ]
    # fmt: on

    filter_to_non_care_home_rows = [
        ("Y", CQCLValues.independent),
        ("N", CQCLValues.independent),
    ]

    expected_filtered_to_non_care_home_rows = [
        ("N", CQCLValues.independent),
    ]


@dataclass
class CareHomeFeaturesData:
    clean_merged_data_rows = [
        (
            "1-1783948",
            date(2022, 2, 1),
            "South East",
            0,
            ["Domiciliary care service"],
            5,
            None,
            "N",
            "Independent",
            "Rural hamlet and isolated dwellings in a sparse setting",
            "2023",
            "01",
            "01",
            "20230101",
        ),
        (
            "1-1783948",
            date(2022, 1, 1),
            "South East",
            0,
            ["Domiciliary care service"],
            5,
            67.0,
            "N",
            "Independent",
            "Rural hamlet and isolated dwellings in a sparse setting",
            "2023",
            "01",
            "01",
            "20230101",
        ),
        (
            "1-10235302415",
            date(2022, 1, 12),
            "South West",
            0,
            ["Urgent care services", "Supported living service"],
            17,
            None,
            "N",
            "Independent",
            "Rural hamlet and isolated dwellings",
            "2023",
            "01",
            "01",
            "20230101",
        ),
        (
            "1-1060912125",
            date(2022, 1, 12),
            "Yorkshire and the Humber",
            0,
            ["Hospice services at home"],
            34,
            None,
            "N",
            "Independent",
            "Rural hamlet and isolated dwellings",
            "2023",
            "01",
            "01",
            "20230101",
        ),
        (
            "1-107095666",
            date(2022, 3, 1),
            "Yorkshire and the Humber",
            0,
            [
                "Specialist college service",
                "Community based services for people who misuse substances",
                "Urgent care services'",
            ],
            34,
            None,
            "N",
            "Independent",
            "Urban city and town",
            "2023",
            "01",
            "01",
            "20230101",
        ),
        (
            "1-108369587",
            date(2022, 3, 8),
            "South West",
            0,
            ["Specialist college service"],
            15,
            None,
            "N",
            "Independent",
            "Rural town and fringe in a sparse setting",
            "2023",
            "01",
            "01",
            "20230101",
        ),
        (
            "1-10894414510",
            date(2022, 3, 8),
            "Yorkshire and the Humber",
            10,
            ["Care home service with nursing"],
            0,
            25.0,
            "Y",
            "Independent",
            "Urban city and town",
            "2023",
            "01",
            "01",
            "20230101",
        ),
        (
            "1-108967195",
            date(2022, 4, 22),
            "North West",
            0,
            ["Supported living service", "Acute services with overnight beds"],
            11,
            None,
            "N",
            "Independent",
            "Urban city and town",
            "2023",
            "01",
            "01",
            "20230101",
        ),
    ]
    # fmt: on

    filter_to_care_home_rows = rows = [
        ("Y", CQCLValues.independent),
        ("N", CQCLValues.independent),
    ]

    expected_filtered_to_care_home_rows = rows = [
        ("Y", CQCLValues.independent),
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

    populate_known_jobs_rows = [
        ("1-000000001", 1.0, date(2022, 3, 4), None, None),
        ("1-000000002", None, date(2022, 3, 4), None, None),
        ("1-000000003", 5.0, date(2022, 3, 4), 4.0, "already_populated"),
        ("1-000000004", 10.0, date(2022, 3, 4), None, None),
        ("1-000000002", 7.0, date(2022, 2, 4), None, None),
    ]

    expected_populate_known_jobs_rows = [
        ("1-000000001", 1.0, date(2022, 3, 4), 1.0, "ascwds_filled_posts"),
        ("1-000000002", None, date(2022, 3, 4), None, None),
        ("1-000000003", 5.0, date(2022, 3, 4), 4.0, "already_populated"),
        ("1-000000004", 10.0, date(2022, 3, 4), 10.0, "ascwds_filled_posts"),
        ("1-000000002", 7.0, date(2022, 2, 4), 7.0, "ascwds_filled_posts"),
    ]


@dataclass
class ModelPrimaryServiceRollingAverage:
    input_rows = [
        ("1-000000001", "2023-01-01", 1672531200, 4.0, "non-residential"),
        ("1-000000002", "2023-01-01", 1672531200, 6.0, "non-residential"),
        ("1-000000003", "2023-02-01", 1675209600, 20.0, "non-residential"),
        ("1-000000004", "2023-03-01", 1677628800, 30.0, "non-residential"),
        ("1-000000005", "2023-04-01", 1680303600, 40.0, "non-residential"),
        ("1-000000006", "2023-01-01", 1672531200, None, "non-residential"),
        ("1-000000007", "2023-02-01", 1675209600, None, "non-residential"),
        ("1-000000008", "2023-03-01", 1677628800, None, "non-residential"),
        ("1-000000011", "2023-01-01", 1672531200, 14.0, "Care home with nursing"),
        ("1-000000012", "2023-01-01", 1672531200, 16.0, "Care home with nursing"),
        ("1-000000013", "2023-02-01", 1675209600, 120.0, "Care home with nursing"),
        ("1-000000014", "2023-03-01", 1677628800, 131.0, "Care home with nursing"),
        ("1-000000015", "2023-04-01", 1680303600, 142.0, "Care home with nursing"),
        ("1-000000016", "2023-01-01", 1672531200, None, "Care home with nursing"),
        ("1-000000017", "2023-02-01", 1675209600, None, "Care home with nursing"),
        ("1-000000018", "2023-03-01", 1677628800, None, "Care home with nursing"),
    ]
    known_filled_posts_rows = [
        ("1-000000001", 1672531200, 4.0, "non-residential"),
        ("1-000000002", 1672531200, 6.0, "non-residential"),
        ("1-000000003", 1675209600, 20.0, "non-residential"),
        ("1-000000004", 1677628800, 30.0, "non-residential"),
        ("1-000000005", 1680303600, 40.0, "non-residential"),
        ("1-000000011", 1672531200, 14.0, "Care home with nursing"),
        ("1-000000012", 1672531200, 16.0, "Care home with nursing"),
        ("1-000000013", 1675209600, 120.0, "Care home with nursing"),
        ("1-000000014", 1677628800, 131.0, "Care home with nursing"),
        ("1-000000015", 1680303600, 142.0, "Care home with nursing"),
    ]
    rolling_sum_rows = [
        ("service_1", 86400, 10),
        ("service_1", 172800, 12),
        ("service_1", 259200, 15),
        ("service_1", 345600, 17),
        ("service_1", 432000, 20),
        ("service_2", 86400, 10),
        ("service_2", 172800, 11),
    ]
    rolling_average_rows = [
        ("random_data", 1672531200, "non-residential", 44.24),
        ("random_data", 1680303600, "Care home with nursing", 25.1),
    ]
    calculate_rolling_average_column_rows = [
        ("Care home with nursing", 1672531200, 2, 30.0),
        ("Care home with nursing", 1675209600, 1, 120.0),
        ("Care home with nursing", 1677628800, 1, 131.0),
        ("Care home with nursing", 1680303600, 1, 142.0),
        ("non-residential", 1672531200, 2, 10.0),
        ("non-residential", 1675209600, 1, 20.0),
        ("non-residential", 1677628800, 1, 30.0),
        ("non-residential", 1680303600, 1, 40.0),
    ]


@dataclass
class ModelExtrapolation:
    extrapolation_rows = [
        (
            "1-000000001",
            "2023-01-01",
            1672531200,
            15.0,
            "Care home with nursing",
            None,
            None,
            15.0,
        ),
        (
            "1-000000001",
            "2023-02-01",
            1675209600,
            None,
            "Care home with nursing",
            None,
            None,
            15.1,
        ),
        (
            "1-000000001",
            "2023-03-01",
            1677628800,
            30.0,
            "Care home with nursing",
            30.0,
            "already_populated",
            15.2,
        ),
        (
            "1-000000002",
            "2023-01-01",
            1672531200,
            4.0,
            "non-residential",
            None,
            None,
            50.3,
        ),
        (
            "1-000000002",
            "2023-02-01",
            1675209600,
            None,
            "non-residential",
            None,
            None,
            50.5,
        ),
        (
            "1-000000002",
            "2023-03-01",
            1677628800,
            None,
            "non-residential",
            5.0,
            "already_populated",
            50.7,
        ),
        (
            "1-000000002",
            "2023-04-01",
            1680303600,
            None,
            "non-residential",
            None,
            None,
            50.1,
        ),
        (
            "1-000000003",
            "2023-01-01",
            1672531200,
            None,
            "non-residential",
            None,
            None,
            50.3,
        ),
        (
            "1-000000003",
            "2023-02-01",
            1675209600,
            20.0,
            "non-residential",
            None,
            None,
            50.5,
        ),
        (
            "1-000000003",
            "2023-03-01",
            1677628800,
            None,
            "non-residential",
            30.0,
            "already_populated",
            50.7,
        ),
        (
            "1-000000004",
            "2023-03-01",
            1677628800,
            None,
            "non-residential",
            None,
            None,
            50.7,
        ),
    ]
    data_to_filter_rows = [
        ("1-000000001", "2023-01-01", 15.0, "Care home with nursing"),
        ("1-000000002", "2023-01-01", None, "non-residential"),
        ("1-000000002", "2023-02-01", None, "non-residential"),
        ("1-000000003", "2023-01-01", 20.0, "non-residential"),
        ("1-000000003", "2023-02-01", None, "non-residential"),
    ]
    first_and_last_submission_rows = [
        ("1-000000001", "2023-01-01", 1672531200, None, 12.0),
        ("1-000000001", "2023-02-01", 1675209600, 5.0, 15.0),
        ("1-000000001", "2023-03-01", 1677628800, None, 18.0),
        ("1-000000002", "2023-01-01", 1672531200, 4.0, 12.0),
        ("1-000000002", "2023-02-01", 1675209600, 6.0, 15.0),
        ("1-000000002", "2023-03-01", 1677628800, None, 18.0),
    ]
    extrapolated_values_rows = [
        (
            "1-000000001",
            "2023-01-01",
            1672531200,
            15.0,
            5.0,
            1672531200,
            1677628800,
            15.0,
            5.0,
            30.0,
            5.0,
        ),
        (
            "1-000000001",
            "2023-02-01",
            1675209600,
            None,
            5.0,
            1672531200,
            1677628800,
            15.0,
            5.0,
            30.0,
            5.0,
        ),
        (
            "1-000000001",
            "2023-03-01",
            1677628800,
            30.0,
            5.0,
            1672531200,
            1677628800,
            15.0,
            5.0,
            30.0,
            5.0,
        ),
        (
            "1-000000002",
            "2023-01-01",
            1672531200,
            40.0,
            1.0,
            1672531200,
            1672531200,
            40.0,
            1.0,
            40.0,
            1.0,
        ),
        (
            "1-000000002",
            "2023-02-01",
            1675209600,
            None,
            1.5,
            1672531200,
            1672531200,
            40.0,
            1.0,
            40.0,
            1.0,
        ),
        (
            "1-000000002",
            "2023-03-01",
            1677628800,
            None,
            0.5,
            1672531200,
            1672531200,
            40.0,
            1.0,
            40.0,
            1.0,
        ),
        (
            "1-000000003",
            "2023-01-01",
            1672531200,
            None,
            1.0,
            1675209600,
            1675209600,
            20.0,
            1.7,
            20.0,
            1.7,
        ),
        (
            "1-000000003",
            "2023-02-01",
            1675209600,
            20.0,
            1.7,
            1675209600,
            1675209600,
            20.0,
            1.7,
            20.0,
            1.7,
        ),
        (
            "1-000000003",
            "2023-03-01",
            1677628800,
            None,
            2.0,
            1675209600,
            1675209600,
            20.0,
            1.7,
            20.0,
            1.7,
        ),
    ]
    extrapolated_values_to_be_added_rows = [
        ("1-000000001", "2023-01-01", 1672531200),
        ("1-000000001", "2023-02-01", 1675209600),
        ("1-000000001", "2023-03-01", 1677628800),
        ("1-000000002", "2023-01-01", 1672531200),
        ("1-000000002", "2023-02-01", 1675209600),
        ("1-000000002", "2023-03-01", 1677628800),
        ("1-000000003", "2023-01-01", 1672531200),
        ("1-000000003", "2023-02-01", 1675209600),
        ("1-000000003", "2023-03-01", 1677628800),
        ("1-000000003", "2023-04-01", 1680303600),
        ("1-000000004", "2023-01-01", 1672531200),
    ]
    extrapolated_ratios_rows = [
        ("1-000000001", 1675000000, 1.0, 1678000000, 1680000000, 2.0, 3.0),
        ("1-000000002", 1675000000, 1.0, 1678000000, 1680000000, 1.0, 3.0),
        ("1-000000003", 1675000000, 2.0, 1670000000, 1672000000, 3.0, 1.7),
    ]
    extrapolated_model_outputs_rows = [
        (
            "1-000000001",
            "2023-01-01",
            1675000000,
            1.0,
            1678000000,
            1680000000,
            15.0,
            10.0,
            0.5,
        ),
        (
            "1-000000002",
            "2023-02-01",
            1675000000,
            1.0,
            1678000000,
            1680000000,
            15.0,
            10.0,
            1.0,
        ),
        (
            "1-000000003",
            "2023-03-01",
            1675000000,
            2.0,
            1670000000,
            1672000000,
            10.0,
            15.0,
            1.46882452,
        ),
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
            "Care home with nursing",
            10.0,
            "Y",
            "South West",
            67,
            date(2022, 3, 29),
            Vectors.sparse(46, {0: 1.0, 1: 60.0, 3: 1.0, 32: 97.0, 33: 1.0}),
            34,
        ),
        (
            "1-000000003",
            "Care home with nursing",
            20.0,
            "N",
            "Merseyside",
            34,
            date(2022, 3, 29),
            None,
            0,
        ),
    ]


@dataclass
class InsertPredictionsIntoLocations:
    cleaned_cqc_rows = ModelCareHomes.care_homes_cleaned_ind_cqc_rows

    care_home_features_rows = ModelCareHomes.care_homes_features_rows

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
        (
            "1-000000004",
            "non-residential",
            10.0,
            "N",
            None,
            0,
            date(2022, 3, 29),
            12.34,
        ),
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
class ModelInterpolation:
    interpolation_rows = [
        ("1-000000001", date(2023, 1, 1), 1672531200, None, None, None),
        (
            "1-000000001",
            date(2023, 1, 2),
            1672617600,
            30.0,
            30.0,
            "ascwds_filled_posts",
        ),
        ("1-000000001", date(2023, 1, 3), 1672704000, None, None, None),
        ("1-000000002", date(2023, 1, 1), 1672531200, None, None, None),
        ("1-000000002", date(2023, 1, 3), 1672704000, 4.0, 4.0, "ascwds_filled_posts"),
        ("1-000000002", date(2023, 1, 5), 1672876800, None, None, None),
        ("1-000000002", date(2023, 1, 7), 1673049600, 5.0, 5.0, "ascwds_filled_posts"),
        ("1-000000002", date(2023, 1, 9), 1673222400, 5.0, 5.0, "ascwds_filled_posts"),
        ("1-000000002", date(2023, 1, 11), 1673395200, None, None, None),
        ("1-000000002", date(2023, 1, 13), 1673568000, None, None, None),
        (
            "1-000000002",
            date(2023, 1, 15),
            1673740800,
            20.0,
            20.0,
            "ascwds_filled_posts",
        ),
        ("1-000000002", date(2023, 1, 17), 1673913600, None, 21.0, "other_source"),
        ("1-000000002", date(2023, 1, 19), 1674086400, None, None, None),
    ]

    calculating_submission_dates_rows = [
        ("1-000000001", 1672617600, 1.0),
        ("1-000000002", 1672704000, 1.0),
        ("1-000000002", 1673049600, 1.0),
        ("1-000000002", 1673222400, 1.0),
    ]

    creating_timeseries_rows = [
        ("1-000000001", 1672617600, 1672617600),
        ("1-000000002", 1672704000, 1673049600),
    ]

    merging_exploded_data_rows = [
        ("1-000000001", 1672617600),
        ("1-000000002", 1672704000),
        ("1-000000002", 1672790400),
        ("1-000000002", 1672876800),
        ("1-000000003", 1672790400),
    ]

    merging_known_values_rows = [
        ("1-000000002", 1672704000, 1.0),
        ("1-000000002", 1672876800, 2.5),
        ("1-000000003", 1672790400, 15.0),
    ]

    calculating_interpolated_values_rows = [
        ("1-000000001", 1, 30.0, 1),
        ("1-000000002", 1, 4.0, 1),
        ("1-000000002", 2, None, None),
        ("1-000000002", 3, 5.0, 3),
        ("1-000000003", 2, 5.0, 2),
        ("1-000000003", 3, None, None),
        ("1-000000003", 4, None, None),
        ("1-000000003", 5, 8.5, 5),
    ]


@dataclass
class ValidateMergedIndCqcData:
    # fmt: off
    cqc_locations_rows =  [
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
   
    merged_ind_cqc_rows =[
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Independent", "Y", 10, "1", 1, 10, date(2024, 1, 1)),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, None, None, 20, date(2024, 1, 1)),
        ("1-000000003", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, "3", 2, None, date(2024, 1, 1)),
        ("1-000000001", date(2024, 1, 9), date(2024, 2, 1), "Independent", "Y", 10, "1", 4, 1, date(2024, 2, 1)),
        ("1-000000002", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, None, None, 4, date(2024, 2, 1)),
        ("1-000000003", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, "3", 5, None, date(2024, 2, 1)),
        ("1-000000001", date(2024, 3, 1), date(2024, 3, 1), "Independent", "Y", 10, None, None, 1, date(2024, 2, 1)),
        ("1-000000002", date(2024, 3, 1), date(2024, 3, 1), "Independent", "N", None, None, None, 4, date(2024, 2, 1)),
        ("1-000000003", date(2024, 3, 1), date(2024, 3, 1), "Independent", "N", None, "4", 6, None, date(2024, 2, 1)),
    ]

    merged_ind_cqc_extra_row_rows =[
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Independent", "Y", 10, "1", 1, 10, date(2024, 1, 1)),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, None, None, 20, date(2024, 1, 1)),
        ("1-000000003", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, "3", 2, None, date(2024, 1, 1)),
        ("1-000000001", date(2024, 1, 9), date(2024, 2, 1), "Independent", "Y", 10, "1", 4, 1, date(2024, 2, 1)),
        ("1-000000002", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, None, None, 4, date(2024, 2, 1)),
        ("1-000000003", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, "3", 5, None, date(2024, 2, 1)),
        ("1-000000001", date(2024, 3, 1), date(2024, 3, 1), "Independent", "Y", 10, None, None, 1, date(2024, 2, 1)),
        ("1-000000002", date(2024, 3, 1), date(2024, 3, 1), "Independent", "N", None, None, None, 4, date(2024, 2, 1)),
        ("1-000000003", date(2024, 3, 1), date(2024, 3, 1), "Independent", "N", None, "4", 6, None, date(2024, 2, 1)),
        ("1-000000004", date(2024, 3, 1), date(2024, 3, 1), "Independent", "N", None, "4", 6, None, date(2024, 2, 1)),
    ]

    merged_ind_cqc_missing_row_rows =[
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Independent", "Y", 10, "1", 1, 10, date(2024, 1, 1)),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, None, None, 20, date(2024, 1, 1)),
        ("1-000000003", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, "3", 2, None, date(2024, 1, 1)),
        ("1-000000001", date(2024, 1, 9), date(2024, 2, 1), "Independent", "Y", 10, "1", 4, 1, date(2024, 2, 1)),
        ("1-000000002", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, None, None, 4, date(2024, 2, 1)),
        ("1-000000003", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, "3", 5, None, date(2024, 2, 1)),
        ("1-000000001", date(2024, 3, 1), date(2024, 3, 1), "Independent", "Y", 10, None, None, 1, date(2024, 2, 1)),
        ("1-000000002", date(2024, 3, 1), date(2024, 3, 1), "Independent", "N", None, None, None, 4, date(2024, 2, 1)),
    ]

    merged_ind_cqc_with_cqc_sector_null_rows =[
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Independent", "Y", 10, "1", 1, 10, date(2024, 1, 1)),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, None, None, 20, date(2024, 1, 1)),
        ("1-000000003", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, "3", 2, None, date(2024, 1, 1)),
        ("1-000000001", date(2024, 1, 9), date(2024, 2, 1), "Independent", "Y", 10, "1", 4, 1, date(2024, 2, 1)),
        ("1-000000002", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, None, None, 4, date(2024, 2, 1)),
        ("1-000000003", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, "3", 5, None, date(2024, 2, 1)),
        ("1-000000001", date(2024, 3, 1), date(2024, 3, 1), "Independent", "Y", 10, None, None, 1, date(2024, 2, 1)),
        ("1-000000002", date(2024, 3, 1), date(2024, 3, 1), "Independent", "N", None, None, None, 4, date(2024, 2, 1)),
        ("1-000000003", date(2024, 3, 1), date(2024, 3, 1), None, "N", None, "4", 6, None, date(2024, 2, 1)),
    ]

    merged_ind_cqc_with_duplicate_data_rows =[
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Independent", "Y", 10, "1", 1, 10, date(2024, 1, 1)),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, None, None, 20, date(2024, 1, 1)),
        ("1-000000003", date(2024, 1, 1), date(2024, 1, 1), "Independent", "N", None, "3", 2, None, date(2024, 1, 1)),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), "Independent", "Y", 10, "1", 4, 1, date(2024, 2, 1)),
        ("1-000000002", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, None, None, 4, date(2024, 2, 1)),
        ("1-000000003", date(2024, 1, 9), date(2024, 2, 1), "Independent", "N", None, "3", 5, None, date(2024, 2, 1)),
        ("1-000000001", date(2024, 3, 1), date(2024, 3, 1), "Independent", "Y", 10, None, None, 1, date(2024, 2, 1)),
        ("1-000000002", date(2024, 3, 1), date(2024, 3, 1), "Independent", "N", None, None, None, 4, date(2024, 2, 1)),
        ("1-000000003", date(2024, 3, 1), date(2024, 3, 1), "Independent", "N", None, "4", 6, None, date(2024, 2, 1)),
    ]
    # fmt: on
