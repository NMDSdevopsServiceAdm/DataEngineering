from dataclasses import dataclass
from datetime import date

from pyspark.ml.linalg import Vectors

from utils.column_names.capacity_tracker_columns import CapacityTrackerColumns as CT
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
    NewCqcLocationApiColumns as CQCLNew,
)
from utils.column_values.categorical_column_values import (
    RegistrationStatus,
    PrimaryServiceType,
    CareHome,
    Sector,
    LocationType,
    CQCRatingsValues,
    ParentsOrSinglesAndSubs,
    IsParent,
    SingleSubDescription,
    Services,
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
from utils.raw_data_adjustments import RecordsToRemoveInLocationsData
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class CreateJobEstimatesDiagnosticsData:
    # fmt: off
    estimate_jobs_rows = [
        ("location_1", date(2023, 4, 1), 40.0, 40.0, PrimaryServiceType.care_home_with_nursing, 60.9, 23.4, 45.1, None, None, 40.0, 45,),
    ]
    capacity_tracker_care_home_rows = [
        ("location_1", 8.0, 12.0, 15.0, 1.0, 3.0, 2.0),
    ]
    expected_add_date_to_capacity_tracker_rows = [
        ("location_1", 8.0, 12.0, 15.0, 1.0, 3.0, 2.0, date(2023, 4, 1)),
    ]
    capacity_tracker_non_residential_rows = [
        ("location_2", 67.0),
    ]
    prepare_capacity_tracker_care_home_rows = [
        ("location_1", PrimaryServiceType.care_home_with_nursing, 8.0, 12.0, 15.0, 1.0, 3.0, 2.0, None,),
        ("location_2", PrimaryServiceType.non_residential, None, None, None, None, None, None, 30.0,),
    ]
    expected_prepare_capacity_tracker_care_home_rows = [
        ("location_1", PrimaryServiceType.care_home_with_nursing, 8.0, 12.0, 15.0, 1.0, 3.0, 2.0, None, 41.0),
        ("location_2", PrimaryServiceType.non_residential, None, None, None, None, None, None, 30.0, None),
    ]
    prepare_capacity_tracker_non_residential_rows = [
        ("location_1", PrimaryServiceType.care_home_with_nursing, 8.0, 12.0, 15.0, 1.0, 3.0, 2.0, None,),
        ("location_2", PrimaryServiceType.non_residential, None, None, None, None, None, None, 75.0,),
    ]
    expected_prepare_capacity_tracker_non_residential_rows = [
        ("location_1", PrimaryServiceType.care_home_with_nursing, 8.0, 12.0, 15.0, 1.0, 3.0, 2.0, None, None),
        ("location_2", PrimaryServiceType.non_residential, None, None, None, None, None, None, 75.0, 97.5),
    ]
    calculate_residuals_rows = [
        ("location_2", 40.0, 40.0, PrimaryServiceType.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, 40, None, 40.0,),
        ("location_3", 40.0, 40.0, PrimaryServiceType.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, 45, 41.0, None,),
        ("location_4", None, None, PrimaryServiceType.non_residential, 60.9, 23.4, None, None, None, 60.0, 45, None, None,),
        ("location_5", None, None, PrimaryServiceType.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, 50.0, None,),
    ]
    expected_calculate_residuals_rows = [
        ("location_2", 40.0, 40.0, PrimaryServiceType.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, 40, None, 40.0, 0.0),
        ("location_3", 40.0, 40.0, PrimaryServiceType.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, 45, 41.0, None, -5.0),
        ("location_4", None, None, PrimaryServiceType.non_residential, 60.9, 23.4, None, None, None, 60.0, 45, None, None, 15.0),
        ("location_5", None, None, PrimaryServiceType.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, 50.0, None, None),
    ]
    models = [
        IndCQC.estimate_filled_posts,
        IndCQC.ascwds_filled_posts_dedup_clean,
    ]

    services = [
        CareHome.care_home,
        CareHome.not_care_home,
    ]

    data_source_columns = [
        IndCQC.ascwds_filled_posts_dedup_clean,
        CT.care_home_employed,
        CT.non_residential_employed,
    ]
    residuals_list = [
        [
            IndCQC.estimate_filled_posts,
            CareHome.care_home,
            IndCQC.ascwds_filled_posts_dedup_clean,
        ],
        [
            IndCQC.estimate_filled_posts,
            CareHome.care_home,
            CT.care_home_employed,
        ],
        [
            IndCQC.estimate_filled_posts,
            CareHome.not_care_home,
            IndCQC.ascwds_filled_posts_dedup_clean,
        ],
        [
            IndCQC.estimate_filled_posts,
            CareHome.not_care_home,
            CT.non_residential_employed,
        ],
        [
            IndCQC.ascwds_filled_posts_dedup_clean,
            CareHome.care_home,
            IndCQC.ascwds_filled_posts_dedup_clean,
        ],
        [
            IndCQC.ascwds_filled_posts_dedup_clean,
            CareHome.care_home,
            CT.care_home_employed,
        ],
        [
            IndCQC.ascwds_filled_posts_dedup_clean,
            CareHome.not_care_home,
            IndCQC.ascwds_filled_posts_dedup_clean,
        ],
        [
            IndCQC.ascwds_filled_posts_dedup_clean,
            CareHome.not_care_home,
            CT.non_residential_employed,
        ],
    ]
    run_residuals_rows = [
        ("location_1", None, None, PrimaryServiceType.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, None, None,),
        ("location_2", 40.0, 40.0, PrimaryServiceType.non_residential, 60.9, 23.4, 45.1, None, None, 40.0, None, None, 40.0,),
        ("location_3", 40.0, 40.0, PrimaryServiceType.care_home_only, 60.9, 23.4, 45.1, None, None, 40.0, 45, 41.0, None,),
        ("location_4", None, None, PrimaryServiceType.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, 45, None, None,),
        ("location_5", None, None, PrimaryServiceType.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, 50.0, None,),
        ("location_6", None, None, PrimaryServiceType.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, 45, 50.0, None,),
        ("location_7", None, None, PrimaryServiceType.non_residential, 60.9, None, None, None, 40.0, 60.9, 45, None, None,),
        ("location_8", None, None, PrimaryServiceType.non_residential, 60.9, None, None, None, 40.0, 60.9, None, None, 40.0,),
        ("location_9", None, None, PrimaryServiceType.non_residential, 60.9, None, None, None, 40.0, 60.9, 45, None, 40.0,),
        ("location_10", None, None, PrimaryServiceType.care_home_with_nursing, 60.9, 23.4, None, None, None, 60.9, None, None, None,),
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
    expected_calculate_average_residual_rows = [
        (2.0,),
    ]
    expected_create_empty_dataframe_rows = [
        ("test",),
    ]
    add_timestamps_rows = [
        ("location_1", 0.0, 0.0,),
        ("location_2", -1.0, 0.0,),
    ]
    expected_add_timestamps_rows = [
        ("location_1", 0.0, 0.0, "12/24/2018, 04:59:31"),
        ("location_2", -1.0, 0.0, "12/24/2018, 04:59:31"),
    ]
    # fmt: on
    expected_run_average_residuals_rows = [("test", 2.0, 0.0)]


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

    reduce_year_by_one_rows = [
        (2024, "some data"),
        (2023, "other data"),
    ]
    expected_reduce_year_by_one_rows = [
        (2023, "some data"),
        (2022, "other data"),
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
            "2",
        ),
        (
            "loc-3",
            "2020-01-01",
            "3",
        ),
        (
            "loc-4",
            "2021-01-01",
            "4",
        ),
        (
            None,
            "2021-01-01",
            "5",
        ),
        (
            None,
            "2021-01-01",
            "6",
        ),
    ]

    location_rows_with_duplicates = [
        *small_location_rows,
        (
            "loc-3",
            "2020-01-01",
            "7",
        ),
        (
            "loc-4",
            "2021-01-01",
            "8",
        ),
    ]

    location_rows_with_different_import_dates = [
        *small_location_rows,
        (
            "loc-3",
            "2021-01-01",
            "3",
        ),
        (
            "loc-4",
            "2022-01-01",
            "4",
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
            "2",
        ),
        (
            None,
            "2021-01-01",
            "5",
        ),
        (
            None,
            "2021-01-01",
            "6",
        ),
    ]

    mupddate_for_org_rows = [
        ("1", date(2024, 3, 1), "1", date(2024, 1, 10)),
        ("1", date(2024, 3, 1), "2", date(2024, 1, 20)),
        ("1", date(2024, 4, 1), "3", date(2024, 3, 10)),
        ("1", date(2024, 4, 1), "4", date(2024, 3, 15)),
        ("2", date(2024, 4, 1), "5", date(2024, 2, 15)),
        ("2", date(2024, 4, 1), "6", date(2024, 3, 10)),
    ]
    expected_mupddate_for_org_rows = [
        ("1", date(2024, 3, 1), "1", date(2024, 1, 10), date(2024, 1, 20)),
        ("1", date(2024, 3, 1), "2", date(2024, 1, 20), date(2024, 1, 20)),
        ("1", date(2024, 4, 1), "3", date(2024, 3, 10), date(2024, 3, 15)),
        ("1", date(2024, 4, 1), "4", date(2024, 3, 15), date(2024, 3, 15)),
        ("2", date(2024, 4, 1), "5", date(2024, 2, 15), date(2024, 3, 10)),
        ("2", date(2024, 4, 1), "6", date(2024, 3, 10), date(2024, 3, 10)),
    ]

    add_purge_data_col_rows = [
        ("1", "Yes", date(2024, 2, 2), date(2024, 2, 2)),
        ("2", "Yes", date(2024, 2, 2), date(2024, 3, 3)),
        ("3", "No", date(2024, 2, 2), date(2024, 2, 2)),
        ("4", "No", date(2024, 2, 2), date(2024, 3, 3)),
    ]
    expected_add_purge_data_col_rows = [
        ("1", "Yes", date(2024, 2, 2), date(2024, 2, 2), date(2024, 2, 2)),
        ("2", "Yes", date(2024, 2, 2), date(2024, 3, 3), date(2024, 3, 3)),
        ("3", "No", date(2024, 2, 2), date(2024, 2, 2), date(2024, 2, 2)),
        ("4", "No", date(2024, 2, 2), date(2024, 3, 3), date(2024, 2, 2)),
    ]

    add_workplace_last_active_date_col_rows = [
        ("1", date(2024, 3, 3), date(2024, 2, 2)),
        ("2", date(2024, 4, 4), date(2024, 5, 5)),
    ]
    expected_add_workplace_last_active_date_col_rows = [
        ("1", date(2024, 3, 3), date(2024, 2, 2), date(2024, 3, 3)),
        ("2", date(2024, 4, 4), date(2024, 5, 5), date(2024, 5, 5)),
    ]

    date_col_for_purging_rows = [
        ("1", date(2024, 3, 3)),
        ("2", date(2024, 4, 4)),
    ]
    expected_date_col_for_purging_rows = [
        ("1", date(2024, 3, 3), date(2022, 3, 3)),
        ("2", date(2024, 4, 4), date(2022, 4, 4)),
    ]

    workplace_last_active_rows = [
        ("1", date(2024, 4, 4), date(2024, 5, 5)),
        ("2", date(2024, 4, 4), date(2024, 4, 4)),
        ("3", date(2024, 4, 4), date(2024, 3, 3)),
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
class PAFilledPostsByIcbArea:
    # fmt: off
    sample_ons_contemporary_with_duplicates_rows = [
        ("AB10AA", date(2024,1,1), "cssr1", "icb1"),
        ("AB10AB", date(2024,1,1), "cssr1", "icb1"),
        ("AB10AC", date(2024,1,1), "cssr1", "icb1"),
        ("AB10AC", date(2024,1,1), "cssr1", "icb1"),
    ]

    sample_ons_contemporary_rows = [
        ("AB10AA", date(2024,1,1), "cssr1", "icb1"),
        ("AB10AB", date(2024,1,1), "cssr1", "icb1"),
        ("AB10AC", date(2024,1,1), "cssr1", "icb1"),
        ("AB10AA", date(2024,1,1), "cssr2", "icb2"), 
        ("AB10AB", date(2024,1,1), "cssr2", "icb3"), 
        ("AB10AC", date(2024,1,1), "cssr2", "icb3"), 
        ("AB10AD", date(2024,1,1), "cssr2", "icb3"), 
        ("AB10AA", date(2023,1,1), "cssr1", "icb1"),
        ("AB10AB", date(2023,1,1), "cssr1", "icb1"),
        ("AB10AC", date(2023,1,1), "cssr1", "icb1"),
        ("AB10AA", date(2023,1,1), "cssr2", "icb2"), 
        ("AB10AB", date(2023,1,1), "cssr2", "icb3"), 
        ("AB10AC", date(2023,1,1), "cssr2", "icb3"), 
    ]

    expected_postcode_count_per_la_rows = [
        ("AB10AA", date(2024,1,1), "cssr1", "icb1",3),
        ("AB10AB", date(2024,1,1), "cssr1", "icb1",3),
        ("AB10AC", date(2024,1,1), "cssr1", "icb1",3),
        ("AB10AA", date(2024,1,1), "cssr2", "icb2",4), 
        ("AB10AB", date(2024,1,1), "cssr2", "icb3",4), 
        ("AB10AC", date(2024,1,1), "cssr2", "icb3",4), 
        ("AB10AD", date(2024,1,1), "cssr2", "icb3",4), 
        ("AB10AA", date(2023,1,1), "cssr1", "icb1",3),
        ("AB10AB", date(2023,1,1), "cssr1", "icb1",3),
        ("AB10AC", date(2023,1,1), "cssr1", "icb1",3),
        ("AB10AA", date(2023,1,1), "cssr2", "icb2",3), 
        ("AB10AB", date(2023,1,1), "cssr2", "icb3",3), 
        ("AB10AC", date(2023,1,1), "cssr2", "icb3",3), 
    ]

    expected_postcode_count_per_la_icb_rows = [
        ("AB10AA", date(2024,1,1), "cssr1", "icb1", 3),
        ("AB10AB", date(2024,1,1), "cssr1", "icb1", 3),
        ("AB10AC", date(2024,1,1), "cssr1", "icb1", 3),
        ("AB10AA", date(2024,1,1), "cssr2", "icb2", 1), 
        ("AB10AB", date(2024,1,1), "cssr2", "icb3", 3), 
        ("AB10AC", date(2024,1,1), "cssr2", "icb3", 3), 
        ("AB10AD", date(2024,1,1), "cssr2", "icb3", 3), 
        ("AB10AA", date(2023,1,1), "cssr1", "icb1", 3),
        ("AB10AB", date(2023,1,1), "cssr1", "icb1", 3),
        ("AB10AC", date(2023,1,1), "cssr1", "icb1", 3),
        ("AB10AA", date(2023,1,1), "cssr2", "icb2", 1), 
        ("AB10AB", date(2023,1,1), "cssr2", "icb3", 2), 
        ("AB10AC", date(2023,1,1), "cssr2", "icb3", 2), 
    ]

    sample_rows_with_la_and_hybrid_area_postcode_counts = [
        (date(2024,1,1), 3, 3),
        (date(2024,1,1), 4, 1), 
        (date(2024,1,1), 4, 3), 
        (date(2023,1,1), 3, 3),
        (date(2023,1,1), 3, 1), 
        (date(2023,1,1), 3, 2),
    ]

    expected_ratio_between_hybrid_area_and_la_area_postcodes_rows = [
        (date(2024,1,1), 3, 3, 1.00000),
        (date(2024,1,1), 4, 1, 0.25000), 
        (date(2024,1,1), 4, 3, 0.75000), 
        (date(2023,1,1), 3, 3, 1.00000),
        (date(2023,1,1), 3, 1, 0.33333), 
        (date(2023,1,1), 3, 2, 0.66666),
    ]

    full_rows_with_la_and_hybrid_area_postcode_counts = [
        ("AB10AA", date(2023,5,1), "cssr1", "icb1", 3, 3, 1.00000),
        ("AB10AB", date(2023,5,1), "cssr1", "icb1", 3, 3, 1.00000),
        ("AB10AA", date(2023,5,1), "cssr2", "icb2", 4, 1, 0.25000), 
        ("AB10AB", date(2023,5,1), "cssr2", "icb3", 4, 3, 0.75000), 
        ("AB10AA", date(2022,5,1), "cssr1", "icb1", 3, 3, 1.00000),
        ("AB10AB", date(2022,5,1), "cssr1", "icb1", 3, 3, 1.00000),
        ("AB10AC", date(2022,5,1), "cssr1", "icb1", 3, 3, 1.00000),
    ]

    expected_deduplicated_import_date_hybrid_and_la_and_ratio_rows = [
        (date(2023,5,1), "cssr1", "icb1", 1.00000),
        (date(2023,5,1), "cssr2", "icb2", 0.25000), 
        (date(2023,5,1), "cssr2", "icb3", 0.75000), 
        (date(2022,5,1), "cssr1", "icb1", 1.00000),
    ]
    # fmt: on

    sample_pa_filled_posts_rows = [
        ("Leeds", 100.2, 2023, "2023"),
        ("Bradford", 200.3, 2023, "2023"),
        ("Hull", 300.3, 2022, "2023"),
    ]

    expected_create_date_column_from_year_in_pa_estimates_rows = [
        ("Leeds", 100.2, 2023, "2023", date(2024, 3, 31)),
        ("Bradford", 200.3, 2023, "2023", date(2024, 3, 31)),
        ("Hull", 300.3, 2022, "2023", date(2023, 3, 31)),
    ]

    sample_postcode_proportions_before_joining_pa_filled_posts_rows = [
        (date(2023, 5, 1), "Leeds", "icb1", 1.00000),
        (date(2023, 5, 1), "Bradford", "icb2", 0.25000),
        (date(2023, 5, 1), "Bradford", "icb3", 0.75000),
        (date(2022, 5, 1), "Leeds", "icb1", 1.00000),
        (date(2022, 5, 1), "Barking & Dagenham", "icb4", 1.00000),
    ]

    sample_pa_filled_posts_prepared_for_joining_to_postcode_proportions_rows = [
        ("Leeds", 100.2, "2023", date(2024, 3, 31)),
        ("Bradford", 200.3, "2023", date(2024, 3, 31)),
        ("Leeds", 300.3, "2022", date(2023, 3, 31)),
        ("Barking and Dagenham", 300.3, "2022", date(2023, 3, 31)),
    ]

    # fmt: off
    expected_postcode_proportions_after_joining_pa_filled_posts_rows = [
        (date(2023,5,1), "Leeds", "icb1", 1.00000, 100.2, "2023"),
        (date(2023,5,1), "Bradford", "icb2", 0.25000, 200.3, "2023"), 
        (date(2023,5,1), "Bradford", "icb3", 0.75000, 200.3, "2023"), 
        (date(2022,5,1), "Leeds", "icb1", 1.00000, 300.3, "2022"),
        (date(2022, 5, 1), "Barking & Dagenham", "icb4", 1.00000, None, None),
    ]

    sample_proportions_and_pa_filled_posts_rows = [
        (0.25000, 100.2),
        (None, 200.3),
        (0.75000, None),
        (None, None),
    ]

    expected_pa_filled_posts_after_applying_proportions_rows = [
        (0.25000, 25.05000),
        (None, None),
        (0.75000, None),
        (None, None),
    ]
    # fmt: on

    sample_la_name_rows = [
        ("Bath & N E Somerset",),
        ("Southend",),
        ("Bedford",),
        (None,),
    ]

    expected_la_names_with_correct_spelling_rows = [
        ("Bath and North East Somerset",),
        ("Southend on Sea",),
        ("Bedford",),
        (None,),
    ]


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
    remove_unused_pir_types_rows = add_care_home_column_rows
    expected_remove_unused_pir_types_rows = [
        ("loc 1", "Residential"),
        ("loc 4", "Community"),
    ]

    remove_rows_missing_people_directly_employed = [
        ("loc_1", 1),
        ("loc_1", 0),
        ("loc_1", None),
    ]

    expected_remove_rows_missing_people_directly_employed = [
        ("loc_1", 1),
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
            "2020-01-01",
        ),
    ]

    list_of_services_rows = [
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


@dataclass
class UtilsData:
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
class MergeCoverageData:
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
        ("1-000000001", "2024-01-01", "Good", 0),
        ("1-000000001", "2024-01-02", "Good", 1),
        ("1-000000001", None, "Good", None),
        ("1-000000002", "2024-01-01", None, 1),
    ]

    # fmt: off
    expected_cqc_locations_and_latest_cqc_rating_rows = [
        ("1-000000001", "2024-01-02", "Good",),
        ("1-000000002", "2024-01-01", None,),
    ]
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
            "Workplace",
            "Reports",
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
class FilterAscwdsFilledPostsData:
    input_rows = [
        ("01", date(2023, 1, 1), "Y", 25, 1.0),
        ("02", date(2023, 1, 1), "Y", 25, 2.0),
        ("03", date(2023, 1, 1), "Y", 25, 3.0),
    ]


@dataclass
class RemoveCareHomeFilledPostsPerBedRatioOutliersData:
    # fmt: off
    unfiltered_ind_cqc_rows = [
        ("01", date(2023, 1, 1), "Y", 25, 1.0, 1.0),
        ("02", date(2023, 1, 1), "Y", 25, 2.0, 2.0),
        ("03", date(2023, 1, 1), "Y", 25, 3.0, 3.0),
        ("04", date(2023, 1, 1), "Y", 25, 4.0, 4.0),
        ("05", date(2023, 1, 1), "Y", 25, 5.0, 5.0),
        ("06", date(2023, 1, 1), "Y", 25, 6.0, 6.0),
        ("07", date(2023, 1, 1), "Y", 25, 7.0, 7.0),
        ("08", date(2023, 1, 1), "Y", 25, 8.0, 8.0),
        ("09", date(2023, 1, 1), "Y", 25, 9.0, 9.0),
        ("10", date(2023, 1, 1), "Y", 25, 10.0, 10.0),
        ("11", date(2023, 1, 1), "Y", 25, 11.0, 11.0),
        ("12", date(2023, 1, 1), "Y", 25, 12.0, 12.0),
        ("13", date(2023, 1, 1), "Y", 25, 13.0, 13.0),
        ("14", date(2023, 1, 1), "Y", 25, 14.0, 14.0),
        ("15", date(2023, 1, 1), "Y", 25, 15.0, 15.0),
        ("16", date(2023, 1, 1), "Y", 25, 16.0, 16.0),
        ("17", date(2023, 1, 1), "Y", 25, 17.0, 17.0),
        ("18", date(2023, 1, 1), "Y", 25, 18.0, 18.0),
        ("19", date(2023, 1, 1), "Y", 25, 19.0, 19.0),
        ("20", date(2023, 1, 1), "Y", 25, 20.0, 20.0),
        ("21", date(2023, 1, 1), "Y", 25, 21.0, 21.0),
        ("22", date(2023, 1, 1), "Y", 25, 22.0, 22.0),
        ("23", date(2023, 1, 1), "Y", 25, 23.0, 23.0),
        ("24", date(2023, 1, 1), "Y", 25, 24.0, 24.0),
        ("25", date(2023, 1, 1), "Y", 25, 25.0, 25.0),
        ("26", date(2023, 1, 1), "Y", 25, 26.0, 26.0),
        ("27", date(2023, 1, 1), "Y", 25, 27.0, 27.0),
        ("28", date(2023, 1, 1), "Y", 25, 28.0, 28.0),
        ("29", date(2023, 1, 1), "Y", 25, 29.0, 29.0),
        ("30", date(2023, 1, 1), "Y", 25, 30.0, 30.0),
        ("31", date(2023, 1, 1), "Y", 25, 31.0, 31.0),
        ("32", date(2023, 1, 1), "Y", 25, 32.0, 32.0),
        ("33", date(2023, 1, 1), "Y", 25, 33.0, 33.0),
        ("34", date(2023, 1, 1), "Y", 25, 34.0, 34.0),
        ("35", date(2023, 1, 1), "Y", 25, 35.0, 35.0),
        ("36", date(2023, 1, 1), "Y", 25, 36.0, 36.0),
        ("37", date(2023, 1, 1), "Y", 25, 37.0, 37.0),
        ("38", date(2023, 1, 1), "Y", 25, 38.0, 38.0),
        ("39", date(2023, 1, 1), "Y", 25, 39.0, 39.0),
        ("40", date(2023, 1, 1), "Y", 25, 40.0, 40.0),
        ("41", date(2023, 1, 1), "Y", 25, None, None),
        ("42", date(2023, 1, 1), "Y", None, 42.0, 42.0),
        ("43", date(2023, 1, 1), "N", 25, 43.0, 43.0),
        ("44", date(2023, 1, 1), "N", None, 44.0, 44.0),
    ]
    # fmt: on

    # fmt: off
    expected_care_home_jobs_per_bed_ratio_filtered_rows = [
        ("01", date(2023, 1, 1), "Y", 25, 1.0, None),
        ("02", date(2023, 1, 1), "Y", 25, 2.0, 2.0),
        ("03", date(2023, 1, 1), "Y", 25, 3.0, 3.0),
        ("04", date(2023, 1, 1), "Y", 25, 4.0, 4.0),
        ("05", date(2023, 1, 1), "Y", 25, 5.0, 5.0),
        ("06", date(2023, 1, 1), "Y", 25, 6.0, 6.0),
        ("07", date(2023, 1, 1), "Y", 25, 7.0, 7.0),
        ("08", date(2023, 1, 1), "Y", 25, 8.0, 8.0),
        ("09", date(2023, 1, 1), "Y", 25, 9.0, 9.0),
        ("10", date(2023, 1, 1), "Y", 25, 10.0, 10.0),
        ("11", date(2023, 1, 1), "Y", 25, 11.0, 11.0),
        ("12", date(2023, 1, 1), "Y", 25, 12.0, 12.0),
        ("13", date(2023, 1, 1), "Y", 25, 13.0, 13.0),
        ("14", date(2023, 1, 1), "Y", 25, 14.0, 14.0),
        ("15", date(2023, 1, 1), "Y", 25, 15.0, 15.0),
        ("16", date(2023, 1, 1), "Y", 25, 16.0, 16.0),
        ("17", date(2023, 1, 1), "Y", 25, 17.0, 17.0),
        ("18", date(2023, 1, 1), "Y", 25, 18.0, 18.0),
        ("19", date(2023, 1, 1), "Y", 25, 19.0, 19.0),
        ("20", date(2023, 1, 1), "Y", 25, 20.0, 20.0),
        ("21", date(2023, 1, 1), "Y", 25, 21.0, 21.0),
        ("22", date(2023, 1, 1), "Y", 25, 22.0, 22.0),
        ("23", date(2023, 1, 1), "Y", 25, 23.0, 23.0),
        ("24", date(2023, 1, 1), "Y", 25, 24.0, 24.0),
        ("25", date(2023, 1, 1), "Y", 25, 25.0, 25.0),
        ("26", date(2023, 1, 1), "Y", 25, 26.0, 26.0),
        ("27", date(2023, 1, 1), "Y", 25, 27.0, 27.0),
        ("28", date(2023, 1, 1), "Y", 25, 28.0, 28.0),
        ("29", date(2023, 1, 1), "Y", 25, 29.0, 29.0),
        ("30", date(2023, 1, 1), "Y", 25, 30.0, 30.0),
        ("31", date(2023, 1, 1), "Y", 25, 31.0, 31.0),
        ("32", date(2023, 1, 1), "Y", 25, 32.0, 32.0),
        ("33", date(2023, 1, 1), "Y", 25, 33.0, 33.0),
        ("34", date(2023, 1, 1), "Y", 25, 34.0, 34.0),
        ("35", date(2023, 1, 1), "Y", 25, 35.0, 35.0),
        ("36", date(2023, 1, 1), "Y", 25, 36.0, 36.0),
        ("37", date(2023, 1, 1), "Y", 25, 37.0, 37.0),
        ("38", date(2023, 1, 1), "Y", 25, 38.0, 38.0),
        ("39", date(2023, 1, 1), "Y", 25, 39.0, 39.0),
        ("40", date(2023, 1, 1), "Y", 25, 40.0, None),
        ("41", date(2023, 1, 1), "Y", 25, None, None),
        ("42", date(2023, 1, 1), "Y", None, 42.0, 42.0),
        ("43", date(2023, 1, 1), "N", 25, 43.0, 43.0),
        ("44", date(2023, 1, 1), "N", None, 44.0, 44.0),
    ]
    # fmt: on

    standardised_residual_percentile_cutoff_rows = [
        ("1", "Y", 0.54),
        ("2", "Y", -3.2545),
        ("3", "Y", -4.25423),
        ("4", "Y", 2.41654),
        ("5", "Y", 25.0),
    ]

    expected_standardised_residual_percentile_cutoff_with_percentiles_rows = [
        ("1", "Y", 0.54, -3.45, 6.93),
        ("2", "Y", -3.2545, -3.45, 6.93),
        ("3", "Y", -4.25423, -3.45, 6.93),
        ("4", "Y", 2.41654, -3.45, 6.93),
        ("5", "Y", 25.0, -3.45, 6.93),
    ]

    null_values_below_standardised_residual_cutoff_rows = [
        ("1", 1.0, -2.50, -1.23, 1.23),
        ("2", 2.0, -1.23, -1.23, 1.23),
        ("3", 3.0, 0.00, -1.23, 1.23),
        ("4", 2.0, 1.23, -1.23, 1.23),
        ("5", 1.0, 1.25, -1.23, 1.23),
    ]

    expected_null_values_below_standardised_residual_cutoff_rows = [
        ("1", None, -2.50, -1.23, 1.23),
        ("2", 2.0, -1.23, -1.23, 1.23),
        ("3", 3.0, 0.00, -1.23, 1.23),
        ("4", 2.0, 1.23, -1.23, 1.23),
        ("5", None, 1.25, -1.23, 1.23),
    ]


@dataclass
class NonResAscwdsWithDormancyFeaturesData(object):
    # fmt: off
    rows = [
        ("1-00001", date(2022, 2, 1), date(2019, 2, 1), "South East", "Y", ["Domiciliary care service"], "non-residential", None, "N", "Rural hamlet and isolated dwellings in a sparse setting", '2022', '02', '01', '20220201'),
        ("1-00002", date(2022, 1, 1), date(2019, 2, 1), "South East", "N", ["Domiciliary care service"], "non-residential", 67.0, "N", "Rural hamlet and isolated dwellings in a sparse setting", '2022', '01', '01', '20220101'),
        ("1-00003", date(2022, 1, 2), date(2019, 2, 1), "South West", "Y", ["Urgent care services", "Supported living service"], "non-residential", None, "N", "Rural hamlet and isolated dwellings", '2022', '01', '12', '20220112'),
        ("1-00004", date(2022, 1, 2), date(2019, 2, 1), "North East", "Y", ["Hospice services at home"], "non-residential", None, "N", "Rural hamlet and isolated dwellings", '2022', '01', '12', '20220212'),
        ("1-00005", date(2022, 3, 1), date(2019, 2, 1), "North East", "N", ["Specialist college service", "Community based services for people who misuse substances", "Urgent care services'"], "non-residential", None, "N", "Urban city and town", '2022', '03', '01', '20220301'),
        ("1-00006", date(2022, 3, 8), date(2019, 2, 1), "South West", None, ["Specialist college service"], "non-residential", None, "N", "Rural town and fringe in a sparse setting", '2022', '03', '08', '20220308'),
        ("1-00007", date(2022, 3, 8), date(2019, 2, 1), "North East", "Y", ["Care home service with nursing"], "Care home with nursing", None, "Y", "Urban city and town", '2022', '03', '08', '20220308'),
        ("1-00008", date(2022, 3, 8), date(2019, 2, 1), "North East", "Y", ["Care home service with nursing"], "Care home with nursing", 25.0, "Y", "Urban city and town", '2022', '03', '08', '20220308'),
        ("1-00009", date(2022, 3, 9), date(2019, 2, 1), "North West", None, ["Care home service without nursing"], "Care home without nursing", None, "Y", "Urban city and town", '2022', '03', '15', '20220315'),
        ("1-00010", date(2022, 4, 2), date(2019, 2, 1), "North West", "Y", ["Supported living service", "Acute services with overnight beds"], "non-residential", None, "N", "Urban city and town", '2022', '04', '22', '20220422'),
    ]
    # fmt: on

    filter_to_non_care_home_rows = [
        ("Y", Sector.independent),
        ("N", Sector.independent),
    ]

    expected_filtered_to_non_care_home_rows = [
        ("N", Sector.independent),
    ]

    filter_to_dormancy_rows = [
        ("1-00001", "Y"),
        ("1-00002", None),
        ("1-00003", "N"),
    ]

    expected_filtered_to_dormancy_rows = [
        ("1-00001", "Y"),
        ("1-00003", "N"),
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
        ("Y", Sector.independent),
        ("N", Sector.independent),
    ]

    expected_filtered_to_care_home_rows = rows = [
        ("Y", Sector.independent),
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
class ModelPrimaryServiceRollingAverage:
    # fmt: off
    input_rows = [
        ("1-000000001", "2023-01-01", 1672531200, 4.0, PrimaryServiceType.non_residential),
        ("1-000000002", "2023-01-01", 1672531200, 6.0, PrimaryServiceType.non_residential),
        ("1-000000003", "2023-02-01", 1675209600, 20.0, PrimaryServiceType.non_residential),
        ("1-000000004", "2023-03-01", 1677628800, 30.0, PrimaryServiceType.non_residential),
        ("1-000000005", "2023-04-01", 1680303600, 40.0, PrimaryServiceType.non_residential),
        ("1-000000006", "2023-01-01", 1672531200, 5.0, PrimaryServiceType.non_residential),
        ("1-000000007", "2023-02-01", 1675209600, None, PrimaryServiceType.non_residential),
        ("1-000000008", "2023-03-01", 1677628800, None, PrimaryServiceType.non_residential),
        ("1-000000009", "2023-04-01", 1680303600, None, PrimaryServiceType.non_residential),
        ("1-000000010", "2023-05-01", 1682895600, None, PrimaryServiceType.non_residential),
        ("1-000000011", "2023-01-01", 1672531200, 14.0, PrimaryServiceType.care_home_with_nursing),
        ("1-000000012", "2023-01-01", 1672531200, 16.0, PrimaryServiceType.care_home_with_nursing),
        ("1-000000013", "2023-02-01", 1675209600, 120.0, PrimaryServiceType.care_home_with_nursing),
        ("1-000000014", "2023-03-01", 1677628800, 131.0, PrimaryServiceType.care_home_with_nursing),
        ("1-000000015", "2023-04-01", 1680303600, 142.0, PrimaryServiceType.care_home_with_nursing),
        ("1-000000016", "2023-01-01", 1672531200, 15.0, PrimaryServiceType.care_home_with_nursing),
        ("1-000000017", "2023-02-01", 1675209600, None, PrimaryServiceType.care_home_with_nursing),
        ("1-000000018", "2023-03-01", 1677628800, None, PrimaryServiceType.care_home_with_nursing),
        ("1-000000019", "2023-04-01", 1680303600, None, PrimaryServiceType.care_home_with_nursing),
        ("1-000000020", "2023-05-01", 1682895600, None, PrimaryServiceType.care_home_with_nursing),
    ]
    expected_rolling_average_rows = [
        ("1-000000001", "2023-01-01", 1672531200, 4.0, PrimaryServiceType.non_residential, 5.0),
        ("1-000000002", "2023-01-01", 1672531200, 6.0, PrimaryServiceType.non_residential, 5.0),
        ("1-000000003", "2023-02-01", 1675209600, 20.0, PrimaryServiceType.non_residential, 8.75),
        ("1-000000004", "2023-03-01", 1677628800, 30.0, PrimaryServiceType.non_residential, 13.0),
        ("1-000000005", "2023-04-01", 1680303600, 40.0, PrimaryServiceType.non_residential, 30.0),
        ("1-000000006", "2023-01-01", 1672531200, 5.0, PrimaryServiceType.non_residential, 5.0),
        ("1-000000007", "2023-02-01", 1675209600, None, PrimaryServiceType.non_residential, 8.75),
        ("1-000000008", "2023-03-01", 1677628800, None, PrimaryServiceType.non_residential, 13.0),
        ("1-000000009", "2023-04-01", 1680303600, None, PrimaryServiceType.non_residential, 30.0),
        ("1-000000010", "2023-05-01", 1682895600, None, PrimaryServiceType.non_residential, 35.0),
        ("1-000000011", "2023-01-01", 1672531200, 14.0, PrimaryServiceType.care_home_with_nursing, 15.0),
        ("1-000000012", "2023-01-01", 1672531200, 16.0, PrimaryServiceType.care_home_with_nursing, 15.0),
        ("1-000000013", "2023-02-01", 1675209600, 120.0, PrimaryServiceType.care_home_with_nursing, 41.25),
        ("1-000000014", "2023-03-01", 1677628800, 131.0, PrimaryServiceType.care_home_with_nursing, 59.2),
        ("1-000000015", "2023-04-01", 1680303600, 142.0, PrimaryServiceType.care_home_with_nursing, 131.0),
        ("1-000000016", "2023-01-01", 1672531200, 15.0, PrimaryServiceType.care_home_with_nursing, 15.0),
        ("1-000000017", "2023-02-01", 1675209600, None, PrimaryServiceType.care_home_with_nursing, 41.25),
        ("1-000000018", "2023-03-01", 1677628800, None, PrimaryServiceType.care_home_with_nursing, 59.2),
        ("1-000000019", "2023-04-01", 1680303600, None, PrimaryServiceType.care_home_with_nursing, 131.0),
        ("1-000000020", "2023-05-01", 1682895600, None, PrimaryServiceType.care_home_with_nursing, 136.5),
    ]
    # fmt: on
    add_flag_rows = [
        ("1-000000001", 4.0),
        ("1-000000002", None),
    ]
    expected_add_flag_rows = [
        ("1-000000001", 4.0, 1),
        ("1-000000002", None, 0),
    ]
    rolling_sum_rows = [
        (PrimaryServiceType.care_home_with_nursing, 1672531200, 30.0),
        (PrimaryServiceType.care_home_with_nursing, 1675209600, 120.0),
        (PrimaryServiceType.care_home_with_nursing, 1677628800, None),
        (PrimaryServiceType.care_home_with_nursing, 1680303600, 142.0),
        (PrimaryServiceType.non_residential, 1672531200, 10.0),
        (PrimaryServiceType.non_residential, 1675209600, 20.0),
        (PrimaryServiceType.non_residential, 1677628800, None),
        (PrimaryServiceType.non_residential, 1680303600, 40.0),
    ]
    expected_rolling_sum_rows = [
        (PrimaryServiceType.care_home_with_nursing, 1672531200, 30.0, 30.0),
        (PrimaryServiceType.care_home_with_nursing, 1675209600, 120.0, 150.0),
        (PrimaryServiceType.care_home_with_nursing, 1677628800, None, 150.0),
        (PrimaryServiceType.care_home_with_nursing, 1680303600, 142.0, 262.0),
        (PrimaryServiceType.non_residential, 1672531200, 10.0, 10.0),
        (PrimaryServiceType.non_residential, 1675209600, 20.0, 30.0),
        (PrimaryServiceType.non_residential, 1677628800, None, 30.0),
        (PrimaryServiceType.non_residential, 1680303600, 40.0, 60.0),
    ]
    calculate_rolling_average_column_rows = [
        (PrimaryServiceType.care_home_with_nursing, 1672531200, 1, 30.0),
        (PrimaryServiceType.care_home_with_nursing, 1675209600, 1, 120.0),
        (PrimaryServiceType.care_home_with_nursing, 1677628800, 0, None),
        (PrimaryServiceType.care_home_with_nursing, 1680303600, 1, 142.0),
        (PrimaryServiceType.non_residential, 1672531200, 1, 10.0),
        (PrimaryServiceType.non_residential, 1675209600, 1, 20.0),
        (PrimaryServiceType.non_residential, 1677628800, 0, None),
        (PrimaryServiceType.non_residential, 1680303600, 1, 40.0),
    ]
    expected_calculate_rolling_average_column_rows = [
        (PrimaryServiceType.care_home_with_nursing, 1672531200, 30.0, 30.0),
        (PrimaryServiceType.care_home_with_nursing, 1675209600, 120.0, 75.0),
        (PrimaryServiceType.care_home_with_nursing, 1677628800, None, 75.0),
        (PrimaryServiceType.care_home_with_nursing, 1680303600, 142.0, 131.0),
        (PrimaryServiceType.non_residential, 1672531200, 10.0, 10.0),
        (PrimaryServiceType.non_residential, 1675209600, 20.0, 15.0),
        (PrimaryServiceType.non_residential, 1677628800, None, 15.0),
        (PrimaryServiceType.non_residential, 1680303600, 40.0, 30.0),
    ]


@dataclass
class ModelExtrapolation:
    extrapolation_rows = [
        ("1-000000001", "2023-01-01", 1672531200, 15.0, "Care home with nursing", 15.0),
        ("1-000000001", "2023-02-01", 1675209600, None, "Care home with nursing", 15.1),
        ("1-000000001", "2023-03-01", 1677628800, 30.0, "Care home with nursing", 15.2),
        ("1-000000002", "2023-01-01", 1672531200, 4.0, "non-residential", 50.3),
        ("1-000000002", "2023-02-01", 1675209600, None, "non-residential", 50.5),
        ("1-000000002", "2023-03-01", 1677628800, None, "non-residential", 50.7),
        ("1-000000002", "2023-04-01", 1680303600, None, "non-residential", 50.1),
        ("1-000000003", "2023-01-01", 1672531200, None, "non-residential", 50.3),
        ("1-000000003", "2023-02-01", 1675209600, 20.0, "non-residential", 50.5),
        ("1-000000003", "2023-03-01", 1677628800, None, "non-residential", 50.7),
        ("1-000000004", "2023-03-01", 1677628800, None, "non-residential", 50.7),
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
    add_time_registered_rows = [
        (date(2013, 1, 10), date(2023, 1, 10)),
    ]
    expected_add_time_registered_rows = [
        (date(2013, 1, 10), date(2023, 1, 10), 3652),
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
        ("1-000000001", date(2023, 1, 1), 1672531200, None),
        ("1-000000001", date(2023, 1, 2), 1672617600, 30.0),
        ("1-000000001", date(2023, 1, 3), 1672704000, None),
        ("1-000000002", date(2023, 1, 1), 1672531200, None),
        ("1-000000002", date(2023, 1, 3), 1672704000, 4.0),
        ("1-000000002", date(2023, 1, 5), 1672876800, None),
        ("1-000000002", date(2023, 1, 7), 1673049600, 5.0),
        ("1-000000002", date(2023, 1, 9), 1673222400, 5.0),
        ("1-000000002", date(2023, 1, 11), 1673395200, None),
        ("1-000000002", date(2023, 1, 13), 1673568000, None),
        ("1-000000002", date(2023, 1, 15), 1673740800, 20.0),
        ("1-000000002", date(2023, 1, 17), 1673913600, None),
        ("1-000000002", date(2023, 1, 19), 1674086400, None),
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
    ]

    add_current_or_historic_rows = [
        ("loc_1",),
    ]
    expected_add_current_rows = [
        ("loc_1", CQCRatingsValues.current),
    ]
    expected_add_historic_rows = [
        ("loc_1", CQCRatingsValues.historic),
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
        ),
    ]
    expected_create_standard_rating_dataset_rows = [
        ("loc_1", "2024-01-01", "Good", "Good", "Good", "Good", "Good", "Good", 1, 1),
    ]
    select_ratings_for_benchmarks_rows = [
        ("loc_1", RegistrationStatus.registered, CQCRatingsValues.current),
        ("loc_2", RegistrationStatus.registered, CQCRatingsValues.historic),
        ("loc_3", RegistrationStatus.deregistered, CQCRatingsValues.current),
        ("loc_4", RegistrationStatus.deregistered, CQCRatingsValues.historic),
    ]
    expected_select_ratings_for_benchmarks_rows = [
        ("loc_1", RegistrationStatus.registered, CQCRatingsValues.current),
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
            "UniquenessConstraint(Uniqueness(Stream(locationId, ?),None))",
            "Success",
            "",
        ),
    ]
    unique_index_columns_result_not_unique_rows = [
        (
            "Index columns are unique",
            "Warning",
            "Warning",
            "UniquenessConstraint(Uniqueness(Stream(locationId, ?),None))",
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
            "CompletenessConstraint(Completeness(locationId,None))",
            "Success",
            "",
        ),
    ]
    one_complete_column_result_incomplete_rows = [
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(locationId,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Completeness of locationId should be 1.",
        ),
    ]
    two_complete_columns_result_both_complete_rows = [
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(locationId,None))",
            "Success",
            "",
        ),
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None))",
            "Success",
            "",
        ),
    ]
    two_complete_columns_result_one_incomplete_rows = [
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(locationId,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Completeness of locationId should be 1.",
        ),
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None))",
            "Success",
            "",
        ),
    ]
    two_complete_columns_result_both_incomplete_rows = [
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(locationId,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! Completeness of locationId should be 1.",
        ),
        (
            "Column is complete",
            "Warning",
            "Warning",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None))",
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
            "UniquenessConstraint(Uniqueness(Stream(locationId, ?),None))",
            "Success",
            "",
        ),
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(locationId,None))",
            "Success",
            "",
        ),
        (
            "Column is complete",
            "Warning",
            "Success",
            "CompletenessConstraint(Completeness(cqc_location_import_date,None))",
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
            IndCQC.people_directly_employed: 0,
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
            "MinimumConstraint(Minimum(numberOfBeds,None))",
            "Success",
            "",
        ),
    ]
    min_values_result_below_minimum_rows = [
        (
            "Min value in column",
            "Warning",
            "Warning",
            "MinimumConstraint(Minimum(numberOfBeds,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! The minimum value for numberOfBeds should be 1.",
        ),
    ]
    min_values_result_multiple_columns_rows = [
        (
            "Min value in column",
            "Warning",
            "Warning",
            "MinimumConstraint(Minimum(numberOfBeds,None))",
            "Failure",
            "Value: 0.0 does not meet the constraint requirement! The minimum value for numberOfBeds should be 1.",
        ),
        (
            "Min value in column",
            "Warning",
            "Success",
            "MinimumConstraint(Minimum(people_directly_employed,None))",
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
            IndCQC.people_directly_employed: 20,
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
            "MaximumConstraint(Maximum(numberOfBeds,None))",
            "Success",
            "",
        ),
    ]
    max_values_result_above_maximum_rows = [
        (
            "Max value in column",
            "Warning",
            "Warning",
            "MaximumConstraint(Maximum(numberOfBeds,None))",
            "Failure",
            "Value: 11.0 does not meet the constraint requirement! The maximum value for numberOfBeds should be 10.",
        ),
    ]
    max_values_result_multiple_columns_rows = [
        (
            "Max value in column",
            "Warning",
            "Warning",
            "MaximumConstraint(Maximum(numberOfBeds,None))",
            "Failure",
            "Value: 20.0 does not meet the constraint requirement! The maximum value for numberOfBeds should be 10.",
        ),
        (
            "Max value in column",
            "Warning",
            "Success",
            "MaximumConstraint(Maximum(people_directly_employed,None))",
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
            "ComplianceConstraint(Compliance(cqc_sector contained in Independent,Local authority,`cqc_sector` IS NULL OR `cqc_sector` IN ('Independent','Local authority'),None,List(cqc_sector)))",
            "Success",
            "",
        ),
    ]
    categorical_values_result_failure_rows = [
        (
            "Categorical values are in list of expected values",
            "Warning",
            "Warning",
            "ComplianceConstraint(Compliance(cqc_sector contained in Independent,Local authority,`cqc_sector` IS NULL OR `cqc_sector` IN ('Independent','Local authority'),None,List(cqc_sector)))",
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


@dataclass
class ValidateLocationsAPICleanedData:
    # fmt: off
    raw_cqc_locations_rows = [
        ("1-000000001", "20240101", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}]),
        ("1-000000002", "20240101", LocationType.social_care_identifier, RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}]),
        ("1-000000001", "20240201", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}]),
        ("1-000000002", "20240201", "not social care org", RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}]),
    ]

    cleaned_cqc_locations_rows = [
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None),
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", None),
    ]
    

    calculate_expected_size_rows = [
        ("loc_1", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}]),
        ("loc_2", "non social care org", RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}]),
        ("loc_3", None, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}]),
        ("loc_4", LocationType.social_care_identifier, RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}]),
        ("loc_5", "non social care org", RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}]),
        ("loc_6", None, RegistrationStatus.deregistered,  [{CQCL.name: "name", CQCL.description: Services.care_home_service_with_nursing}]),
        ("loc_7", LocationType.social_care_identifier, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}]),
        ("loc_8", "non social care org", RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}]),
        ("loc_9", None, RegistrationStatus.registered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}]),
        ("loc_10", LocationType.social_care_identifier, RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}]),
        ("loc_11", "non social care org", RegistrationStatus.deregistered, [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}]),
        ("loc_12", None, RegistrationStatus.deregistered,  [{CQCL.name: "name", CQCL.description: Services.specialist_college_service}]),
        (RecordsToRemoveInLocationsData.dental_practice, LocationType.social_care_identifier, RegistrationStatus.registered, None),
        (RecordsToRemoveInLocationsData.temp_registration, LocationType.social_care_identifier, RegistrationStatus.registered, None),
    ]
    # fmt: on


@dataclass
class ValidateProvidersAPICleanedData:
    # fmt: off
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
    # fmt: on

    calculate_expected_size_rows = raw_cqc_providers_rows


@dataclass
class ValidatePIRCleanedData:
    # fmt: off
    cleaned_cqc_pir_rows = [
        ("1-000000001", date(2024, 1, 1), 10, "Y"),
        ("1-000000002", date(2024, 1, 1), 10, "Y"),
        ("1-000000001", date(2024, 1, 9), 10, "Y"),
        ("1-000000002", date(2024, 1, 9), 10, "Y"),
    ]
    # fmt: on


@dataclass
class ValidateASCWDSWorkplaceCleanedData:
    # fmt: off
    cleaned_ascwds_workplace_rows = [
        ("estab_1", date(2024, 1, 1), "org_id", "location_id", 10, 10),
        ("estab_2", date(2024, 1, 1), "org_id", "location_id", 10, 10),
        ("estab_1", date(2024, 1, 9), "org_id", "location_id", 10, 10),
        ("estab_2", date(2024, 1, 9), "org_id", "location_id", 10, 10),
    ]
    # fmt: on


@dataclass
class ValidateASCWDSWorkerCleanedData:
    # fmt: off
    cleaned_ascwds_worker_rows = [
        ("estab_1", date(2024, 1, 1), "worker_1", "8", "Care Worker"),
        ("estab_2", date(2024, 1, 1), "worker_2", "8", "Care Worker"),
        ("estab_1", date(2024, 1, 9), "worker_3", "8", "Care Worker"),
        ("estab_2", date(2024, 1, 9), "worker_4", "8", "Care Worker"),
    ]
    # fmt: on


@dataclass
class ValidatePostcodeDirectoryCleanedData:
    # fmt: off
    raw_postcode_directory_rows = [
        ("AB1 2CD", "20240101"),
        ("AB2 2CD", "20240101"),
        ("AB1 2CD", "20240201"),
        ("AB2 2CD", "20240201"),
    ]

    cleaned_postcode_directory_rows = [
        ("AB1 2CD", date(2024, 1, 1), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
        ("AB2 2CD", date(2024, 1, 1), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
        ("AB1 2CD", date(2024, 1, 9), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
        ("AB2 2CD", date(2024, 1, 9), "cssr", "region", date(2024, 1, 9), "cssr", "region", "rui"),
    ]
    # fmt: on

    calculate_expected_size_rows = raw_postcode_directory_rows


@dataclass
class ValidateCleanedIndCqcData:
    # fmt: off
    merged_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1),),
        ("1-000000002", date(2024, 1, 1),),
        ("1-000000001", date(2024, 2, 1),),
        ("1-000000002", date(2024, 2, 1),),
    ]

    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5, "source", 5.0, 5.0, 5.0, 5),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5, "source", 5.0, 5.0, 5.0, 5),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5, "source", 5.0, 5.0, 5.0, 5),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5, "source", 5.0, 5.0, 5.0, 5),
    ]
    # fmt: on

    calculate_expected_size_rows = [
        (
            "1-000000001",
            date(2024, 1, 1),
        ),
    ]


@dataclass
class ValidateCareHomeIndCqcFeaturesData:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1), PrimaryServiceType.care_home_only),
        ("1-000000002", date(2024, 1, 1), PrimaryServiceType.care_home_only),
        ("1-000000001", date(2024, 1, 9), PrimaryServiceType.care_home_only),
        ("1-000000002", date(2024, 1, 9), PrimaryServiceType.care_home_only),
    ]

    care_home_ind_cqc_features_rows = [
        ("1-000000001", date(2024, 1, 1), "region", 5, 5, "Y", "features", 5.0),
        ("1-000000002", date(2024, 1, 1), "region", 5, 5, "Y", "features", 5.0),
        ("1-000000001", date(2024, 1, 9), "region", 5, 5, "Y", "features", 5.0),
        ("1-000000002", date(2024, 1, 9), "region", 5, 5, "Y", "features", 5.0),
    ]

    calculate_expected_size_rows = [
        ("1-000000001", date(2024, 1, 1), CareHome.care_home),
        ("1-000000002", date(2024, 1, 1), CareHome.care_home),
        ("1-000000001", date(2024, 1, 9), CareHome.not_care_home),
        ("1-000000002", date(2024, 1, 9), None),
    ]
    # fmt: on


@dataclass
class ValidateNonResASCWDSIncDormancyIndCqcFeaturesData:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1), PrimaryServiceType.non_residential, "Y"),
        ("1-000000002", date(2024, 1, 1), PrimaryServiceType.non_residential, "Y"),
        ("1-000000001", date(2024, 1, 9), PrimaryServiceType.non_residential, "Y"),
        ("1-000000002", date(2024, 1, 9), PrimaryServiceType.non_residential, "Y"),
    ]

    non_res_ascwds_inc_dormancy_ind_cqc_features_rows = [
        ("1-000000001", date(2024, 1, 1),),
        ("1-000000002", date(2024, 1, 1),),
        ("1-000000001", date(2024, 1, 9),),
        ("1-000000002", date(2024, 1, 9),),
    ]

    calculate_expected_size_rows = [
        ("1-000000001", date(2024, 1, 1), PrimaryServiceType.care_home_only, "Y"),
        ("1-000000002", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, "Y"),
        ("1-000000001", date(2024, 1, 9), PrimaryServiceType.non_residential, "Y"),
        ("1-000000002", date(2024, 1, 9), None, "Y"),
        ("1-000000003", date(2024, 1, 1), PrimaryServiceType.care_home_only, "N"),
        ("1-000000004", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, "N"),
        ("1-000000003", date(2024, 1, 9), PrimaryServiceType.non_residential, "N"),
        ("1-000000004", date(2024, 1, 9), None, "N"),
        ("1-000000005", date(2024, 1, 1), PrimaryServiceType.care_home_only, None),
        ("1-000000006", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, None),
        ("1-000000005", date(2024, 1, 9), PrimaryServiceType.non_residential, None),
        ("1-000000006", date(2024, 1, 9), None, None),
    ]
    # fmt: on


@dataclass
class ValidateNonResASCWDSWithoutDormancyIndCqcFeaturesData:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1), PrimaryServiceType.non_residential, None),
        ("1-000000002", date(2024, 1, 1), PrimaryServiceType.non_residential, None),
        ("1-000000001", date(2024, 1, 9), PrimaryServiceType.non_residential, None),
        ("1-000000002", date(2024, 1, 9), PrimaryServiceType.non_residential, None),
    ]

    non_res_ascwds_without_dormancy_ind_cqc_features_rows = [
        ("1-000000001", date(2024, 1, 1),),
        ("1-000000002", date(2024, 1, 1),),
        ("1-000000001", date(2024, 1, 9),),
        ("1-000000002", date(2024, 1, 9),),
    ]

    calculate_expected_size_rows = [
        ("1-000000001", date(2024, 1, 1), PrimaryServiceType.care_home_only, "Y"),
        ("1-000000002", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, "Y"),
        ("1-000000001", date(2024, 1, 9), PrimaryServiceType.non_residential, "Y"),
        ("1-000000002", date(2024, 1, 9), None, "Y"),
        ("1-000000003", date(2024, 1, 1), PrimaryServiceType.care_home_only, "N"),
        ("1-000000004", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, "N"),
        ("1-000000003", date(2024, 1, 9), PrimaryServiceType.non_residential, "N"),
        ("1-000000004", date(2024, 1, 9), None, "N"),
        ("1-000000005", date(2024, 1, 1), PrimaryServiceType.care_home_only, None),
        ("1-000000006", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, None),
        ("1-000000005", date(2024, 1, 9), PrimaryServiceType.non_residential, None),
        ("1-000000006", date(2024, 1, 9), None, None),
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
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0, 5.0, 5.0, 5.0),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0, 5.0, 5.0, 5.0),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0, 5.0, 5.0, 5.0),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 123456789, 5.0, "source", 5.0, 5.0, 5.0, 5.0, 5.0),
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
class ValidateASCWDSWorkplaceRawData:
    raw_ascwds_workplace_rows = [
        ("estab_1", "20240101"),
        ("estab_2", "20240101"),
        ("estab_1", "20240109"),
        ("estab_2", "20240109"),
    ]


@dataclass
class ValidateASCWDSWorkerRawData:
    # fmt: off
    raw_ascwds_worker_rows = [
        ("estab_1", "20240101", "worker_1", "8"),
        ("estab_2", "20240101", "worker_2", "8"),
        ("estab_1", "20240109", "worker_3", "8"),
        ("estab_2", "20240109", "worker_4", "8"),
    ]
    # fmt: on


@dataclass
class ValidateLocationsAPIRawData:
    # fmt: off
    raw_cqc_locations_rows = [
        ("1-000000001", "20240101", "Y", "prov_1", RegistrationStatus.registered, "2020-01-01", "location name", 5, "N"),
        ("1-000000002", "20240101", "Y", "prov_1", RegistrationStatus.deregistered, "2020-01-01", "location name", 5, "N"),
        ("1-000000001", "20240201", "Y", "prov_1", RegistrationStatus.registered, "2020-01-01", "location name", 5, "N"),
        ("1-000000002", "20240201", "Y", "prov_1", RegistrationStatus.deregistered, "2020-01-01", "location name", 5, "N"),
    ]
    # fmt: on


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
class ValidatePIRRawData:
    # fmt: off
    raw_cqc_pir_rows = [
        ("1-000000001", "20240101", 10),
        ("1-000000002", "20240101", 10),
        ("1-000000001", "20240109", 10),
        ("1-000000002", "20240109", 10),
    ]
    # fmt: on


@dataclass
class ValidatePostcodeDirectoryRawData:
    # fmt: off
    raw_postcode_directory_rows = [
        ("AB1 2CD", "20240101", "cssr", "region", "rui"),
        ("AB2 2CD", "20240101", "cssr", "region", "rui"),
        ("AB1 2CD", "20240201", "cssr", "region", "rui"),
        ("AB2 2CD", "20240201", "cssr", "region", "rui"),
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

    expected_pir_data = [
        ("loc_1", "20240101", "Non-residential", "24-Jan-24", "0", "other"),
        ("1-1199876096", "20240101", "Non-residential", "24-Jan-24", "0", "other"),
        ("1-1199876096", "20230601", "Non-residential", "24-Jan-24", "0", "other"),
        ("1-1199876096", "20240101", "Residential", "24-Jan-24", "0", "other"),
        ("1-1199876096", "20240101", "Non-residential", "24-May-23", "0", "other"),
        ("1-1199876096", "20230601", "Residential", "24-Jan-24", "0", "other"),
        ("1-1199876096", "20230601", "Non-residential", "24-May-23", "0", "other"),
        ("loc_1", "20230601", "Non-residential", "24-Jan-24", "0", "other"),
        ("loc_1", "20230601", "Residential", "24-Jan-24", "0", "other"),
        ("loc_1", "20230601", "Non-residential", "24-May-23", "0", "other"),
        ("loc_1", "20230601", "Residential", "24-May-23", "0", "other"),
        ("loc_1", "20240101", "Residential", "24-Jan-24", "0", "other"),
        ("loc_1", "20240101", "Residential", "24-May-23", "0", "other"),
        ("loc_1", "20240101", "Non-residential", "24-May-23", "0", "other"),
        ("loc_1", "20240101", "Non-residential", "24-Jan-24", None, "other"),
        ("1-1199876096", "20240101", "Non-residential", "24-Jan-24", None, "other"),
        ("1-1199876096", "20230601", "Non-residential", "24-Jan-24", None, "other"),
        ("1-1199876096", "20240101", "Residential", "24-Jan-24", None, "other"),
        ("1-1199876096", "20240101", "Non-residential", "24-May-23", None, "other"),
        ("1-1199876096", "20230601", "Residential", "24-Jan-24", None, "other"),
        ("1-1199876096", "20230601", "Non-residential", "24-May-23", None, "other"),
        ("loc_1", "20230601", "Non-residential", "24-Jan-24", None, "other"),
        ("loc_1", "20230601", "Residential", "24-Jan-24", None, "other"),
        ("loc_1", "20230601", "Non-residential", "24-May-23", None, "other"),
        ("loc_1", "20230601", "Residential", "24-May-23", None, "other"),
        ("loc_1", "20240101", "Residential", "24-Jan-24", None, "other"),
        ("loc_1", "20240101", "Residential", "24-May-23", None, "other"),
        ("loc_1", "20240101", "Non-residential", "24-May-23", None, "other"),
    ]
    pir_data_with_single_row_to_remove = [
        *expected_pir_data,
        ("1-1199876096", "20230601", "Residential", "24-May-23", None, "other"),
    ]
    pir_data_with_multiple_rows_to_remove = [
        *expected_pir_data,
        ("1-1199876096", "20230601", "Residential", "24-May-23", None, "other"),
        (
            "1-1199876096",
            "20230601",
            "Residential",
            "24-May-23",
            None,
            "something else",
        ),
    ]

    pir_data_without_rows_to_remove = expected_pir_data

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
        ),
    ]
    filter_to_known_values_rows = []
    expected_filter_to_known_values_rows = []
    restructure_dataframe_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            10.0,
            10.0,
            PrimaryServiceType.care_home_only,
            11.0,
            12.0,
            9.0,
            8.0,
            10.0,
        ),
    ]
    expected_restructure_dataframe_rows = [
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            IndCQC.ascwds_filled_posts_dedup_clean,
            10.0,
        ),
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            IndCQC.rolling_average_model,
            11.0,
        ),
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            IndCQC.care_home_model,
            12.0,
        ),
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            IndCQC.extrapolation_care_home_model,
            9.0,
        ),
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            IndCQC.interpolation_model,
            8.0,
        ),
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            IndCQC.estimate_filled_posts,
            10.0,
        ),
    ]
