import math
from dataclasses import dataclass
from datetime import date

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    CareHome,
    EstimateFilledPostsSource,
    MainJobRoleLabels,
    PrimaryServiceType,
    Region,
    RelatedLocation,
    Sector,
)
from utils.column_values.categorical_columns_by_dataset import (
    DiagnosticOnKnownFilledPostsCategoricalValues as CatValues,
)


@dataclass
class ImputeIndCqcAscwdsAndPirData:
    cleaned_ind_cqc_rows = [
        (
            "1-001",
            date(2025, 1, 1),
            9,
            CareHome.care_home,
            PrimaryServiceType.care_home_with_nursing,
            25.0,
            2.5,
            10.0,
        ),
    ]


@dataclass
class ModelAndMergePirData:
    expected_convert_pir_to_filled_posts_rows = [
        ("loc 1", date(2024, 1, 1), CareHome.not_care_home,   10, 14.0,  15.0),
        ("loc 2", date(2024, 1, 1), CareHome.not_care_home,   10, 16.0,  15.0),
        ("loc 3", date(2024, 1, 1), CareHome.not_care_home, None, 10.0,  None),
        ("loc 4", date(2024, 1, 1), CareHome.not_care_home,    1,  0.0,  1.5),
        ("loc 5", date(2024, 1, 1), CareHome.not_care_home,    0,  1.0,  None),
        ("loc 5", date(2024, 1, 1), CareHome.not_care_home,   10,  7.0,  15.0),
        ("loc 6", date(2024, 1, 1), CareHome.care_home,       10, None,  None),
    ]  # fmt: skip

    blend_pir_and_ascwds_rows = [
        ("loc 1", date(2024, 1, 1), CareHome.not_care_home, 10.0, 20.0),
    ]
    create_repeated_ascwds_clean_column_when_missing_earlier_and_later_data_rows = [
        ("loc 1", date(2024, 1, 1), None),
        ("loc 1", date(2024, 2, 1), 100.0),
        ("loc 1", date(2024, 3, 1), None),
    ]
    expected_create_repeated_ascwds_clean_column_when_missing_earlier_and_later_data_rows = [
        ("loc 1", date(2024, 1, 1), None, None),
        ("loc 1", date(2024, 2, 1), 100.0, 100.0),
        ("loc 1", date(2024, 3, 1), None, 100.0),
    ]

    create_repeated_ascwds_clean_column_when_missing_middle_data_rows = [
        ("loc 3", date(2024, 1, 1), 40.0),
        ("loc 3", date(2024, 2, 1), None),
        ("loc 3", date(2024, 3, 1), 60.0),
    ]
    expected_create_repeated_ascwds_clean_column_when_missing_middle_data_rows = [
        ("loc 3", date(2024, 1, 1), 40.0, 40.0),
        ("loc 3", date(2024, 2, 1), None, 40.0),
        ("loc 3", date(2024, 3, 1), 60.0, 60.0),
    ]

    create_repeated_ascwds_clean_column_separates_repetition_by_location_id_rows = [
        ("loc 1", date(2024, 1, 1), 100.0),
        ("loc 1", date(2024, 2, 1), None),
        ("loc 2", date(2024, 1, 1), 50.0),
        ("loc 2", date(2024, 2, 1), None),
    ]
    expected_create_repeated_ascwds_clean_column_separates_repetition_by_location_id_rows = [
        ("loc 1", date(2024, 1, 1), 100.0, 100.0),
        ("loc 1", date(2024, 2, 1), None, 100.0),
        ("loc 2", date(2024, 1, 1), 50.0, 50.0),
        ("loc 2", date(2024, 2, 1), None, 50.0),
    ]

    create_last_submission_columns_rows = [
        ("loc 1", date(2024, 1, 1), 10.0, None),
        ("loc 1", date(2024, 2, 1), None, 20.0),
        ("loc 2", date(2024, 1, 1), None, 30.0),
        ("loc 2", date(2024, 2, 1), 40.0, None),
        ("loc 3", date(2024, 1, 1), None, None),
        ("loc 3", date(2024, 2, 1), None, None),
        ("loc 4", date(2024, 1, 1), 50.0, None),
        ("loc 4", date(2024, 2, 1), None, None),
        ("loc 4", date(2024, 3, 1), 60.0, None),
        ("loc 4", date(2024, 4, 1), None, 70.0),
    ]
    expected_create_last_submission_columns_rows = [
        ("loc 1", date(2024, 1, 1), 10.0, None, date(2024, 1, 1), date(2024, 2, 1)),
        ("loc 1", date(2024, 2, 1), None, 20.0, date(2024, 1, 1), date(2024, 2, 1)),
        ("loc 2", date(2024, 1, 1), None, 30.0, date(2024, 2, 1), date(2024, 1, 1)),
        ("loc 2", date(2024, 2, 1), 40.0, None, date(2024, 2, 1), date(2024, 1, 1)),
        ("loc 3", date(2024, 1, 1), None, None, None, None),
        ("loc 3", date(2024, 2, 1), None, None, None, None),
        ("loc 4", date(2024, 1, 1), 50.0, None, date(2024, 3, 1), date(2024, 4, 1)),
        ("loc 4", date(2024, 2, 1), None, None, date(2024, 3, 1), date(2024, 4, 1)),
        ("loc 4", date(2024, 3, 1), 60.0, None, date(2024, 3, 1), date(2024, 4, 1)),
        ("loc 4", date(2024, 4, 1), None, 70.0, date(2024, 3, 1), date(2024, 4, 1)),
    ]

    # fmt: off
    create_ascwds_pir_merged_column_when_pir_more_than_two_years_after_asc_and_difference_greater_than_thresholds_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 200.0, None),
    ]
    expected_create_ascwds_pir_merged_column_when_pir_more_than_two_years_after_asc_and_difference_greater_than_thresholds_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 200.0, None, 200.0),
    ]
    create_ascwds_pir_merged_column_when_pir_less_than_two_years_after_asc_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, None, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, 200.0, None),
    ]
    expected_create_ascwds_pir_merged_column_when_pir_less_than_two_years_after_asc_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, None, 10.0, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2022, 1, 1), 10.0, 200.0, None, None),
    ]
    create_ascwds_pir_merged_column_when_asc_after_pir_rows = [
        ("loc 1", date(2020, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, 200.0, None),
        ("loc 1", date(2021, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, None, None),
        ("loc 1", date(2022, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, None, None),
        ("loc 1", date(2023, 1, 1), date(2023, 1, 1), date(2020, 1, 1), 10.0, None, 10.0),
    ]
    expected_create_ascwds_pir_merged_column_when_asc_after_pir_rows = [
        ("loc 1", date(2020, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, 200.0, None, None),
        ("loc 1", date(2021, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, None, None, None),
        ("loc 1", date(2022, 1, 1), date(2023, 1, 1), date(2020, 1, 1), None, None, None, None),
        ("loc 1", date(2023, 1, 1), date(2023, 1, 1), date(2020, 1, 1), 10.0, None, 10.0, 10.0),
    ]
    create_ascwds_pir_merged_column_when_difference_less_than_absolute_threshold_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 110.0, None),
        ("loc 2", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, 110.0),
        ("loc 2", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, None),
        ("loc 2", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, None),
        ("loc 2", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, 10.0, None),
    ]
    expected_create_ascwds_pir_merged_column_when_difference_less_than_absolute_threshold_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 110.0, None, None),
        ("loc 2", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, 110.0, 110.0),
        ("loc 2", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, None, None),
        ("loc 2", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, None, None, None),
        ("loc 2", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 110.0, 10.0, None, None),
    ]
    create_ascwds_pir_merged_column_when_difference_less_than_percentage_threshold_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 5.0, None),
        ("loc 2", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, 5.0),
        ("loc 2", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, None),
        ("loc 2", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, None),
        ("loc 2", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, 10.0, None),
    ]
    expected_create_ascwds_pir_merged_column_when_difference_less_than_percentage_threshold_rows = [
        ("loc 1", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, 10.0, 10.0),
        ("loc 1", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, None, None, None),
        ("loc 1", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 10.0, 5.0, None, None),
        ("loc 2", date(2020, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, 5.0, 5.0),
        ("loc 2", date(2021, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, None, None),
        ("loc 2", date(2022, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, None, None, None),
        ("loc 2", date(2023, 1, 1), date(2020, 1, 1), date(2023, 1, 1), 5.0, 10.0, None, None),
    ]
    # fmt: on

    include_pir_if_never_submitted_ascwds_rows = [
        ("1-001", date(2024, 1, 1), 10.0, None),
        ("1-001", date(2024, 2, 1), 20.0, 50.0),
        ("1-002", date(2024, 1, 1), 30.0, None),
        ("1-002", date(2024, 2, 1), None, 60.0),
        ("1-003", date(2024, 1, 1), None, 70.0),
        ("1-003", date(2024, 2, 1), 40.0, None),
        ("1-004", date(2024, 1, 1), None, None),
        ("1-004", date(2024, 2, 1), None, 80.0),
    ]
    expected_include_pir_if_never_submitted_ascwds_rows = [
        ("1-001", date(2024, 1, 1), 10.0, None),
        ("1-001", date(2024, 2, 1), 20.0, 50.0),
        ("1-002", date(2024, 1, 1), 30.0, None),
        ("1-002", date(2024, 2, 1), None, 60.0),
        ("1-003", date(2024, 1, 1), None, 70.0),
        ("1-003", date(2024, 2, 1), 40.0, None),
        ("1-004", date(2024, 1, 1), None, None),
        ("1-004", date(2024, 2, 1), 80.0, 80.0),
    ]


@dataclass
class ImputeUtilsData:
    convert_care_home_ratios_to_posts_rows = [
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
    expected_convert_care_home_ratios_to_posts_rows = [
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

    combine_care_home_and_non_res_values_into_single_column_rows = [
        ("1-001", CareHome.care_home, 20.0, 1.6),
        ("1-002", CareHome.care_home, 10.0, None),
        ("1-003", CareHome.care_home, None, 1.8),
        ("1-004", CareHome.care_home, None, None),
        ("1-005", CareHome.not_care_home, 20.0, 1.6),
        ("1-006", CareHome.not_care_home, 10.0, None),
        ("1-007", CareHome.not_care_home, None, 1.6),
        ("1-008", CareHome.not_care_home, None, None),
    ]
    expected_combine_care_home_and_non_res_values_into_single_column_rows = [
        ("1-001", CareHome.care_home, 20.0, 1.6, 1.6),
        ("1-002", CareHome.care_home, 10.0, None, None),
        ("1-003", CareHome.care_home, None, 1.8, 1.8),
        ("1-004", CareHome.care_home, None, None, None),
        ("1-005", CareHome.not_care_home, 20.0, 1.6, 20.0),
        ("1-006", CareHome.not_care_home, 10.0, None, 10.0),
        ("1-007", CareHome.not_care_home, None, 1.8, None),
        ("1-008", CareHome.not_care_home, None, None, None),
    ]


@dataclass
class ValidateImputedIndCqcAscwdsAndPir:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1),),
        ("1-000000002", date(2024, 1, 1),),
        ("1-000000001", date(2024, 2, 1),),
        ("1-000000002", date(2024, 2, 1),),
    ]

    imputed_ind_cqc_ascwds_and_pir_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, 5, "source", 5.0, 5),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, 5, "source", 5.0, 5),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, 5, "source", 5.0, 5),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, 5, "source", 5.0, 5),
    ]
    # fmt: on

    calculate_expected_size_rows = [
        (
            "1-000000001",
            date(2024, 1, 1),
        ),
    ]


@dataclass
class ModelPrimaryServiceRateOfChange:
    # fmt: off
    primary_service_rate_of_change_rows = [
        ("1-001", CareHome.care_home, 100000, PrimaryServiceType.care_home_only, 10, 1.0, 3.0, 1),
        ("1-001", CareHome.care_home, 200000, PrimaryServiceType.care_home_only, 10, 1.0, 2.8, 1),
        ("1-001", CareHome.care_home, 300000, PrimaryServiceType.care_home_only, 10, 1.0, 3.4, 1),
        ("1-001", CareHome.care_home, 400000, PrimaryServiceType.care_home_only, 10, 1.0, 3.2, 1),
        ("1-002", CareHome.care_home, 100000, PrimaryServiceType.care_home_only, 10, 1.0, 2.0, 1),
        ("1-002", CareHome.care_home, 200000, PrimaryServiceType.care_home_only, 10, 1.0, None, 1),
        ("1-002", CareHome.care_home, 300000, PrimaryServiceType.care_home_only, 10, 1.0, None, 1),
        ("1-002", CareHome.care_home, 400000, PrimaryServiceType.care_home_only, 10, 1.0, 3.2, 1),
        ("1-003", CareHome.not_care_home, 100000, PrimaryServiceType.non_residential, None, 0.0, 40.0, 1),
        ("1-003", CareHome.not_care_home, 200000, PrimaryServiceType.non_residential, None, 0.0, 50.0, 1),
        ("1-004", CareHome.not_care_home, 200000, PrimaryServiceType.non_residential, None, 0.0, 60.0, 1),
        ("1-005", CareHome.care_home, 100000, PrimaryServiceType.care_home_only, 10, 1.0, 4.0, 2),
        ("1-005", CareHome.not_care_home, 200000, PrimaryServiceType.non_residential, None, 0.0, 50.0, 2),
    ]
    # fmt: on
    expected_primary_service_rate_of_change_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 200000, 1.03999),
        (PrimaryServiceType.care_home_only, 1.0, 300000, 1.1176),
        (PrimaryServiceType.care_home_only, 1.0, 400000, 1.0854),
        (PrimaryServiceType.non_residential, None, 200000, 1.25),
    ]

    eligible_location_rows = [
        ("1-001", 100000, CareHome.care_home, 1, 10.0, 2),
        ("1-001", 200000, CareHome.care_home, 1, None, 2),
        ("1-001", 300000, CareHome.care_home, 1, 10.0, 2),
        ("1-001", 400000, CareHome.care_home, 1, None, 2),
    ]
    remove_ineligible_locations_with_one_submission_rows = [
        ("1-001", 100000, CareHome.care_home, 1, 10.0, 1),
        ("1-001", 200000, CareHome.care_home, 1, None, 1),
    ]
    remove_ineligible_locations_with_multiple_statuses_rows = [
        ("1-001", 100000, CareHome.care_home, 2, 10.0, 2),
        ("1-001", 200000, CareHome.care_home, 2, 10.0, 2),
        ("1-001", 300000, CareHome.not_care_home, 2, 10.0, 1),
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

    interpolate_current_values_rows = [
        ("1-001", 100000, 30.0),
        ("1-001", 200000, None),
        ("1-001", 300000, 34.0),
        ("1-001", 400000, None),
    ]
    expected_interpolate_current_values_rows = [
        ("1-001", 100000, 30.0, 30.0),
        ("1-001", 200000, None, 32.0),
        ("1-001", 300000, 34.0, 34.0),
        ("1-001", 400000, None, None),
    ]

    add_previous_value_column_rows = [
        ("1-001", 100000, 1.1),
        ("1-001", 200000, 1.2),
        ("1-001", 300000, None),
        ("1-001", 400000, 1.4),
        ("1-001", 500000, 1.5),
        ("1-002", 200000, 10.2),
        ("1-002", 300000, 10.3),
    ]
    expected_add_previous_value_column_rows = [
        ("1-001", 100000, 1.1, None),
        ("1-001", 200000, 1.2, 1.1),
        ("1-001", 300000, None, 1.2),
        ("1-001", 400000, 1.4, None),
        ("1-001", 500000, 1.5, 1.4),
        ("1-002", 200000, 10.2, None),
        ("1-002", 300000, 10.3, 10.2),
    ]

    calculate_primary_service_rolling_sums_rows = [
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 100000, 1.1, None),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 200000, 1.2, 1.1),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 300000, 1.3, 1.2),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 400000, None, 1.3),
        ("1-002", PrimaryServiceType.care_home_only, 1.0, 100000, 1.4, None),
        ("1-002", PrimaryServiceType.care_home_only, 1.0, 200000, 1.3, 1.4),
        ("1-003", PrimaryServiceType.care_home_only, 2.0, 100000, 1.5, None),
        ("1-003", PrimaryServiceType.care_home_only, 2.0, 200000, 1.6, 1.5),
        ("1-004", PrimaryServiceType.non_residential, 0.0, 100000, 10.0, None),
        ("1-004", PrimaryServiceType.non_residential, 0.0, 200000, 20.0, 10.0),
        ("1-004", PrimaryServiceType.non_residential, 0.0, 300000, 30.0, 20.0),
    ]
    expected_calculate_primary_service_rolling_sums_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 100000, 2.5, 2.5),
        (PrimaryServiceType.care_home_only, 1.0, 200000, 3.8, 3.7),
        (PrimaryServiceType.care_home_only, 2.0, 100000, 1.6, 1.5),
        (PrimaryServiceType.non_residential, 0.0, 100000, 20.0, 10.0),
        (PrimaryServiceType.non_residential, 0.0, 200000, 50.0, 30.0),
    ]

    rolling_sums_filtering_rows = [
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 100000, None, 1.3),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 200000, 1.2, 1.1),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 300000, 1.1, None),
    ]
    expected_rolling_sums_filtering_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 200000, 1.2, 1.1),
    ]

    rolling_sums_simple_window_rows = [
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 100000, 1.0, 2.0),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 100001, 3.0, 4.0),
    ]
    expected_rolling_sums_simple_window_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 100000, 1.0, 2.0),
        (PrimaryServiceType.care_home_only, 1.0, 100001, 4.0, 6.0),
    ]

    rolling_sums_window_includes_values_within_range_rows = [
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 100000, 1.0, 2.0),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 300000, 3.0, 4.0),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 500000, 5.0, 6.0),
    ]
    expected_rolling_sums_window_includes_values_within_range_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 100000, 1.0, 2.0),
        (PrimaryServiceType.care_home_only, 1.0, 300000, 4.0, 6.0),
        (PrimaryServiceType.care_home_only, 1.0, 500000, 8.0, 10.0),
    ]

    rolling_sums_window_partitions_correctly_rows = [
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 100000, 1.0, 2.0),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 200000, 10.0, 20.0),
        ("1-002", PrimaryServiceType.care_home_only, 2.0, 100000, 3.0, 4.0),
        ("1-002", PrimaryServiceType.care_home_only, 2.0, 200000, 30.0, 40.0),
        ("1-003", PrimaryServiceType.non_residential, 1.0, 100000, 5.0, 6.0),
        ("1-003", PrimaryServiceType.non_residential, 1.0, 200000, 50.0, 60.0),
    ]
    expected_rolling_sums_window_partitions_correctly_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 100000, 1.0, 2.0),
        (PrimaryServiceType.care_home_only, 1.0, 200000, 11.0, 22.0),
        (PrimaryServiceType.care_home_only, 2.0, 100000, 3.0, 4.0),
        (PrimaryServiceType.care_home_only, 2.0, 200000, 33.0, 44.0),
        (PrimaryServiceType.non_residential, 1.0, 100000, 5.0, 6.0),
        (PrimaryServiceType.non_residential, 1.0, 200000, 55.0, 66.0),
    ]

    rolling_sums_deduplication_rows = [
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 100000, 1.0, 2.0),
        ("1-002", PrimaryServiceType.care_home_only, 1.0, 100000, 1.0, 2.0),
        ("1-003", PrimaryServiceType.care_home_only, 2.0, 100000, 1.0, 2.0),
        ("1-004", PrimaryServiceType.care_home_only, 2.0, 100000, 1.0, 2.0),
        ("1-005", PrimaryServiceType.non_residential, 1.0, 100000, 1.0, 2.0),
        ("1-006", PrimaryServiceType.non_residential, 1.0, 100000, 1.0, 2.0),
        ("1-007", PrimaryServiceType.non_residential, 1.0, 500000, 1.0, 2.0),
        ("1-008", PrimaryServiceType.non_residential, 1.0, 500000, 1.0, 2.0),
    ]
    expected_rolling_sums_deduplication_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 100000, 2.0, 4.0),
        (PrimaryServiceType.care_home_only, 2.0, 100000, 2.0, 4.0),
        (PrimaryServiceType.non_residential, 1.0, 100000, 2.0, 4.0),
        (PrimaryServiceType.non_residential, 1.0, 500000, 2.0, 4.0),
    ]


@dataclass
class ModelPrimaryServiceRateOfChangeCleaningData:
    calculate_absolute_and_percentage_change_rows = [
        ("1-001", None, 40.0, None, None),
        ("1-002", 0.0, 40.0, 40.0, None),
        ("1-003", 40.0, None, None, None),
        ("1-004", 40.0, 0.0, 40.0, 0.0),
        ("1-005", 40.0, 20.0, 20.0, 0.5),
        ("1-006", 40.0, 40.0, 0.0, 1.0),
        ("1-007", 40.0, 80.0, 40.0, 2.0),
    ]

    compute_non_res_threshold_valid_rows = [
        ("1-001", CareHome.not_care_home, 20.0, 25.0, 5.0, 1.25),
        ("1-002", CareHome.not_care_home, 30.0, 45.0, 15.0, 1.5),
        ("1-003", CareHome.not_care_home, 15.0, 18.0, 3.0, 1.2),
    ]
    # fmt: off
    compute_non_res_threshold_invalid_rows = [
        ("1-004", CareHome.care_home, 2.0, 200.0, 198.0, 100.0), # exclude care home
        ("1-005", CareHome.not_care_home, None, 100.0, 100.0, 100.0), # exclude null prev value
        ("1-006", CareHome.not_care_home, 100.0, None, 100.0, 100.0), # exclude null curr value
        ("1-007", CareHome.not_care_home, 5.0, 6.0, 1.0, 1.2), # exclude when curr and prev below 10
        ("1-008", CareHome.not_care_home, 50.0, 50.0, 0.0, 1.0), # exclude when curr = prev
    ]
    # fmt: on
    compute_non_res_threshold_with_invalid_rows = (
        compute_non_res_threshold_valid_rows + compute_non_res_threshold_invalid_rows
    )

    # fmt: off
    build_keep_condition_rows = [
        ("1-001", CareHome.care_home, 100.0, 200.0, 100.0, 2.0, True), # care home
        ("1-002", CareHome.not_care_home, 5.0, 8.0, 3.0, 1.6, True), # both values below 10
        ("1-003", CareHome.not_care_home, 100.0, 120.0, 20.0, 1.2, True), # changes within thresholds
        ("1-004", CareHome.not_care_home, 100.0, 200.0, 100.0, 2.0, False), # abs change too large
        ("1-005", CareHome.not_care_home, 100.0, 260.0, 160.0, 2.6, False), # perc change too large
        ("1-006", CareHome.not_care_home, 100.0, 40.0, 60.0, 0.4, False), # perc change too small
    ]
    # fmt: on

    apply_rate_of_change_cleaning_rows = [
        ("1-001", 1.0, 10.0, True, 1.0, 10.0),
        ("1-002", 1.0, 10.0, False, None, None),
    ]


@dataclass
class ModelPrimaryServiceRateOfChangeTrendlineData:
    # fmt: off
    primary_service_rate_of_change_trendline_rows = [
        ("1-001", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.0, 1),
        ("1-001", 1704153600, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 2.8, 1),
        ("1-001", 1704240000, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.4, 1),
        ("1-001", 1704326400, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.2, 1),
        ("1-002", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 2.0, 1),
        ("1-002", 1704153600, CareHome.care_home, 10, PrimaryServiceType.care_home_only, None, 1),
        ("1-002", 1704240000, CareHome.care_home, 10, PrimaryServiceType.care_home_only, None, 1),
        ("1-002", 1704326400, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.2, 1),
        ("1-003", 1704067200, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 40.0, 1),
        ("1-003", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 50.0, 1),
        ("1-004", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 60.0, 1),
        ("1-005", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 4.0, 1),
        ("1-005", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 50.0, 2),
    ]
    expected_primary_service_rate_of_change_trendline_rows = [
        ("1-001", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.0, 1, 1.0),
        ("1-001", 1704153600, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 2.8, 1, 1.03999),
        ("1-001", 1704240000, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.4, 1, 1.16235),
        ("1-001", 1704326400, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.2, 1, 1.26158),
        ("1-002", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 2.0, 1, 1.0),
        ("1-002", 1704153600, CareHome.care_home, 10, PrimaryServiceType.care_home_only, None, 1, 1.03999),
        ("1-002", 1704240000, CareHome.care_home, 10, PrimaryServiceType.care_home_only, None, 1, 1.16235),
        ("1-002", 1704326400, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.2, 1, 1.26158),
        ("1-003", 1704067200, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 40.0, 1, 1.0),
        ("1-003", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 50.0, 1, 1.25),
        ("1-004", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 60.0, 1, 1.25),
        ("1-005", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 4.0, 1, 1.0),
        ("1-005", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 50.0, 2, 1.25),
    ]
    # fmt: on

    calculate_rate_of_change_trendline_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 1672531200, 1.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672617600, 1.5),
        (PrimaryServiceType.care_home_only, 1.0, 1672704000, 2.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672790400, 1.5),
        (PrimaryServiceType.care_home_only, 2.0, 1672531200, 1.0),
        (PrimaryServiceType.care_home_only, 2.0, 1672617600, 1.2),
        (PrimaryServiceType.non_residential, 0.0, 1672531200, 1.0),
        (PrimaryServiceType.non_residential, 0.0, 1672617600, 1.2),
        (PrimaryServiceType.non_residential, 0.0, 1672704000, 1.0),
        (PrimaryServiceType.non_residential, 0.0, 1672790400, 1.5),
    ]
    expected_calculate_rate_of_change_trendline_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 1672531200, 1.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672617600, 1.5),
        (PrimaryServiceType.care_home_only, 1.0, 1672704000, 3.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672790400, 4.5),
        (PrimaryServiceType.care_home_only, 2.0, 1672531200, 1.0),
        (PrimaryServiceType.care_home_only, 2.0, 1672617600, 1.2),
        (PrimaryServiceType.non_residential, 0.0, 1672531200, 1.0),
        (PrimaryServiceType.non_residential, 0.0, 1672617600, 1.2),
        (PrimaryServiceType.non_residential, 0.0, 1672704000, 1.2),
        (PrimaryServiceType.non_residential, 0.0, 1672790400, 1.8),
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
        ("1-003", None, 30.0, None),
        ("1-004", -2.0, None, None),
        ("1-005", None, -2.0, 30.0),
        ("1-006", 10.0, -2.0, None),
        ("1-007", 10.0, None, 30.0),
        ("1-008", 10.0, -2.0, 30.0),
    ]
    expected_imputation_model_rows = [
        ("1-001", None, None, None, None),
        ("1-002", None, None, 30.0, 30.0),
        ("1-003", None, 30.0, None, 30.0),
        ("1-004", -2.0, None, None, -2.0),
        ("1-005", None, -2.0, 30.0, -2.0),
        ("1-006", 10.0, -2.0, None, 10.0),
        ("1-007", 10.0, None, 30.0, 10.0),
        ("1-008", 10.0, -2.0, 30.0, 10.0),
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
    expected_extrapolation_forwards_when_nominal_rows = [
        ("1-001", 1672531200, 15.0, 10.0, None),
        ("1-001", 1675209600, None, 20.0, 25.0),
        ("1-001", 1677628800, 30.0, 30.0, 35.0),
        ("1-002", 1672531200, None, 10.0, None),
        ("1-002", 1675209600, 10.0, 20.0, None),
        ("1-002", 1677628800, None, 30.0, 20.0),
        ("1-002", 1677629000, None, 100.0, 90.0),
        ("1-003", 1672531200, 20.0, 100.0, None),
        ("1-003", 1675209600, None, 20.0, -60.0),
        ("1-004", 1677628800, None, 20.0, None),
    ]
    expected_extrapolation_forwards_when_ratio_rows = [
        ("1-001", 1672531200, 15.0, 10.0, None),
        ("1-001", 1675209600, None, 20.0, 30.0),
        ("1-001", 1677628800, 30.0, 30.0, 45.0),
        ("1-002", 1672531200, None, 10.0, None),
        ("1-002", 1675209600, 10.0, 20.0, None),
        ("1-002", 1677628800, None, 30.0, 15.0),
        ("1-002", 1677629000, None, 100.0, 50.0),
        ("1-003", 1672531200, 20.0, 100.0, None),
        ("1-003", 1675209600, None, 20.0, 4.0),
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
        ("1-004", 1672531200, None, 1675209600, 1675209600, 50.0),
        ("1-004", 1675209600, 20.0, 1675209600, 1675209600, 10.0),
        ("1-005", 1677628800, None, None, None, 20.0),
    ]
    # fmt: off
    expected_extrapolation_backwards_when_nominal_rows = [
        ("1-001", 1672531200, 15.0, 1672531200, 1677628800, 10.0, None),
        ("1-001", 1675209600, None, 1672531200, 1677628800, 20.0, None),
        ("1-001", 1677628800, 30.0, 1672531200, 1677628800, 30.0, None),
        ("1-002", 1672531200, None, 1675209600, 1675209600, 10.0, 0.0),
        ("1-002", 1675209600, 10.0, 1675209600, 1675209600, 20.0, None),
        ("1-002", 1677628800, None, 1675209600, 1675209600, 30.0, None),
        ("1-003", 1672531200, None, 1675209600, 1675209600, 1.0, 1.0),
        ("1-003", 1675209600, 20.0, 1675209600, 1675209600, 20.0, None),
        ("1-004", 1672531200, None, 1675209600, 1675209600, 50.0, 60.0),
        ("1-004", 1675209600, 20.0, 1675209600, 1675209600, 10.0, None),
        ("1-005", 1677628800, None, None, None, 20.0, None),
    ]
    expected_extrapolation_backwards_when_ratio_rows = [
        ("1-001", 1672531200, 15.0, 1672531200, 1677628800, 10.0, None),
        ("1-001", 1675209600, None, 1672531200, 1677628800, 20.0, None),
        ("1-001", 1677628800, 30.0, 1672531200, 1677628800, 30.0, None),
        ("1-002", 1672531200, None, 1675209600, 1675209600, 10.0, 5.0),
        ("1-002", 1675209600, 10.0, 1675209600, 1675209600, 20.0, None),
        ("1-002", 1677628800, None, 1675209600, 1675209600, 30.0, None),
        ("1-003", 1672531200, None, 1675209600, 1675209600, 1.0, 1.0),
        ("1-003", 1675209600, 20.0, 1675209600, 1675209600, 20.0, None),
        ("1-004", 1672531200, None, 1675209600, 1675209600, 50.0, 100.0),
        ("1-004", 1675209600, 20.0, 1675209600, 1675209600, 10.0, None),
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
class ModelNonResWithAndWithoutDormancyCombinedRows:
    estimated_posts_rows = [
        ("1-001", date(2021, 1, 1), CareHome.not_care_home, "Y", 1, 1.0, None),
        ("1-001", date(2022, 2, 1), CareHome.not_care_home, "Y", 2, 3.0, None),
        ("1-001", date(2023, 3, 1), CareHome.not_care_home, "Y", 3, 4.0, 5.0),
        ("1-001", date(2024, 4, 1), CareHome.not_care_home, "Y", 4, 5.0, 5.5),
        ("1-001", date(2025, 5, 1), CareHome.not_care_home, "Y", 5, 6.0, 6.0),
        ("1-001", date(2025, 6, 1), CareHome.not_care_home, "Y", 6, 7.0, 6.5),
        ("1-002", date(2021, 1, 1), CareHome.not_care_home, "Y", 3, 8.0, None),
        ("1-002", date(2022, 2, 1), CareHome.not_care_home, "Y", 4, 8.0, None),
        ("1-002", date(2023, 3, 1), CareHome.not_care_home, "Y", 5, 8.0, 4.0),
        ("1-002", date(2024, 4, 1), CareHome.not_care_home, "Y", 6, 8.0, 4.5),
        ("1-002", date(2025, 5, 1), CareHome.not_care_home, "Y", 7, 8.0, 5.0),
        ("1-002", date(2025, 6, 1), CareHome.not_care_home, "Y", 8, 8.0, 5.5),
        ("1-003", date(2021, 1, 1), CareHome.not_care_home, "N", 1, 2.0, None),
        ("1-003", date(2022, 2, 1), CareHome.not_care_home, "N", 2, 2.0, None),
        ("1-003", date(2021, 3, 1), CareHome.not_care_home, "N", 3, 4.0, None),
        ("1-003", date(2022, 4, 1), CareHome.not_care_home, "N", 4, 4.0, None),
        ("1-003", date(2023, 5, 1), CareHome.not_care_home, "N", 5, 6.0, 8.0),
        ("1-003", date(2024, 6, 1), CareHome.not_care_home, "N", 6, 6.0, 9.0),
        ("1-004", date(2024, 4, 1), CareHome.care_home, "Y", 1, None, None),
        ("1-005", date(2024, 5, 1), CareHome.not_care_home, "Y", 1, 4.0, 2.0),
        ("1-005", date(2024, 6, 1), CareHome.not_care_home, "Y", 2, 5.0, 2.5),
        ("1-006", date(2024, 5, 1), CareHome.not_care_home, "N", 1, 3.0, 2.5),
        ("1-006", date(2024, 6, 1), CareHome.not_care_home, "N", 2, 3.0, 3.0),
        ("1-006", date(2024, 7, 1), CareHome.not_care_home, "N", 3, 3.0, 3.0),
        ("1-006", date(2024, 8, 1), CareHome.not_care_home, "N", 4, 3.0, 3.0),
    ]

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
    enrich_model_ind_cqc_rows = [
        ("1-001", date(2025, 1, 1), CareHome.not_care_home, None),
        ("1-002", date(2025, 1, 1), CareHome.not_care_home, None),
        ("1-003", date(2025, 1, 1), CareHome.care_home, 2),
        ("1-004", date(2025, 1, 1), CareHome.care_home, 2),
    ]

    enrich_model_predictions_care_home_rows = [
        ("1-003", date(2025, 1, 1), 2, -0.5, "v1_r1"),
        ("1-004", date(2025, 1, 1), 2, 2.5, "v1_r1"),
    ]
    # fmt: off
    expected_enrich_model_ind_cqc_care_home_rows = [
        ("1-001", date(2025, 1, 1), CareHome.not_care_home, None, None, None), # no prediction expected
        ("1-002", date(2025, 1, 1), CareHome.not_care_home, None, None, None), # no prediction expected
        ("1-003", date(2025, 1, 1), CareHome.care_home, 2, -1.0, "v1_r1"), # prediction (converted to posts) joined in (maintains negative)
        ("1-004", date(2025, 1, 1), CareHome.care_home, 2, 5.0, "v1_r1"), # prediction (converted to posts) joined in
    ]
    # fmt: on

    enrich_model_predictions_non_res_rows = [
        ("1-001", date(2025, 1, 1), 2, -5.0, "v1_r1"),
        ("1-002", date(2025, 1, 1), 2, 2.5, "v1_r1"),
    ]
    # fmt: off
    expected_enrich_model_ind_cqc_non_res_rows = [
        ("1-001", date(2025, 1, 1), CareHome.not_care_home, None, -5.0, "v1_r1"), # prediction joined in (maintains negative)
        ("1-002", date(2025, 1, 1), CareHome.not_care_home, None, 2.5, "v1_r1"), # prediction joined in
        ("1-003", date(2025, 1, 1), CareHome.care_home, 2, None, None), # no prediction expected
        ("1-004", date(2025, 1, 1), CareHome.care_home, 2, None, None), # no prediction expected
    ]
    # fmt: on

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

    join_ind_cqc_rows = [
        ("1-001", Region.london, 67, date(2022, 2, 20)),
        ("1-001", Region.london, 67, date(2022, 3, 29)),
        ("1-002", Region.north_east, 12, date(2022, 3, 29)),
    ]
    join_prediction_rows = [
        ("1-001", 67, date(2022, 3, 29), 10.0, "v1.0.0_r2"),
    ]
    expected_join_without_run_id_rows = [
        ("1-001", Region.london, 67, date(2022, 2, 20), None),
        ("1-001", Region.london, 67, date(2022, 3, 29), 10.0),
        ("1-002", Region.north_east, 12, date(2022, 3, 29), None),
    ]
    expected_join_with_run_id_rows = [
        ("1-001", Region.london, 67, date(2022, 2, 20), None, None),
        ("1-001", Region.london, 67, date(2022, 3, 29), 10.0, "v1.0.0_r2"),
        ("1-002", Region.north_east, 12, date(2022, 3, 29), None, None),
    ]


@dataclass
class EstimateNonResCTFilledPostsData:
    estimates_rows = [
        ("1-001", CareHome.not_care_home, 8.0, 10.0),
        ("1-002", CareHome.not_care_home, 16.0, 20.0),
        ("1-003", CareHome.not_care_home, 24.0, 30.0),
        ("1-004", CareHome.not_care_home, None, 40.0),
        ("1-005", CareHome.not_care_home, 40.0, None),
        ("1-006", CareHome.not_care_home, None, None),
        ("1-007", CareHome.care_home, 100.0, 100.0),
    ]

    expected_care_worker_ratio = 0.8

    convert_to_all_posts_using_ratio_rows = [
        ("1-001", CareHome.not_care_home, 1.0),
        ("1-002", CareHome.not_care_home, 6.0),
        ("1-003", CareHome.not_care_home, None),
        ("1-004", CareHome.care_home, 100.0),
        ("1-005", CareHome.care_home, None),
    ]
    expected_convert_to_all_posts_using_ratio_rows = [
        ("1-001", CareHome.not_care_home, 1.0, 1.25),
        ("1-002", CareHome.not_care_home, 6.0, 7.5),
        ("1-003", CareHome.not_care_home, None, None),
        ("1-004", CareHome.care_home, 100.0, None),
        ("1-005", CareHome.care_home, None, None),
    ]


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
            11.0,
            None,
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
            11.0,
            None,
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
            10.0,
        ),
    ]


@dataclass
class DiagnosticsUtilsData:
    filter_to_known_values_rows = [
        ("loc 1", 1.0, 1.0),
        ("loc 2", None, 1.0),
        ("loc 3", 2.0, None),
    ]

    expected_filter_to_known_values_rows = [
        ("loc 1", 1.0, 1.0),
        ("loc 3", 2.0, None),
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
        ),
        (
            "loc 1",
            date(2024, 1, 1),
            PrimaryServiceType.care_home_only,
            10.0,
            "model_type_two",
            12.0,
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
    # fmt: on

    # fmt: off
    create_summary_dataframe_rows = [
        ("loc 1", date(2024, 1, 1), PrimaryServiceType.care_home_only, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        ("loc 2", date(2024, 1, 1), PrimaryServiceType.care_home_only, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        ("loc 3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        ("loc 4", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.care_home_model, 100.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        ("loc 5", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.imputed_posts_care_home_model, 100.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
        ("loc 6", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, 100.0, EstimateFilledPostsSource.imputed_posts_care_home_model, 100.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
    ]
    expected_create_summary_dataframe_rows = [
        (PrimaryServiceType.care_home_only, EstimateFilledPostsSource.care_home_model, 1.0, 2.0, 3.0, 4.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0),
        (PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.care_home_model, 2.0, 3.0, 4.0, 5.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0),
        (PrimaryServiceType.care_home_with_nursing, EstimateFilledPostsSource.imputed_posts_care_home_model, 3.0, 4.0, 5.0, 6.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0),
    ]
    # fmt: on


@dataclass
class IndCQCDataUtils:
    merge_columns_in_order_when_double_type = [
        ("1-001", -1.0, 10.0, 30.0),
        ("1-002", 10.0, None, 80.0),
        ("1-003", None, 30.0, 50.0),
        ("1-004", None, 0.5, 40.0),
        ("1-005", None, None, 40.0),
        ("1-006", None, None, None),
    ]
    expected_merge_columns_in_order_when_double_type = [
        ("1-001", -1.0, 10.0, 30.0, -1.0, "model_name_1"),
        ("1-002", 10.0, None, 80.0, 10.0, "model_name_1"),
        ("1-003", None, 30.0, 50.0, 30.0, "model_name_2"),
        ("1-004", None, 0.5, 40.0, 0.5, "model_name_2"),
        ("1-005", None, None, 40.0, 40.0, "model_name_3"),
        ("1-006", None, None, None, None, None),
    ]

    list_of_map_columns_to_be_merged = [
        IndCQC.ascwds_job_role_ratios,
        IndCQC.ascwds_job_role_rolling_ratio,
    ]

    # fmt: off
    merge_columns_in_order_when_map_type = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4}),
        ("1-002",
         None,
         {MainJobRoleLabels.care_worker: 0.8, MainJobRoleLabels.registered_nurse: 0.2})
    ]
    expected_merge_columns_in_order_when_map_type = [
        ("1-001",
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         {MainJobRoleLabels.care_worker: 0.6, MainJobRoleLabels.registered_nurse: 0.4},
         {MainJobRoleLabels.care_worker: 0.5, MainJobRoleLabels.registered_nurse: 0.5},
         IndCQC.ascwds_job_role_ratios),
        ("1-002",
         None,
         {MainJobRoleLabels.care_worker: 0.8, MainJobRoleLabels.registered_nurse: 0.2},
         {MainJobRoleLabels.care_worker: 0.8, MainJobRoleLabels.registered_nurse: 0.2},
         IndCQC.ascwds_job_role_rolling_ratio)
    ]
    # fmt: on

    merge_columns_in_order_when_all_null = [("1-001", None, None)]
    expected_merge_columns_in_order_when_all_null = [("1-001", None, None, None, None)]

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

    nullify_ct_values_previous_to_first_submission_rows = [
        ("1-001", date(2021, 4, 30), 20.0, 1.6, "str_1"),
        ("1-002", date(2021, 5, 1), 10.0, 2.6, "str_2"),
    ]
    expected_nullify_ct_values_previous_to_first_submission_rows = [
        ("1-001", date(2021, 4, 30), None, 1.6, None),
        ("1-002", date(2021, 5, 1), 10.0, 2.6, "str_2"),
    ]


# converted to polars -> projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas.ForwardFillLatestKnownValue
@dataclass
class ForwardFillLatestKnownValue:
    expected_size_based_forward_fill_days_dict = {
        -math.inf: 250,
        10: 125,
        50: 65,
    }

    TEST_SIZE_BASED_FORWARD_FILL_DAYS = {
        -math.inf: 1,
        2: 2,
        4: 3,
    }
    size_based_forward_fill_days_rows = [
        ("loc-1", -1),
        ("loc-2", 1),
        ("loc-3", 2),
        ("loc-4", 3),
        ("loc-5", 4),
        ("loc-6", None),
    ]
    expected_size_based_forward_fill_days_rows = [
        ("loc-1", -1, 1),
        ("loc-2", 1, 1),
        ("loc-3", 2, 2),
        ("loc-4", 3, 2),
        ("loc-5", 4, 3),
        ("loc-6", None, None),
    ]

    last_known_latest_per_location_rows = [
        ("loc-1", date(2025, 1, 1), 10),
        ("loc-1", date(2025, 1, 2), 20),
        ("loc-1", date(2025, 1, 3), 15),
        ("loc-2", date(2025, 1, 1), 5),
        ("loc-2", date(2025, 1, 3), 15),
        ("loc-2", date(2025, 1, 4), 12),
    ]
    expected_last_known_latest_per_location_rows = [
        ("loc-1", date(2025, 1, 3), 15),
        ("loc-2", date(2025, 1, 4), 12),
    ]

    last_known_ignores_null_rows = [
        ("loc-1", date(2025, 1, 1), 10),
        ("loc-1", date(2025, 1, 2), None),
        ("loc-1", date(2025, 1, 3), None),
        ("loc-2", date(2025, 1, 1), None),
        ("loc-2", date(2025, 1, 3), 15),
    ]
    expected_last_known_ignores_null_rows = [
        ("loc-1", date(2025, 1, 1), 10),
        ("loc-2", date(2025, 1, 3), 15),
    ]

    forward_fill_within_days_rows = [
        ("loc-1", date(2025, 1, 1), 100, date(2025, 1, 1), 100, 2),
        ("loc-1", date(2025, 1, 2), None, date(2025, 1, 1), 100, 2),
        ("loc-1", date(2025, 1, 3), None, date(2025, 1, 1), 100, 2),
    ]
    expected_forward_fill_within_days_rows = [
        ("loc-1", date(2025, 1, 1), 100, date(2025, 1, 1), 100, 2),
        ("loc-1", date(2025, 1, 2), 100, date(2025, 1, 1), 100, 2),
        ("loc-1", date(2025, 1, 3), 100, date(2025, 1, 1), 100, 2),
    ]

    forward_fill_beyond_days_rows = [
        ("loc-1", date(2025, 1, 1), 100, date(2025, 1, 1), 100, 2),
        ("loc-1", date(2025, 1, 4), None, date(2025, 1, 1), 100, 2),
    ]
    expected_forward_fill_beyond_days_rows = [
        ("loc-1", date(2025, 1, 1), 100, date(2025, 1, 1), 100, 2),
        ("loc-1", date(2025, 1, 4), None, date(2025, 1, 1), 100, 2),
    ]

    forward_fill_before_last_known_rows = [
        ("loc-1", date(2025, 1, 1), None, date(2025, 1, 4), 20, 2),
        ("loc-1", date(2025, 1, 2), 20, date(2025, 1, 4), 20, 2),
        ("loc-1", date(2025, 1, 3), None, date(2025, 1, 4), 20, 2),
        ("loc-1", date(2025, 1, 4), 30, date(2025, 1, 4), 20, 2),
    ]
