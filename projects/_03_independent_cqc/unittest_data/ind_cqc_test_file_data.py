from dataclasses import dataclass
from datetime import date

from pyspark.ml.linalg import Vectors

from utils.column_values.categorical_column_values import (
    CareHome,
    PrimaryServiceType,
    RegistrationStatus,
    Sector,
)


@dataclass
class MergeIndCQCData:
    clean_cqc_location_for_merge_rows = [
        (date(2024, 1, 1), "1-001", Sector.independent, "Y", 10),
        (date(2024, 1, 1), "1-002", Sector.independent, "N", None),
        (date(2024, 1, 1), "1-003", Sector.independent, "N", None),
        (date(2024, 2, 1), "1-001", Sector.independent, "Y", 10),
        (date(2024, 2, 1), "1-002", Sector.independent, "N", None),
        (date(2024, 2, 1), "1-003", Sector.independent, "N", None),
        (date(2024, 3, 1), "1-001", Sector.independent, "Y", 10),
        (date(2024, 3, 1), "1-002", Sector.independent, "N", None),
        (date(2024, 3, 1), "1-003", Sector.independent, "N", None),
    ]

    data_to_merge_without_care_home_col_rows = [
        (date(2024, 1, 1), "1-001", "1", 1),
        (date(2024, 1, 1), "1-003", "3", 2),
        (date(2024, 1, 5), "1-001", "1", 3),
        (date(2024, 1, 9), "1-001", "1", 4),
        (date(2024, 1, 9), "1-003", "3", 5),
        (date(2024, 3, 1), "1-003", "4", 6),
    ]
    # fmt: off
    expected_merged_without_care_home_col_rows = [
        ("1-001", date(2024, 1, 1), date(2024, 1, 1), Sector.independent, "Y", 10, "1", 1),
        ("1-002", date(2024, 1, 1), date(2024, 1, 1), Sector.independent, "N", None, None, None),
        ("1-003", date(2024, 1, 1), date(2024, 1, 1), Sector.independent, "N", None, "3", 2),
        ("1-001", date(2024, 1, 9), date(2024, 2, 1), Sector.independent, "Y", 10, "1", 4),
        ("1-002", date(2024, 1, 9), date(2024, 2, 1), Sector.independent, "N", None, None, None),
        ("1-003", date(2024, 1, 9), date(2024, 2, 1), Sector.independent, "N", None, "3", 5),
        ("1-001", date(2024, 3, 1), date(2024, 3, 1), Sector.independent, "Y", 10, None, None),
        ("1-002", date(2024, 3, 1), date(2024, 3, 1), Sector.independent, "N", None, None, None),
        ("1-003", date(2024, 3, 1), date(2024, 3, 1), Sector.independent, "N", None, "4", 6),
    ]
    # fmt: on

    data_to_merge_with_care_home_col_rows = [
        ("1-001", "Y", date(2024, 1, 1), 10),
        ("1-002", "N", date(2024, 1, 1), 20),
        ("1-003", "Y", date(2024, 1, 1), 30),
        ("1-001", "Y", date(2024, 2, 1), 1),
        ("1-002", "N", date(2024, 2, 1), 4),
    ]
    # fmt: off
    expected_merged_with_care_home_col_rows = [
        (date(2024, 1, 1), "1-001", Sector.independent, "Y", 10, 10, date(2024, 1, 1)),
        (date(2024, 1, 1), "1-002", Sector.independent, "N", None, 20, date(2024, 1, 1)),
        (date(2024, 1, 1), "1-003", Sector.independent, "N", None, None, date(2024, 1, 1)),
        (date(2024, 2, 1), "1-001", Sector.independent, "Y", 10, 1, date(2024, 2, 1)),
        (date(2024, 2, 1), "1-002", Sector.independent, "N", None, 4, date(2024, 2, 1)),
        (date(2024, 2, 1), "1-003", Sector.independent, "N", None, None, date(2024, 2, 1)),
        (date(2024, 3, 1), "1-001", Sector.independent, "Y", 10, 1, date(2024, 2, 1)),
        (date(2024, 3, 1), "1-002", Sector.independent, "N", None, 4, date(2024, 2, 1)),
        (date(2024, 3, 1), "1-003", Sector.independent, "N", None, None, date(2024, 2, 1)),
    ]
    # fmt: on


@dataclass
class ValidateMergedIndCqcData:
    cqc_locations_rows = [
        (date(2024, 1, 1), "1-001", "Independent", "Y", 10),
        (date(2024, 1, 1), "1-002", "Independent", "N", None),
        (date(2024, 2, 1), "1-001", "Independent", "Y", 10),
        (date(2024, 2, 1), "1-002", "Independent", "N", None),
    ]

    # fmt: off
    merged_ind_cqc_rows = [
        ("1-001", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5),
        ("1-002", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5),
        ("1-001", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5),
        ("1-002", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", "prov_name", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, "estab_1", "org_1", 5, 5),
    ]
    # fmt: on

    calculate_expected_size_rows = [
        ("1-001", Sector.independent),
        ("1-002", Sector.local_authority),
        ("1-003", None),
    ]


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
    model_pir_filled_posts_rows = [
        ("loc 1", date(2024, 1, 1), CareHome.not_care_home, 10),
        ("loc 2", date(2024, 1, 1), CareHome.not_care_home, None),
        ("loc 3", date(2024, 1, 1), CareHome.care_home, 10),
        ("loc 4", date(2024, 1, 1), CareHome.care_home, None),
    ]
    expected_model_pir_filled_posts_rows = [
        ("loc 1", date(2024, 1, 1), CareHome.not_care_home, 10, 10.64384),
        ("loc 2", date(2024, 1, 1), CareHome.not_care_home, None, None),
        ("loc 3", date(2024, 1, 1), CareHome.care_home, 10, None),
        ("loc 4", date(2024, 1, 1), CareHome.care_home, None, None),
    ]

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
class ValidateImputedIndCqcAscwdsAndPir:
    # fmt: off
    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1),),
        ("1-000000002", date(2024, 1, 1),),
        ("1-000000001", date(2024, 2, 1),),
        ("1-000000002", date(2024, 2, 1),),
    ]

    imputed_ind_cqc_ascwds_and_pir_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, 5, "source", 5.0, 5),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, 5, "source", 5.0, 5),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, 5, "source", 5.0, 5),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, RegistrationStatus.registered, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", 5, 5, "source", 5.0, 5),
    ]
    # fmt: on

    calculate_expected_size_rows = [
        (
            "1-000000001",
            date(2024, 1, 1),
        ),
    ]


@dataclass
class TrainLinearRegressionModelData:
    feature_rows = [
        ("1-001", Vectors.dense([12.0, 0.0, 1.0])),
        ("1-002", Vectors.dense([50.0, 1.0, 1.0])),
        ("1-003", None),
    ]


@dataclass
class ModelMetrics:
    model_metrics_rows = [
        ("1-001", None, 50.0, Vectors.dense([10.0, 1.0, 0.0])),
        ("1-002", 37, 40.0, Vectors.dense([20.0, 0.0, 1.0])),
    ]

    calculate_residual_non_res_rows = [
        ("1-001", None, 50.0, 46.8),
        ("1-002", None, 10.0, 43.2),
    ]
    expected_calculate_residual_non_res_rows = [
        ("1-001", None, 50.0, 46.8, 3.2),
        ("1-002", None, 10.0, 43.2, -33.2),
    ]

    calculate_residual_care_home_rows = [
        ("1-001", 50, 60.0, 1.1),
        ("1-002", 2, 5.0, 6.0),
    ]
    expected_calculate_residual_care_home_rows = [
        ("1-001", 50, 60.0, 1.1, 5.0),
        ("1-002", 2, 5.0, 6.0, -7.0),
    ]

    generate_metric_rows = [
        ("1-001", 50.0, 46.8),
        ("1-002", 10.0, 12.2),
    ]

    generate_proportion_of_predictions_within_range_rows = [
        ("1-001", -15.0),
        ("1-002", -10.0),
        ("1-003", 0.0),
        ("1-004", 10.0),
        ("1-005", 15.0),
    ]
    range_cutoff: float = 10.0
    expected_proportion: float = 0.6

    combine_metrics_current_rows = [
        ("model_name", "2.0.0", "run=1", 0.12, 1.2, 0.45, 0.78),
    ]
    combine_metrics_previous_rows = [
        ("model_name", "1.0.0", 0.1),
    ]
    expected_combined_metrics_rows = [
        ("model_name", "2.0.0", "run=1", 0.12, 1.2, 0.45, 0.78),
        ("model_name", "1.0.0", None, 0.1, None, None, None),
    ]


@dataclass
class RunLinearRegressionModelData:
    feature_rows = [
        ("1-001", 10, Vectors.dense([12.0, 0.0, 1.0])),
        ("1-002", 40, Vectors.dense([50.0, 1.0, 1.0])),
        ("1-003", None, None),
    ]


@dataclass
class ArchiveFilledPostsEstimates:
    filled_posts_rows = [("loc 1", date(2024, 1, 1))]

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

    create_archive_date_partitions_rows = [("loc 1", date(2024, 1, 2))]
    expected_create_archive_date_partitions_rows = [
        ("loc 1", date(2024, 1, 2), "02", "01", "2024", "2024-01-02 12:00"),
    ]

    single_digit_number = 9
    expected_single_digit_number_as_string = "09"
    double_digit_number = 10
    expected_double_digit_number_as_string = "10"


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
