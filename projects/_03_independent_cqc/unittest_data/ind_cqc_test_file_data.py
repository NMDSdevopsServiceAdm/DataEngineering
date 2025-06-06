from dataclasses import dataclass
from datetime import date

from pyspark.ml.linalg import Vectors

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import (
    CareHome,
    Dormancy,
    PrimaryServiceType,
    Region,
    RegistrationStatus,
    RelatedLocation,
    RUI,
    Sector,
    Services,
    Specialisms,
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
    # fmt: on


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
