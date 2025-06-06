from dataclasses import dataclass
from datetime import date

from pyspark.ml.linalg import Vectors

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CareHome,
    EstimateFilledPostsSource,
    JobGroupLabels,
    MainJobRoleLabels,
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
