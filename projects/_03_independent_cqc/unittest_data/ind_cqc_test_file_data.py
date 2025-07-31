from dataclasses import dataclass
from datetime import date

from pyspark.ml.linalg import Vectors

from projects._03_independent_cqc._02_clean.utils.ascwds_filled_posts_calculator.difference_within_range import (
    ascwds_filled_posts_difference_within_range_source_description,
)
from projects._03_independent_cqc._02_clean.utils.ascwds_filled_posts_calculator.total_staff_equals_worker_records import (
    ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_columns_by_dataset import (
    DiagnosticOnKnownFilledPostsCategoricalValues as CatValues,
)
from utils.column_values.categorical_column_values import (
    AscwdsFilteringRule,
    CareHome,
    Dormancy,
    EstimateFilledPostsSource,
    JobGroupLabels,
    MainJobRoleLabels,
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

    test_job_role_to_job_group_dict = {
        MainJobRoleLabels.care_worker: JobGroupLabels.direct_care,
        MainJobRoleLabels.senior_care_worker: JobGroupLabels.direct_care,
        MainJobRoleLabels.middle_management: JobGroupLabels.managers,
        MainJobRoleLabels.senior_management: JobGroupLabels.managers,
        MainJobRoleLabels.registered_nurse: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.social_worker: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.admin_staff: JobGroupLabels.other,
        MainJobRoleLabels.ancillary_staff: JobGroupLabels.other,
    }
    create_estimate_filled_posts_job_group_columns_rows = [
        (1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0),
        (None, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0),
    ]
    expected_create_estimate_filled_posts_job_group_columns_rows = [
        (1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 2.0, 4.0, 6.0, 8.0),
        (None, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, None, 4.0, 6.0, 8.0),
    ]

    create_job_role_estimates_data_validation_columns_rows = [
        (date(2024, 1, 1), 10.0, 1.0, 1.0, 2.0, 3.0, 4.0),
        (date(2024, 1, 1), 10.0, 1.0, 1.0, 2.0, 3.0, 4.0),
        (date(2025, 1, 1), 10.0, 4.0, 4.0, 3.0, 2.0, 1.0),
        (date(2025, 1, 1), 10.0, 4.0, 4.0, 3.0, 2.0, 1.0),
    ]
    expected_create_job_role_estimates_data_validation_columns_rows = [
        (date(2024, 1, 1), 10.0, 1.0, 1.0, 2.0, 3.0, 4.0, 0.1, 0.1, 0.2, 0.4, 0.3),
        (date(2024, 1, 1), 10.0, 1.0, 1.0, 2.0, 3.0, 4.0, 0.1, 0.1, 0.2, 0.4, 0.3),
        (date(2025, 1, 1), 10.0, 4.0, 4.0, 3.0, 2.0, 1.0, 0.4, 0.4, 0.3, 0.1, 0.2),
        (date(2025, 1, 1), 10.0, 4.0, 4.0, 3.0, 2.0, 1.0, 0.4, 0.4, 0.3, 0.1, 0.2),
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
class CalculateAscwdsFilledPostsUtilsData:
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
class CleanAscwdsFilledPostOutliersData:
    # fmt: off
    unfiltered_ind_cqc_rows = [
        ("01", "prov 1", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 30.0),
        ("02", "prov 1", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 35.0),
        ("03", "prov 1", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, None, 8.0),
    ]
    # fmt: on


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
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 13.0, 4, 3.25, AscwdsFilteringRule.populated, 1.0),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", None, None,  None, 4, None, AscwdsFilteringRule.missing_data, 1.0),
        ("loc 3", "prov 1", date(2024, 1, 1), "Y", None, None, None, 4, None, AscwdsFilteringRule.missing_data, 1.0),
        ("loc 1", "prov 1", date(2024, 1, 8), "Y", "estab 1", 12.0, 12.0, 4, 3.0, AscwdsFilteringRule.populated, 1.0),
        ("loc 2", "prov 1", date(2024, 1, 8), "Y", None, None, None, 4, None, AscwdsFilteringRule.missing_data, 1.0),
    ]
    # fmt: on

    # fmt: off
    calculate_data_for_grouped_provider_identification_where_provider_has_one_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", None, 4, 12.0),
        ("loc 2", "prov 2", date(2024, 1, 1), "Y", None, None, 5, None),
        ("loc 3", "prov 3", date(2024, 1, 1), "N", "estab 3", 10.0, None, 15.0),
    ]
    expected_calculate_data_for_grouped_provider_identification_where_provider_has_one_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0, 11.0, 1, 1, 1, 4, 1, 11.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", None, 4, 12.0, 11.0, 1, 1, 0, 4, 1, 11.0),
        ("loc 2", "prov 2", date(2024, 1, 1), "Y", None, None, 5, None, None, 1, 0, 0, 5, 0, None),
        ("loc 3", "prov 3", date(2024, 1, 1), "N", "estab 3", 10.0, None, 15.0, 15.0, 1, 1, 1, None, 1, 15.0),
    ]
    # fmt: on

    # fmt: off
    calculate_data_for_grouped_provider_identification_where_provider_has_multiple_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", 13.0, 4, 20.0),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", "estab 2", 14.0, 3, 15.0),
        ("loc 2", "prov 1", date(2024, 2, 1), "Y", None, None, 5, 25.0),
        ("loc 3", "prov 2", date(2024, 1, 1), "Y", None, None, 6, 10.0),
        ("loc 4", "prov 2", date(2024, 1, 1), "N", "estab 3", None, None, None),
        ("loc 5", "prov 3", date(2024, 1, 1), "N", None, None, None, None),
        ("loc 6", "prov 3", date(2024, 1, 1), "N", None, None, None, None),
    ]
    expected_calculate_data_for_grouped_provider_identification_where_provider_has_multiple_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0, 15.0, 2, 2, 2, 7, 2, 35.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", 13.0, 4, 20.0, 15.0, 2, 1, 1, 9, 2, 35.0),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", "estab 2", 14.0, 3, 15.0, 20.0, 2, 2, 2, 7, 2, 35.0),
        ("loc 2", "prov 1", date(2024, 2, 1), "Y", None, None, 5, 25.0, 20.0, 2, 1, 1, 9, 2, 35.0),
        ("loc 3", "prov 2", date(2024, 1, 1), "Y", None, None, 6, 10.0, 10.0, 2, 1, 0, 6, 1, 10.0),
        ("loc 4", "prov 2", date(2024, 1, 1), "N", "estab 3", None, None, None, None, 2, 1, 0, 6, 1, 10.0),
        ("loc 5", "prov 3", date(2024, 1, 1), "N", None, None, None, None, None, 2, 0, 0, None, 0, None),
        ("loc 6", "prov 3", date(2024, 1, 1), "N", None, None, None, None, None, 2, 0, 0, None, 0, None),
    ]
    # fmt: on

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

    # fmt: off
    null_care_home_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.care_home, 25.0, 25.0, 2, 2, 12.5, True, AscwdsFilteringRule.populated),
        ("1-002", CareHome.care_home, 60.0, 60.0, 2, 2, 30.0, True, AscwdsFilteringRule.populated),
    ]
    expected_null_care_home_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.care_home, 25.0, None, 2, 2, None, True, AscwdsFilteringRule.care_home_location_was_grouped_provider),
        ("1-002", CareHome.care_home, 60.0, None, 2, 2, None, True, AscwdsFilteringRule.care_home_location_was_grouped_provider),
    ]
    null_care_home_grouped_providers_where_location_does_not_meet_criteria_rows = [
        ("1-001", CareHome.not_care_home, 25.0, 25.0, None, 2, None, True, AscwdsFilteringRule.populated),  # non res location
        ("1-002", CareHome.care_home, 25.0, 25.0, 2, 3, 12.5, False, AscwdsFilteringRule.populated),  # not identified as potential grouped provider
        ("1-003", CareHome.care_home, 24.0, 24.0, 2, 2, 12.0, True, AscwdsFilteringRule.populated),  # below minimum size
        ("1-004", CareHome.care_home, 25.0, 25.0, 20, 22, 1.25, True, AscwdsFilteringRule.populated), # below location and provider threshold
    ]
    # fmt: on

    # fmt: off
    null_non_res_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.not_care_home, True, 50.0, 50.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),
        ("1-002", CareHome.not_care_home, True, None, None, 15.0, 2, 25.0, None),
    ]
    expected_null_non_res_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.not_care_home, True, 50.0, None, 10.0, 2, 25.0, AscwdsFilteringRule.non_res_location_was_grouped_provider),
        ("1-002", CareHome.not_care_home, True, None, None, 15.0, 2, 25.0, None),
    ]
    null_non_res_grouped_providers_when_does_not_meet_criteria_rows = [
        ("1-001", CareHome.care_home, True, 50.0, 50.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),  # care home location
        ("1-002", CareHome.not_care_home, False, 50.0, 50.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),  # not identified as potential grouped provider
        ("1-003", CareHome.not_care_home, True, 49.0, 49.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),  # below minimum size
        ("1-004", CareHome.not_care_home, True, 50.0, 50.0, None, 2, 25.0, AscwdsFilteringRule.populated),  # no PIR data for location
        ("1-005", CareHome.not_care_home, True, 50.0, 50.0, 40.0, 2, 45.0, AscwdsFilteringRule.populated),  # below location threshold and provider sum
        ("1-006", CareHome.not_care_home, True, 50.0, 50.0, 40.0, 1, 40.0, AscwdsFilteringRule.populated),  # below location threshold and provider count
        ("1-008", CareHome.not_care_home, True, 50.0, None, 10.0, 2, 25.0, AscwdsFilteringRule.contained_invalid_missing_data_code),  # already filtered
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
        ("1-00001", date(2022, 2, 1), date(2019, 1, 1), 35, 1, Region.south_east, Dormancy.dormant, [Services.domiciliary_care_service], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia], PrimaryServiceType.non_residential, None, 20.0, 17.5, CareHome.not_care_home, RUI.rural_hamlet, RelatedLocation.has_related_location, '2022', '02', '01', '20220201'),
        ("1-00002", date(2022, 2, 1), date(2019, 2, 1), 36, 10, Region.south_east, Dormancy.not_dormant, [Services.domiciliary_care_service], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia], PrimaryServiceType.non_residential, 67.0, 20.0, 20.0, CareHome.not_care_home, RUI.rural_hamlet, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
        ("1-00003", date(2022, 2, 1), date(2019, 2, 1), 36, 1, Region.south_west, Dormancy.dormant, [Services.urgent_care_services, Services.supported_living_service], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia], PrimaryServiceType.non_residential, None, 20.0, 20.0, CareHome.not_care_home, RUI.rural_hamlet, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
        ("1-00004", date(2022, 2, 1), date(2019, 2, 1), 36, None, Region.north_east, None, [Services.domiciliary_care_service], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia], PrimaryServiceType.non_residential, None, 20.0, 20.0, CareHome.not_care_home, RUI.rural_hamlet, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
        ("1-00005", date(2022, 2, 1), date(2019, 2, 1), 36, 10, Region.north_east, Dormancy.not_dormant, [Services.specialist_college_service, Services.domiciliary_care_service], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia, Specialisms.mental_health], PrimaryServiceType.non_residential, None, 20.0, 20.0, CareHome.not_care_home, RUI.urban_city, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
        ("1-00006", date(2022, 2, 1), date(2019, 2, 1), 36, 10, Region.north_east, Dormancy.not_dormant, [Services.specialist_college_service, Services.domiciliary_care_service], [{IndCQC.name:"name", IndCQC.code: "code"}], None, PrimaryServiceType.non_residential, None, 20.0, 20.0, CareHome.not_care_home, RUI.urban_city, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
        ("1-00007", date(2022, 2, 1), date(2019, 2, 1), 36, 1, Region.north_west, Dormancy.dormant, [Services.supported_living_service, Services.care_home_service_with_nursing], [{IndCQC.name:"name", IndCQC.code: "code"}], [Specialisms.dementia], PrimaryServiceType.care_home_with_nursing, None, 20.0, 20.0, CareHome.care_home, RUI.urban_city, RelatedLocation.no_related_location, '2022', '02', '01', '20220201'),
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

    add_squared_column_rows = [
        ("1-001", None),
        ("1-002", 0.0),
        ("1-003", 2.0),
        ("1-004", 4.0),
    ]
    expected_add_squared_column_rows = [
        ("1-001", None, None),
        ("1-002", 0.0, 0.0),
        ("1-003", 2.0, 4.0),
        ("1-004", 4.0, 16.0),
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
class ModelPrimaryServiceRateOfChange:
    # fmt: off
    primary_service_rate_of_change_rows = [
        ("1-001", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 1.0, 3.0),
        ("1-001", CareHome.care_home, 1704153600, PrimaryServiceType.care_home_only, 10, 1.0, 2.8),
        ("1-001", CareHome.care_home, 1704240000, PrimaryServiceType.care_home_only, 10, 1.0, 3.4),
        ("1-001", CareHome.care_home, 1704326400, PrimaryServiceType.care_home_only, 10, 1.0, 3.2),
        ("1-002", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 1.0, 2.0),
        ("1-002", CareHome.care_home, 1704153600, PrimaryServiceType.care_home_only, 10, 1.0, None),
        ("1-002", CareHome.care_home, 1704240000, PrimaryServiceType.care_home_only, 10, 1.0, None),
        ("1-002", CareHome.care_home, 1704326400, PrimaryServiceType.care_home_only, 10, 1.0, 3.2),
        ("1-003", CareHome.not_care_home, 1704067200, PrimaryServiceType.non_residential, None, 0.0, 40.0),
        ("1-003", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 0.0, 50.0),
        ("1-004", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 0.0, 60.0),
        ("1-005", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 1.0, 4.0),
        ("1-005", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 0.0, 50.0),
    ]
    expected_primary_service_rate_of_change_rows = [
        ("1-001", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 1.0, 3.0, 1.0),
        ("1-001", CareHome.care_home, 1704153600, PrimaryServiceType.care_home_only, 10, 1.0, 2.8, 1.03999),
        ("1-001", CareHome.care_home, 1704240000, PrimaryServiceType.care_home_only, 10, 1.0, 3.4, 1.1176),
        ("1-001", CareHome.care_home, 1704326400, PrimaryServiceType.care_home_only, 10, 1.0, 3.2, 1.0854),
        ("1-002", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 1.0, 2.0, 1.0),
        ("1-002", CareHome.care_home, 1704153600, PrimaryServiceType.care_home_only, 10, 1.0, None, 1.03999),
        ("1-002", CareHome.care_home, 1704240000, PrimaryServiceType.care_home_only, 10, 1.0, None, 1.1176),
        ("1-002", CareHome.care_home, 1704326400, PrimaryServiceType.care_home_only, 10, 1.0, 3.2, 1.0854),
        ("1-003", CareHome.not_care_home, 1704067200, PrimaryServiceType.non_residential, None, 0.0, 40.0, 1.0),
        ("1-003", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 0.0, 50.0, 1.25),
        ("1-004", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 0.0, 60.0, 1.25),
        ("1-005", CareHome.care_home, 1704067200, PrimaryServiceType.care_home_only, 10, 1.0, 4.0, 1.0),
        ("1-005", CareHome.not_care_home, 1704153600, PrimaryServiceType.non_residential, None, 0.0, 50.0, 1.25),
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
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 1672531200, 1.1, None),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 1672617600, 1.2, 1.1),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 1672704000, 1.3, 1.2),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 1672790400, None, 1.3),
        ("1-002", PrimaryServiceType.care_home_only, 1.0, 1672531200, 1.4, None),
        ("1-002", PrimaryServiceType.care_home_only, 1.0, 1672617600, 1.3, 1.4),
        ("1-003", PrimaryServiceType.care_home_only, 2.0, 1672531200, 1.5, None),
        ("1-003", PrimaryServiceType.care_home_only, 2.0, 1672617600, 1.6, 1.5),
        ("1-004", PrimaryServiceType.non_residential, 0.0, 1672531200, 10.0, None),
        ("1-004", PrimaryServiceType.non_residential, 0.0, 1672617600, 20.0, 10.0),
        ("1-004", PrimaryServiceType.non_residential, 0.0, 1672704000, 30.0, 20.0),
    ]
    expected_add_rolling_sum_columns_rows = [
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 1672531200, 1.1, None, None, None),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 1672617600, 1.2, 1.1, 2.5, 2.5),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 1672704000, 1.3, 1.2, 3.8, 3.7),
        ("1-001", PrimaryServiceType.care_home_only, 1.0, 1672790400, None, 1.3, 3.8, 3.7),
        ("1-002", PrimaryServiceType.care_home_only, 1.0, 1672531200, 1.4, None, None, None),
        ("1-002", PrimaryServiceType.care_home_only, 1.0, 1672617600, 1.3, 1.4, 2.5, 2.5),
        ("1-003", PrimaryServiceType.care_home_only, 2.0, 1672531200, 1.5, None, None, None),
        ("1-003", PrimaryServiceType.care_home_only, 2.0, 1672617600, 1.6, 1.5, 1.6, 1.5),
        ("1-004", PrimaryServiceType.non_residential, 0.0, 1672531200, 10.0, None, None, None),
        ("1-004", PrimaryServiceType.non_residential, 0.0, 1672617600, 20.0, 10.0, 20.0, 10.0),
        ("1-004", PrimaryServiceType.non_residential, 0.0, 1672704000, 30.0, 20.0, 50.0, 30.0),
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
        ("1-001", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.0),
        ("1-001", 1704153600, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 2.8),
        ("1-001", 1704240000, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.4),
        ("1-001", 1704326400, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.2),
        ("1-002", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 2.0),
        ("1-002", 1704153600, CareHome.care_home, 10, PrimaryServiceType.care_home_only, None),
        ("1-002", 1704240000, CareHome.care_home, 10, PrimaryServiceType.care_home_only, None),
        ("1-002", 1704326400, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.2),
        ("1-003", 1704067200, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 40.0),
        ("1-003", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 50.0),
        ("1-004", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 60.0),
        ("1-005", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 4.0),
        ("1-005", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 50.0),
    ]
    expected_primary_service_rate_of_change_trendline_rows = [
        ("1-001", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.0, 1.0),
        ("1-001", 1704153600, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 2.8, 1.03999),
        ("1-001", 1704240000, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.4, 1.16235),
        ("1-001", 1704326400, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.2, 1.26158),
        ("1-002", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 2.0, 1.0),
        ("1-002", 1704153600, CareHome.care_home, 10, PrimaryServiceType.care_home_only, None, 1.03999),
        ("1-002", 1704240000, CareHome.care_home, 10, PrimaryServiceType.care_home_only, None, 1.16235),
        ("1-002", 1704326400, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 3.2, 1.26158),
        ("1-003", 1704067200, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 40.0, 1.0),
        ("1-003", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 50.0, 1.25),
        ("1-004", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 60.0, 1.25),
        ("1-005", 1704067200, CareHome.care_home, 10, PrimaryServiceType.care_home_only, 4.0, 1.0),
        ("1-005", 1704153600, CareHome.not_care_home, None, PrimaryServiceType.non_residential, 50.0, 1.25),
    ]
    # fmt: on

    calculate_rate_of_change_trendline_mock_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 1672531200, 1.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672617600, 1.5),
        (PrimaryServiceType.care_home_only, 1.0, 1672704000, 3.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672790400, 4.5),
        (PrimaryServiceType.non_residential, 0.0, 1672531200, 1.0),
        (PrimaryServiceType.non_residential, 0.0, 1672617600, 1.2),
        (PrimaryServiceType.non_residential, 0.0, 1672704000, 1.2),
        (PrimaryServiceType.non_residential, 0.0, 1672790400, 1.8),
    ]

    deduplicate_dataframe_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 1672531200, 1.0, 2.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672617600, 1.1, 2.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672704000, 1.2, 2.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672790400, 1.3, 2.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672531200, 1.0, 2.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672617600, 1.1, 2.0),
        (PrimaryServiceType.care_home_only, 2.0, 1672531200, 1.0, 2.0),
        (PrimaryServiceType.non_residential, 0.0, 1672617600, 10.0, 2.0),
        (PrimaryServiceType.non_residential, 0.0, 1672617600, 10.0, 2.0),
    ]
    expected_deduplicate_dataframe_rows = [
        (PrimaryServiceType.care_home_only, 1.0, 1672531200, 1.0),
        (PrimaryServiceType.care_home_only, 1.0, 1672617600, 1.1),
        (PrimaryServiceType.care_home_only, 1.0, 1672704000, 1.2),
        (PrimaryServiceType.care_home_only, 1.0, 1672790400, 1.3),
        (PrimaryServiceType.care_home_only, 2.0, 1672531200, 1.0),
        (PrimaryServiceType.non_residential, 0.0, 1672617600, 10.0),
    ]

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
            1.0,
            1704067200,
            date(2024, 1, 1),
            11,
            None,
            None,
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
            1.0,
            1706832000,
            date(2024, 2, 1),
            11,
            None,
            None,
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
            0.0,
            1704067200,
            None,
            None,
            date(2024, 1, 1),
            10,
            "2024",
            "01",
            "01",
            "20240101",
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
    # fmt: on

    # fmt: off
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
