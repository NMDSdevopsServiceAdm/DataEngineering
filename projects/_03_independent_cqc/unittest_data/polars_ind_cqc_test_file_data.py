import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional

import numpy as np
import pytest

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    ASCWDSFilledPostsSource,
    AscwdsFilteringRule,
    CareHome,
    CTFilteringRule,
    Dormancy,
    EstimateFilledPostsSource,
    JobGroupLabels,
    JobRoleFilteringRule,
    MainJobRoleLabels,
    PrimaryServiceType,
    Region,
    Sector,
)


@dataclass
class PrepareJobRoleCountsUtilsData:
    aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_many_in_same_role_rows = [
        ("101", "101"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        (MainJobRoleLabels.care_worker, MainJobRoleLabels.care_worker),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_many_in_same_role_rows = [
        ("101", "101"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        (MainJobRoleLabels.care_worker, MainJobRoleLabels.senior_care_worker),
        (2, 0),
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_multiple_import_dates_rows = [
        ("101", "101"),
        (date(2025, 1, 1), date(2025, 1, 2)),
        (MainJobRoleLabels.care_worker, MainJobRoleLabels.care_worker),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_multiple_import_dates_rows = [
        ("101", "101", "101", "101"),
        (date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 2)),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
        ),
        (1, 0, 1, 0),
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_when_multiple_estabs_have_same_import_date_rows = [
        ("101", "102"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        (MainJobRoleLabels.care_worker, MainJobRoleLabels.care_worker),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_multiple_estabs_have_same_import_date_rows = [
        ("101", "101", "102", "102"),
        (date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1)),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
        ),
        (1, 0, 1, 0),
    ]

    filter_to_cqc_locations_rows = [("1", "", None)]

    expected_filter_to_cqc_locations_rows = ["1"]


@dataclass
class FeaturesEngineeringUtilsData:
    add_array_column_count_with_one_element_rows = [
        ("1-001", ["name"]),
    ]
    expected_add_array_column_count_with_one_element_rows = [
        ("1-001", ["name"], 1),
    ]

    add_array_column_count_with_multiple_elements_rows = [
        ("1-001", ["name_1", "name_2", "name_3"]),
    ]
    expected_add_array_column_count_with_multiple_elements_rows = [
        ("1-001", ["name_1", "name_2", "name_3"], 3),
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

    add_date_index_column_same_index_for_same_date_rows = [
        ("1-0001", CareHome.not_care_home, date(2024, 12, 1)),
        ("1-0002", CareHome.not_care_home, date(2024, 12, 1)),
    ]
    expected_add_date_index_column_same_index_for_same_date_rows = [
        ("1-0001", CareHome.not_care_home, date(2024, 12, 1), 1),
        ("1-0002", CareHome.not_care_home, date(2024, 12, 1), 1),
    ]

    add_date_index_column_applies_incremental_index_rows = [
        ("1-0001", CareHome.not_care_home, date(2024, 12, 1)),
        ("1-0002", CareHome.not_care_home, date(2024, 12, 1)),
        ("1-0003", CareHome.not_care_home, date(2025, 2, 1)),
    ]
    expected_add_date_index_column_applies_incremental_index_rows = [
        ("1-0001", CareHome.not_care_home, date(2024, 12, 1), 1),
        ("1-0002", CareHome.not_care_home, date(2024, 12, 1), 1),
        ("1-0003", CareHome.not_care_home, date(2025, 2, 1), 2),
    ]

    add_date_index_column_indexes_by_care_home_rows = [
        ("1-0001", CareHome.not_care_home, date(2024, 12, 1)),
        ("1-0002", CareHome.not_care_home, date(2025, 2, 1)),
        ("1-0003", CareHome.care_home, date(2025, 2, 1)),
    ]
    expected_add_date_index_column_indexes_by_care_home_rows = [
        ("1-0001", CareHome.not_care_home, date(2024, 12, 1), 1),
        ("1-0002", CareHome.not_care_home, date(2025, 2, 1), 2),
        ("1-0003", CareHome.care_home, date(2025, 2, 1), 1),
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

    group_rural_urban_sparse_categories_non_sparse_do_not_change_rows = [
        ("1-001", "Rural"),
        ("1-002", "Urban"),
    ]
    expected_group_rural_urban_sparse_categories_non_sparse_do_not_change_rows = [
        ("1-001", "Rural", "Rural"),
        ("1-002", "Urban", "Urban"),
    ]

    group_rural_urban_sparse_categories_identifies_sparse_rows = [
        ("1-001", "Rural sparse"),
        ("1-002", "Another with sparse in it"),
        ("1-003", "Capitalised Sparse"),
    ]
    expected_group_rural_urban_sparse_categories_identifies_sparse_rows = [
        ("1-001", "Rural sparse", "Sparse setting"),
        ("1-002", "Another with sparse in it", "Sparse setting"),
        ("1-003", "Capitalised Sparse", "Sparse setting"),
    ]

    group_rural_urban_sparse_categories_combination_rows = [
        ("1-001", "Rural"),
        ("1-002", "Rural sparse"),
        ("1-003", "Another with sparse in it"),
        ("1-004", "Urban"),
        ("1-005", "Sparse with a capital S"),
    ]
    expected_group_rural_urban_sparse_categories_combination_rows = [
        ("1-001", "Rural", "Rural"),
        ("1-002", "Rural sparse", "Sparse setting"),
        ("1-003", "Another with sparse in it", "Sparse setting"),
        ("1-004", "Urban", "Urban"),
        ("1-005", "Sparse with a capital S", "Sparse setting"),
    ]

    add_power_column_rows = [
        ("1-001", None),
        ("1-002", 0),
        ("1-003", 2),
        ("1-004", 4),
    ]
    expected_add_power_column_rows = [
        ("1-001", None, None),
        ("1-002", 0, 0),
        ("1-003", 2, 4),
        ("1-004", 4, 16),
    ]

    select_and_filter_features_rows = [
        ("1-001", date(2025, 1, 1), "Y", 10, 5, 1, 100.0, 1),
        ("1-002", date(2025, 1, 2), "N", None, 10, 1, 150.0, 1),
        ("1-003", date(2025, 1, 3), "Y", 30, None, 0, 200.0, 1),
    ]
    expected_select_and_filter_features_rows = [
        ("1-001", date(2025, 1, 1), 1, 100.0, 10, 5, 1),
    ]


@dataclass
class ModelTrainingUtilsData:
    split_train_test_rows = [
        ("1-001", 10.0),
        ("1-001", 11.0),
        ("1-002", 20.0),
        ("1-002", 21.0),
        ("1-003", 30.0),
        ("1-003", 31.0),
        ("1-004", 40.0),
        ("1-004", 41.0),
    ]

    convert_dataframe_to_numpy_basic_rows = [
        ("1-001", 1, 10, 5.0),
        ("1-002", 2, 20, 6.0),
        ("1-003", 3, 30, 7.0),
    ]
    expected_numpy_multiple_feature_cols_X = np.array([[1, 10], [2, 20], [3, 30]])
    expected_numpy_single_feature_col_X = np.array([[1], [2], [3]])
    expected_numpy_col_y = np.array([5, 6, 7])


@dataclass
class ModelUtilsData:
    features_rows = [
        ("1-001", date(2025, 1, 1), 1, 4),
        ("1-002", date(2025, 1, 1), 2, 5),
        ("1-003", date(2025, 1, 1), 3, 6),
    ]

    predictions = np.array([10.5, 11.0, 12.3])
    mismatch_predictions = np.array([10.5, 11.0])

    expected_predictions_dataframe_rows = [
        ("1-001", date(2025, 1, 1), 1, 4, 10.5, "v1.2.0_r7"),
        ("1-002", date(2025, 1, 1), 2, 5, 11.0, "v1.2.0_r7"),
        ("1-003", date(2025, 1, 1), 3, 6, 12.3, "v1.2.0_r7"),
    ]


@dataclass
class ValidateModelsData:
    validate_model_feature_rows = [
        ("1-001", date(2025, 1, 1), "N", None, ["activity 1"], 12.0, ["service 1"], ["specialism 1"], "rui", "region", "Y", 10, None),
        ("1-002", date(2025, 1, 1), "Y", "Y", ["activity 1"], 12.0, ["service 1"], ["specialism 1"], "rui", "region", "Y", 10, 5),
        ("1-003", date(2025, 1, 1), "N", "Y", ["activity 1"], 12.0, None, ["specialism 1"], "rui", "region", "Y", 10, 5),
        ("1-004", date(2025, 1, 1), "N", "Y", ["activity 1"], 12.0, ["service 1"], ["specialism 1"], "rui", "region", "Y", 10, 5),
        ("1-005", date(2024, 1, 1), "N", None, ["activity 1"], 12.0, ["service 1"], ["specialism 1"], "rui", "region", "Y", 10, None),
    ] # fmt: skip
    expected_get_expected_row_count_rows = 1


@dataclass
class ValidateModel01FeaturesData:
    validation_rows = [
        ("1-001", date(2025, 1, 1), "Y", "Y", "feature", "feature", None),
    ]
    expected_get_expected_row_count_rows = 2


@dataclass
class MergeIndCQCData:
    cqc_location_data = [
        ("1-001", date(2024, 1, 1), "Y", Sector.independent),
        ("1-002", date(2024, 1, 1), "Y", Sector.local_authority),
        ("1-003", date(2024, 1, 1), "N", Sector.independent),
    ]
    cqc_pir_data = [
        ("1-001", date(2024, 1, 1), "Y", "pir_value"),
        ("1-003", date(2024, 1, 1), "N", "pir_value"),
    ]
    ascwds_workplace_data = [
        ("1-001", date(2024, 1, 1), "ascwds_value"),
        ("1-003", date(2024, 1, 1), "ascwds_value"),
    ]
    ct_non_res_data = [
        ("1-001", date(2024, 1, 1), "Y", "ct_non_res_value"),
        ("1-003", date(2024, 1, 1), "N", "ct_non_res_value"),
    ]
    ct_care_home_data = [
        ("1-001", date(2024, 1, 1), "Y", "ct_care_home_value"),
        ("1-003", date(2024, 1, 1), "N", "ct_care_home_value"),
    ]

    expected_data = [
        ("1-001", date(2024, 1, 1), "Y", Sector.independent, date(2024, 1, 1), "pir_value", date(2024, 1, 1), "ascwds_value", date(2024, 1, 1), "ct_non_res_value", date(2024, 1, 1), "ct_care_home_value"),
        ("1-003", date(2024, 1, 1), "N", Sector.independent, date(2024, 1, 1), "pir_value", date(2024, 1, 1), "ascwds_value", date(2024, 1, 1), "ct_non_res_value", date(2024, 1, 1), "ct_care_home_value"),
    ] # fmt: skip


@dataclass
class MergeUtilsData:

    clean_cqc_location_for_merge_rows = [
        ("1-001", date(2024, 1, 1), Sector.independent, "Y", 10),
        ("1-002", date(2024, 1, 1), Sector.independent, "N", None),
        ("1-003", date(2024, 1, 1), Sector.independent, "N", None),
        ("1-001", date(2024, 2, 1), Sector.independent, "Y", 10),
        ("1-002", date(2024, 2, 1), Sector.independent, "N", None),
        ("1-003", date(2024, 2, 1), Sector.independent, "N", None),
        ("1-001", date(2024, 3, 1), Sector.independent, "Y", 10),
        ("1-002", date(2024, 3, 1), Sector.independent, "N", None),
        ("1-003", date(2024, 3, 1), Sector.independent, "N", None),
    ] # fmt: skip

    data_to_merge_without_care_home_col_rows = [
        ("1-001", date(2024, 1, 1), "1", 1),
        ("1-003", date(2024, 1, 1), "3", 2),
        ("1-001", date(2024, 1, 5), "1", 3),
        ("1-001", date(2024, 1, 9), "1", 4),
        ("1-003", date(2024, 1, 9), "3", 5),
        ("1-003", date(2024, 3, 1), "4", 6),
    ]

    expected_merged_without_care_home_col_rows = [
        ("1-001", date(2024, 1, 1), Sector.independent, "Y", 10, date(2024, 1, 1), "1", 1),
        ("1-002", date(2024, 1, 1), Sector.independent, "N", None, date(2024, 1, 1), None, None),
        ("1-003", date(2024, 1, 1), Sector.independent, "N", None, date(2024, 1, 1), "3", 2),
        ("1-001", date(2024, 2, 1), Sector.independent, "Y", 10, date(2024, 1, 9), "1", 4),
        ("1-002", date(2024, 2, 1), Sector.independent, "N", None, date(2024, 1, 9), None, None),
        ("1-003", date(2024, 2, 1), Sector.independent, "N", None, date(2024, 1, 9), "3", 5),
        ("1-001", date(2024, 3, 1), Sector.independent, "Y", 10, date(2024, 3, 1), None, None),
        ("1-002", date(2024, 3, 1), Sector.independent, "N", None, date(2024, 3, 1), None, None),
        ("1-003", date(2024, 3, 1), Sector.independent, "N", None, date(2024, 3, 1), "4", 6),
    ] # fmt: skip

    data_to_merge_with_care_home_col_rows = [
        ("1-001", "Y", date(2024, 1, 1), 10),
        ("1-002", "N", date(2024, 1, 1), 20),
        ("1-003", "Y", date(2024, 1, 1), 30),
        ("1-001", "Y", date(2024, 2, 1), 1),
        ("1-002", "N", date(2024, 2, 1), 4),
    ]

    expected_merged_with_care_home_col_rows = [
        ("1-001", date(2024, 1, 1), Sector.independent, "Y", 10, date(2024, 1, 1), 10),
        ("1-002", date(2024, 1, 1), Sector.independent, "N", None, date(2024, 1, 1), 20),
        ("1-003", date(2024, 1, 1), Sector.independent, "N", None, date(2024, 1, 1), None),
        ("1-001", date(2024, 2, 1), Sector.independent, "Y", 10, date(2024, 2, 1), 1),
        ("1-002", date(2024, 2, 1), Sector.independent, "N", None, date(2024, 2, 1), 4),
        ("1-003", date(2024, 2, 1), Sector.independent, "N", None, date(2024, 2, 1), None),
        ("1-001", date(2024, 3, 1), Sector.independent, "Y", 10, date(2024, 2, 1), 1),
        ("1-002", date(2024, 3, 1), Sector.independent, "N", None, date(2024, 2, 1), 4),
        ("1-003", date(2024, 3, 1), Sector.independent, "N", None, date(2024, 2, 1), None),
    ] # fmt: skip


@dataclass
class ValidateMergeIndCQCData:

    merged_ind_cqc_data_rows = [
        ("1-001", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, "estab_1", "org_1", 5, 5,"Y"),
        ("1-002", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, "estab_1", "org_1", 5, 5,"N"),
        ("1-001", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, "estab_1", "org_1", 5, 5,"Y"),
        ("1-002", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, "estab_1", "org_1", 5, 5,"N"),
    ] # fmt: skip

    cqc_locations_cleaned_data_rows = [
        (date(2024, 1, 1), "1-001", Sector.independent, "Y", 10),
        (date(2024, 1, 1), "1-002", Sector.independent, "N", None),
        (date(2024, 2, 1), "1-001", Sector.independent, "Y", 10),
        (date(2024, 2, 1), "1-002", Sector.independent, "N", None),
    ] # fmt: skip


@dataclass
class ValidateCleanIndCQCData:

    cleaned_ind_cqc_data_rows = [
        ("1-001", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, "estab_1", "org_1", 5, 5,"Y", "ascwds_filtering_rule", "specialist", "specialist", "specialist", 1, 1.0, "ascwds_filled_posts_source"),
        ("1-002", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, "estab_1", "org_1", 5, 5,"N", "ascwds_filtering_rule", "specialist", "specialist", "specialist", 1, 1.0, "ascwds_filled_posts_source"),
        ("1-001", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, "estab_1", "org_1", 5, 5,"Y", "ascwds_filtering_rule", "specialist", "specialist", "specialist", 1, 1.0, "ascwds_filled_posts_source"),
        ("1-002", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, ["service"], PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, "estab_1", "org_1", 5, 5,"N", "ascwds_filtering_rule", "specialist", "specialist", "specialist", 1, 1.0, "ascwds_filled_posts_source"),
    ] # fmt: skip

    merged_ind_cqc_data_rows = [
        (date(2024, 1, 1), "1-001", Sector.independent, "Y", 10),
        (date(2024, 1, 1), "1-002", Sector.independent, "N", None),
        (date(2024, 2, 1), "1-001", Sector.independent, "Y", 10),
        (date(2024, 2, 1), "1-002", Sector.independent, "N", None),
    ] # fmt: skip

    expected_size_rows = [
        ("1-001", date(2025, 1, 1), "name", "postcode", "Y", date(2025, 1, 1), 1, 1),
        ("1-002", date(2025, 1, 1), "name", "postcode", "Y", date(2025, 1, 1), 1, 1),
        ("1-001", date(2025, 1, 1), "name", "postcode", "Y", date(2025, 1, 1), 1, 1),
    ] # fmt: skip


@dataclass
class ValidateImputedIndCqcAscwdsAndPir:
    cleaned_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1)),
        ("1-000000002", date(2024, 1, 1)),
        ("1-000000001", date(2024, 2, 1)),
        ("1-000000002", date(2024, 2, 1)),
    ]

    imputed_ind_cqc_ascwds_and_pir_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, 5, "ascwds_filtering_rule", "source", 5.0, 5, 1.0, 5, 5.0),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, 5, "ascwds_filtering_rule", "source", 5.0, 5, 1.0, 5, 5.0),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, 5, "ascwds_filtering_rule", "source", 5.0, 5, 1.0, 5, 5.0),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1), "Y", "prov_1", Sector.independent, date(2024, 1, 1), "Y", 5, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", date(2024, 1, 1), "cssr", "region", "RUI", "lsoa", "msoa", 5, 5, "ascwds_filtering_rule", "source", 5.0, 5, 1.0, 5, 5.0),
    ] # fmt: skip


@dataclass
class ValidateEstimatedIndCQCFilledPostsData:
    imputed_ind_cqc_rows = [
        ("1-000000001", date(2024, 1, 1)),
        ("1-000000002", date(2024, 1, 1)),
        ("1-000000001", date(2024, 2, 1)),
        ("1-000000002", date(2024, 2, 1)),
    ]

    estimated_ind_cqc_filled_posts_rows = [
        ("1-000000001", date(2024, 1, 1), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 5.0, 5.0, "source", 5.0, 5.0, 5.0, 5.0, 5.0, 5.0),
        ("1-000000002", date(2024, 1, 1), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 5.0, 5.0, "source", 5.0, 5.0, 5.0, 5.0, 5.0, 5.0),
        ("1-000000001", date(2024, 1, 9), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 5.0, 5.0, "source", 5.0, 5.0, 5.0, 5.0, 5.0, 5.0),
        ("1-000000002", date(2024, 1, 9), date(2024, 1, 1), "Y", Sector.independent, 5, PrimaryServiceType.care_home_only, PrimaryServiceType.care_home_only, date(2024, 1, 1), "cssr", "region", 5, 5, 5, "source", 5.0, 5.0, 5, 5.0, 5.0, "source", 5.0, 5.0, 5.0, 5.0, 5.0, 5.0),
    ] # fmt: skip


@dataclass
class CalculateAscwdsFilledPostsData:
    calculate_ascwds_filled_posts_rows = [
        ("1-000001", 0,    None, None, None),                # Both 0: Return None
        ("1-000002", 500,  500,  None, None),                # Both 500: Return 500
        ("1-000003", 10,   None, None, None),                # Only know total_staff: Return None
        ("1-000004", 23,   1,    None, None),                # worker_record_count below min permitted: return None
        ("1-000005", None, 100,  None, None),                # Only know worker_records: None
        ("1-000006", 900,  600,  None, None),                # None of the rules apply: Return None
        ("1-000007", 12,   11,   None, None),                # Absolute difference is within absolute bounds: Return Average
        ("1-000008", 500,  475,  None, None),                # Absolute difference is within percentage bounds: Return Average
        ("1-000009", 10,   10,   8.0,  "already populated"), # Already populated, shouldn't change it
    ] # fmt: skip
    expected_ascwds_filled_posts_rows = [
        ("1-000001", 0,    None, None,  None),
        ("1-000002", 500,  500,  500.0, ASCWDSFilledPostsSource.worker_records_and_total_staff),
        ("1-000003", 10,   None, None,  None),
        ("1-000004", 23,   1,    None,  None),
        ("1-000005", None, 100,  None,  None),
        ("1-000006", 900,  600,  None,  None),
        ("1-000007", 12,   11,   11.5,  ASCWDSFilledPostsSource.average_of_total_staff_and_worker_records),
        ("1-000008", 500,  475,  487.5, ASCWDSFilledPostsSource.average_of_total_staff_and_worker_records),
        ("1-000009", 10,   10,   10.0,  ASCWDSFilledPostsSource.worker_records_and_total_staff),
    ] # fmt: skip
    expected_totalstaff_equal_wkrrecs_rows = [
        ("1-000001", 0,    None, None,  None),
        ("1-000002", 500,  500,  500.0, ASCWDSFilledPostsSource.worker_records_and_total_staff),
        ("1-000003", 10,   None, None,  None),
        ("1-000004", 23,   1,    None,  None),
        ("1-000005", None, 100,  None,  None),
        ("1-000006", 900,  600,  None,  None),
        ("1-000007", 12,   11,   None,  None),
        ("1-000008", 500,  475,  None,  None),
        ("1-000009", 10,   10,   8.0,   "already populated"),
    ] # fmt: skip
    expected_difference_within_range_rows = [
        ("1-000001", 0,    None, None,  None),
        ("1-000002", 500,  500,  500.0, ASCWDSFilledPostsSource.average_of_total_staff_and_worker_records),
        ("1-000003", 10,   None, None,  None),
        ("1-000004", 23,   1,    None,  None),
        ("1-000005", None, 100,  None,  None),
        ("1-000006", 900,  600,  None,  None),
        ("1-000007", 12,   11,   11.5,  ASCWDSFilledPostsSource.average_of_total_staff_and_worker_records),
        ("1-000008", 500,  475,  487.5, ASCWDSFilledPostsSource.average_of_total_staff_and_worker_records),
        ("1-000009", 10,   10,   8.0,   "already populated"),
    ] # fmt: skip

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
    replace_zero_beds_with_null_rows = [
        ("1-000000001", None),
        ("1-000000002", 0),
        ("1-000000003", 1),
    ]

    expected_replace_zero_beds_with_null_rows = [
        ("1-000000001", None),
        ("1-000000002", None),
        ("1-000000003", 1),
    ]

    populate_missing_care_home_number_of_beds_rows = [
        ("1-000000001", date(2023, 1, 1), "Y", None),
        ("1-000000002", date(2023, 1, 1), "N", None),
        ("1-000000003", date(2023, 1, 1), "Y", 1),
        ("1-000000003", date(2023, 2, 1), "Y", None),
        ("1-000000003", date(2023, 3, 1), "Y", 1),
        ("1-000000004", date(2023, 1, 1), "Y", 1),
        ("1-000000004", date(2023, 2, 1), "Y", 3),
    ]

    expected_populate_missing_care_home_number_of_beds_rows = [
        ("1-000000001", date(2023, 1, 1), "Y", None),
        ("1-000000002", date(2023, 1, 1), "N", None),
        ("1-000000003", date(2023, 1, 1), "Y", 1),
        ("1-000000003", date(2023, 2, 1), "Y", 1),
        ("1-000000003", date(2023, 3, 1), "Y", 1),
        ("1-000000004", date(2023, 1, 1), "Y", 1),
        ("1-000000004", date(2023, 2, 1), "Y", 3),
    ]

    filter_to_care_homes_with_known_beds_rows = [
        ("1-000000001", "Y", None),
        ("1-000000002", "N", None),
        ("1-000000003", "Y", 1),
        ("1-000000004", "N", 1),
    ]

    expected_filter_to_care_homes_with_known_beds_rows = [
        ("1-000000003", "Y", 1),
    ]

    average_beds_per_location_rows = [
        ("1-000000001", 1),
        ("1-000000002", 2),
        ("1-000000002", 3),
        ("1-000000003", 2),
        ("1-000000003", 3),
        ("1-000000003", 4),
    ]

    expected_average_beds_per_location_rows = [
        ("1-000000001", 1),
        ("1-000000002", 2),
        ("1-000000003", 3),
    ]

    replace_null_beds_with_average_rows = [
        ("1-000000001", None, None),
        ("1-000000002", None, 1),
        ("1-000000003", 2, 2),
    ]

    expected_replace_null_beds_with_average_rows = [
        ("1-000000001", None),
        ("1-000000002", 1),
        ("1-000000003", 2),
    ]

    calculate_time_registered_same_day_rows = [
        ("1-0001", date(2025, 1, 1), date(2025, 1, 1)),
    ] # fmt: skip

    expected_calculate_time_registered_same_day_rows = [
        ("1-0001", date(2025, 1, 1), date(2025, 1, 1), 1),
    ] # fmt: skip

    calculate_time_registered_exact_months_apart_rows = [
        ("1-0001", date(2024, 2, 1), date(2024, 1, 1)),
        ("1-0002", date(2020, 1, 1), date(2019, 1, 1)),
    ] # fmt: skip

    expected_calculate_time_registered_exact_months_apart_rows = [
        ("1-0001", date(2024, 2, 1), date(2024, 1, 1), 2),
        ("1-0002", date(2020, 1, 1), date(2019, 1, 1), 13),
    ] # fmt: skip

    calculate_time_registered_one_day_less_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 1), date(2024, 12, 2)),
        ("1-0002", date(2025, 6, 8), date(2025, 1, 9)),
    ] # fmt: skip

    expected_calculate_time_registered_one_day_less_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 1), date(2024, 12, 2), 1),
        ("1-0002", date(2025, 6, 8), date(2025, 1, 9), 5),
    ] # fmt: skip

    calculate_time_registered_one_day_more_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 2), date(2024, 12, 1)),
        ("1-0002", date(2025, 6, 1), date(2025, 1, 31)),
    ] # fmt: skip

    expected_calculate_time_registered_one_day_more_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 2), date(2024, 12, 1), 2),
        ("1-0002", date(2025, 6, 1), date(2025, 1, 31), 5),
    ] # fmt: skip

    calculate_time_since_dormant_rows = [
        ("1-001", date(2025, 1, 1), None),
        ("1-001", date(2025, 2, 1), Dormancy.not_dormant),
        ("1-001", date(2025, 3, 1), Dormancy.dormant),
        ("1-001", date(2025, 4, 1), Dormancy.dormant),
        ("1-001", date(2025, 5, 1), Dormancy.not_dormant),
        ("1-001", date(2025, 6, 1), Dormancy.dormant),
        ("1-001", date(2025, 7, 1), Dormancy.not_dormant),
        ("1-001", date(2025, 8, 1), Dormancy.not_dormant),
        ("1-001", date(2025, 9, 1), None),
        ("1-002", date(2025, 10, 1), Dormancy.not_dormant),
    ] # fmt: skip

    expected_calculate_time_since_dormant_rows = [
        ("1-001", date(2025, 1, 1), None, None),
        ("1-001", date(2025, 2, 1), Dormancy.not_dormant, None),
        ("1-001", date(2025, 3, 1), Dormancy.dormant, 1),
        ("1-001", date(2025, 4, 1), Dormancy.dormant, 1),
        ("1-001", date(2025, 5, 1), Dormancy.not_dormant, 2),
        ("1-001", date(2025, 6, 1), Dormancy.dormant, 1),
        ("1-001", date(2025, 7, 1), Dormancy.not_dormant, 2),
        ("1-001", date(2025, 8, 1), Dormancy.not_dormant, 3),
        ("1-001", date(2025, 9, 1), None, 4),
        ("1-002", date(2025, 10, 1), Dormancy.not_dormant, None),
    ] # fmt: skip

    remove_cqc_dual_registrations_when_carehome_and_asc_data_populated_rows = [
        ("loc 1", date(2024, 1, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2018, 1, 1)),
        ("loc 2", date(2024, 1, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2022, 1, 1)),
    ] # fmt: skip

    expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_populated_rows = [
        ("loc 1", date(2024, 1, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2018, 1, 1)),
    ] # fmt: skip

    remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_earlier_reg_date_rows = [
        ("loc 1", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, None, None, date(2018, 1, 1)),
        ("loc 2", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2022, 1, 1)),
    ] # fmt: skip

    expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_earlier_reg_date_rows = [
        ("loc 1", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2018, 1, 1)),
    ] # fmt: skip

    remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_later_reg_date_rows = [
        ("loc 1", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2018, 1, 1)),
        ("loc 2", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, None, None, date(2022, 1, 1)),
    ] # fmt: skip

    expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_later_reg_date_rows = [
        ("loc 1", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2018, 1, 1)),
    ] # fmt: skip

    remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_all_reg_dates_rows = [
        ("loc 1", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, None, None, date(2018, 1, 1)),
        ("loc 2", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, None, None, date(2022, 1, 1)),
    ] # fmt: skip

    expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_all_reg_dates_rows = [
        ("loc 1", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, None, None, date(2018, 1, 1)),
    ] # fmt: skip

    remove_cqc_dual_registrations_when_carehome_and_asc_data_different_on_all_reg_dates_rows = [
        ("loc 1", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2018, 1, 1)),
        ("loc 2", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, 11, 11, date(2022, 1, 1)),
    ] # fmt: skip

    expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_different_on_all_reg_dates_rows = [
        ("loc 1", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2018, 1, 1)),
    ] # fmt: skip

    remove_cqc_dual_registrations_when_carehome_and_registration_dates_the_same_rows = [
        ("loc 1", date(2024, 1, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2022, 1, 1)),
        ("loc 2", date(2024, 1, 1), "care home", "AB1 2CD", CareHome.care_home, None, None, date(2022, 1, 1)),
    ] # fmt: skip

    expected_remove_cqc_dual_registrations_when_carehome_and_registration_dates_the_same_rows = [
        ("loc 1", date(2024, 1, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2022, 1, 1)),
    ] # fmt: skip

    remove_cqc_dual_registrations_when_locations_not_sorted_numerically = [
        ("loc 3", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2018, 1, 1)),
        ("loc 1", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2018, 1, 1)),
        ("loc 2", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, 11, 11, date(2022, 1, 1)),
    ] # fmt: skip

    expected_remove_cqc_dual_registrations_when_locations_not_sorted_numerically = [
        ("loc 1", date(2024, 2, 1), "care home", "AB1 2CD", CareHome.care_home, 10, 10, date(2018, 1, 1)),
    ] # fmt: skip

    remove_cqc_dual_registrations_when_non_res_rows = [
        ("loc 1", date(2024, 1, 1), "not care home", "AB1 2CD", CareHome.not_care_home, None, None, date(2022, 1, 1)),
        ("loc 2", date(2024, 1, 1), "not care home", "AB1 2CD", CareHome.not_care_home, 10, 10, date(2022, 1, 1)),
    ] # fmt: skip

    expected_remove_cqc_dual_registrations_when_non_res_rows = (
        remove_cqc_dual_registrations_when_non_res_rows
    )

    repeated_value_rows = [
        ("1", "1-0001", 1, date(2023, 2, 1)),
        ("1", "1-0001", 2, date(2023, 3, 1)),
        ("1", "1-0001", 2, date(2023, 4, 1)),
        ("1", "1-0001", 3, date(2023, 8, 1)),
        ("2", "1-0002", 3, date(2023, 2, 1)),
        ("2", "1-0002", 9, date(2023, 4, 1)),
        ("2", "1-0002", 3, date(2024, 1, 1)),
        ("2", "1-0002", 3, date(2024, 2, 1)),
    ]

    expected_without_repeated_values_rows = [
        ("1", "1-0001", 1, date(2023, 2, 1), 1),
        ("1", "1-0001", 2, date(2023, 3, 1), 2),
        ("1", "1-0001", 2, date(2023, 4, 1), None),
        ("1", "1-0001", 3, date(2023, 8, 1), 3),
        ("2", "1-0002", 3, date(2023, 2, 1), 3),
        ("2", "1-0002", 9, date(2023, 4, 1), 9),
        ("2", "1-0002", 3, date(2024, 1, 1), 3),
        ("2", "1-0002", 3, date(2024, 2, 1), None),
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

    merged_rows_for_cleaning_job = [
        ("1-1000001", "20220201", date(2020, 2, 1), "South East", "Surrey", "Rural", "Y", 0, 5, 82, None, "Care home without nursing", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000001", "20220101", date(2022, 1, 1), "South East", "Surrey", "Rural", "Y", 5, 5, None, 67, "Care home without nursing", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000002", "20220101", date(2022, 1, 1), "South East", "Surrey", "Rural", "N", 0, 17, None, None, "non-residential", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000002", "20220201", date(2022, 2, 1), "South East", "Surrey", "Rural", "N", 0, 34, None, None, "non-residential", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000003", "20220301", date(2022, 3, 1), "North West", "Bolton", "Urban", "N", 0, 34, None, None, "non-residential", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000003", "20220308", date(2022, 3, 8), "North West", "Bolton", "Rural", "N", 0, 15, None, None, "non-residential", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
        ("1-1000004", "20220308", date(2022, 3, 8), "South West", "Dorset", "Urban", "Y", 9, 0, 25, 25, "Care home with nursing", "name", "postcode", date(2022, 1, 1), "2020", "01", "01"),
    ] # fmt: skip


@dataclass
class ImputeIndCqcAscwdsAndPirData:
    test_rolling_average_period = "3d"

    expected_rolling_average_rows = [
        ("1-001", date(2023, 1, 1), 1.1, 1.1),
        ("1-001", date(2023, 1, 2), 1.2, 1.15),
        ("1-001", date(2023, 1, 3), 1.3, 1.2),
        ("1-001", date(2023, 1, 4), 1.4, 1.3),
        ("1-001", date(2023, 1, 5), 1.4, 1.35),
        ("1-001", date(2023, 1, 5), 1.3, 1.35),
        ("1-002", date(2023, 1, 1), 10.0, 10.0),
        ("1-002", date(2023, 1, 3), 20.0, 15.0),
        ("1-002", date(2023, 1, 5), 30.0, 25.0),
    ]


@dataclass
class ArchiveFilledPostsEstimates:
    estimate_filled_posts_rows = [("loc 1", date(2024, 1, 1))]

    select_import_dates_to_archive_rows = [
        ("loc 1", date(2023, 3, 1)),
        ("loc 1", date(2023, 4, 1)),
        ("loc 1", date(2024, 3, 1)),
        ("loc 1", date(2024, 4, 1)),
        ("loc 1", date(2024, 5, 1)),
        ("loc 1", date(2024, 6, 1)),
        ("loc 1", date(2024, 6, 8)),
    ]
    expected_select_import_dates_to_archive_rows = [
        ("loc 1", date(2023, 4, 1)),
        ("loc 1", date(2024, 4, 1)),
        ("loc 1", date(2024, 5, 1)),
        ("loc 1", date(2024, 6, 1)),
        ("loc 1", date(2024, 6, 8)),
    ]

    add_latest_annual_estimate_date_rows = [
        ("loc 1", date(2024, 11, 1)),
        ("loc 1", date(2024, 12, 1)),
        ("loc 1", date(2025, 1, 1)),
        ("loc 1", date(2025, 2, 1)),
    ]
    expected_add_latest_annual_estimate_date_rows = [
        ("loc 1", date(2024, 11, 1), date(2024, 4, 1)),
        ("loc 1", date(2024, 12, 1), date(2024, 4, 1)),
        ("loc 1", date(2025, 1, 1), date(2024, 4, 1)),
        ("loc 1", date(2025, 2, 1), date(2024, 4, 1)),
    ]

    create_archive_date_partition_columns_rows = [
        ("loc 1", date(2025, 1, 1)),
    ]
    expected_partitions_when_date_has_single_digits_lf_rows = [
        ("loc 1", date(2025, 1, 1), "01", "01", "2026", "2026-01-01 00:00"),
    ]
    expected_partitions_when_date_has_double_digits_lf_rows = [
        ("loc 1", date(2025, 1, 1), "31", "12", "2025", "2025-12-31 00:00"),
    ]


@dataclass
class CleanFilteringUtilsData:
    aggregate_values_to_provider_level_rows = [
        ("1-001", "1-0001", 1, date(2025, 1, 1)),
        ("1-002", "1-0001", 1, date(2025, 1, 1)),
        ("1-003", "1-0002", 1, date(2025, 1, 1)),
        ("1-004", "1-0002", None, date(2025, 1, 1)),
        ("1-005", "1-0003", None, date(2025, 1, 1)),
        ("1-006", "1-0003", None, date(2025, 1, 1)),
        ("1-001", "1-0001", 2, date(2025, 2, 1)),
        ("1-002", "1-0001", 2, date(2025, 2, 1)),
    ]
    expected_aggregate_values_to_provider_level_rows = [
        ("1-001", "1-0001", 1, date(2025, 1, 1), 2),
        ("1-002", "1-0001", 1, date(2025, 1, 1), 2),
        ("1-003", "1-0002", 1, date(2025, 1, 1), 1),
        ("1-004", "1-0002", None, date(2025, 1, 1), 1),
        ("1-005", "1-0003", None, date(2025, 1, 1), None),
        ("1-006", "1-0003", None, date(2025, 1, 1), None),
        ("1-001", "1-0001", 2, date(2025, 2, 1), 4),
        ("1-002", "1-0001", 2, date(2025, 2, 1), 4),
    ]


@dataclass
class CleanUtilsData:
    locations_with_repeated_value_rows = [
        ("1-0001", 1, date(2023, 2, 1)),
        ("1-0001", 2, date(2023, 3, 1)),
        ("1-0001", 2, date(2023, 4, 1)),
        ("1-0001", 3, date(2023, 8, 1)),
        ("1-0002", 3, date(2023, 2, 1)),
        ("1-0002", 9, date(2023, 4, 1)),
        ("1-0002", 3, date(2024, 1, 1)),
        ("1-0002", 3, date(2024, 2, 1)),
    ]
    expected_locations_without_repeated_value_rows = [
        ("1-0001", 1, date(2023, 2, 1), 1),
        ("1-0001", 2, date(2023, 3, 1), 2),
        ("1-0001", 1, date(2023, 4, 1), 1),
        ("1-0001", 3, date(2023, 8, 1), 3),
        ("1-0002", 3, date(2023, 2, 1), 3),
        ("1-0002", 9, date(2023, 4, 1), 9),
        ("1-0002", 3, date(2024, 1, 1), 3),
        ("1-0002", 9, date(2024, 2, 1), 9),
    ]
    location_without_repeated_value_rows = [
        ("1-0001", 1, date(2023, 2, 1)),
        ("1-0001", 2, date(2023, 3, 1)),
        ("1-0001", 1, date(2023, 4, 1)),
        ("1-0001", 3, date(2023, 8, 1)),
        ("1-0002", 3, date(2023, 2, 1)),
        ("1-0002", 9, date(2023, 4, 1)),
        ("1-0002", 3, date(2024, 1, 1)),
        ("1-0002", 9, date(2024, 2, 1)),
    ]
    expected_locations_without_repeated_values_when_input_has_repeated_values_rows = [
        ("1-0001", 1, date(2023, 2, 1), 1),
        ("1-0001", 2, date(2023, 3, 1), 2),
        ("1-0001", 2, date(2023, 4, 1), None),
        ("1-0001", 3, date(2023, 8, 1), 3),
        ("1-0002", 3, date(2023, 2, 1), 3),
        ("1-0002", 9, date(2023, 4, 1), 9),
        ("1-0002", 3, date(2024, 1, 1), 3),
        ("1-0002", 3, date(2024, 2, 1), None),
    ]
    providers_with_repeated_value_rows = [
        ("1-0001", 1, date(2023, 2, 1)),
        ("1-0001", 2, date(2023, 3, 1)),
        ("1-0001", 2, date(2023, 4, 1)),
        ("1-0001", 3, date(2023, 8, 1)),
        ("1-0002", 3, date(2023, 2, 1)),
        ("1-0002", 9, date(2023, 4, 1)),
        ("1-0002", 3, date(2024, 1, 1)),
        ("1-0002", 3, date(2024, 2, 1)),
    ]
    expected_providers_without_repeated_value_rows = [
        ("1-0001", 1, date(2023, 2, 1), 1),
        ("1-0001", 2, date(2023, 3, 1), 2),
        ("1-0001", 1, date(2023, 4, 1), 1),
        ("1-0001", 3, date(2023, 8, 1), 3),
        ("1-0002", 3, date(2023, 2, 1), 3),
        ("1-0002", 9, date(2023, 4, 1), 9),
        ("1-0002", 3, date(2024, 1, 1), 3),
        ("1-0002", 9, date(2024, 2, 1), 9),
    ]
    providers_without_repeated_value_rows = [
        ("1-0001", 1, date(2023, 2, 1)),
        ("1-0001", 2, date(2023, 3, 1)),
        ("1-0001", 1, date(2023, 4, 1)),
        ("1-0001", 3, date(2023, 8, 1)),
        ("1-0002", 3, date(2023, 2, 1)),
        ("1-0002", 9, date(2023, 4, 1)),
        ("1-0002", 3, date(2024, 1, 1)),
        ("1-0002", 9, date(2024, 2, 1)),
    ]
    expected_providers_without_repeated_values_when_input_has_repeated_values_rows = [
        ("1-0001", 1, date(2023, 2, 1), 1),
        ("1-0001", 2, date(2023, 3, 1), 2),
        ("1-0001", 2, date(2023, 4, 1), None),
        ("1-0001", 3, date(2023, 8, 1), 3),
        ("1-0002", 3, date(2023, 2, 1), 3),
        ("1-0002", 9, date(2023, 4, 1), 9),
        ("1-0002", 3, date(2024, 1, 1), 3),
        ("1-0002", 3, date(2024, 2, 1), None),
    ]


@dataclass
class ForwardFillLatestKnownValue:
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
        ("loc-1", date(2025, 1, 4), None, date(2025, 1, 1), 100, 2),
    ]

    expected_forward_fill_within_days_rows = [
        ("loc-1", date(2025, 1, 1), 100, date(2025, 1, 1), 100, 2),
        ("loc-1", date(2025, 1, 2), 100, date(2025, 1, 1), 100, 2),
        ("loc-1", date(2025, 1, 3), 100, date(2025, 1, 1), 100, 2),
        ("loc-1", date(2025, 1, 4), None, date(2025, 1, 1), 100, 2),
    ]

    forward_fill_beyond_days_rows = [
        ("loc-1", date(2025, 1, 1), 50, date(2025, 1, 1), 50, 2),
        ("loc-1", date(2025, 1, 4), None, date(2025, 1, 1), 50, 2),
    ]

    expected_forward_fill_beyond_days_rows = [
        ("loc-1", date(2025, 1, 1), 50, date(2025, 1, 1), 50, 2),
        ("loc-1", date(2025, 1, 4), None, date(2025, 1, 1), 50, 2),
    ]

    forward_fill_before_last_known_rows = [
        ("loc-1", date(2025, 1, 1), None, date(2025, 1, 2), 20, 2),
        ("loc-1", date(2025, 1, 2), 20, date(2025, 1, 2), 20, 2),
        ("loc-1", date(2025, 1, 3), None, date(2025, 1, 2), 20, 2),
        ("loc-2", date(2025, 1, 1), None, date(2025, 1, 3), 50, 2),
        ("loc-2", date(2025, 1, 2), None, date(2025, 1, 3), 50, 2),
        ("loc-2", date(2025, 1, 3), 50, date(2025, 1, 3), 50, 2),
    ]

    expected_forward_fill_before_last_known_rows = [
        ("loc-1", date(2025, 1, 1), None, date(2025, 1, 2), 20, 2),
        ("loc-1", date(2025, 1, 2), 20, date(2025, 1, 2), 20, 2),
        ("loc-1", date(2025, 1, 3), 20, date(2025, 1, 2), 20, 2),
        ("loc-2", date(2025, 1, 1), None, date(2025, 1, 3), 50, 2),
        ("loc-2", date(2025, 1, 2), None, date(2025, 1, 3), 50, 2),
        ("loc-2", date(2025, 1, 3), 50, date(2025, 1, 3), 50, 2),
    ]

    forward_fill_latest_known_value_rows = [
        ("loc-1", date(2025, 1, 1), 10),
        ("loc-1", date(2025, 1, 2), None),
        ("loc-1", date(2025, 1, 4), 11),
        ("loc-1", date(2025, 1, 5), None),
        ("loc-2", date(2025, 1, 1), 20),
        ("loc-2", date(2025, 1, 2), 20),
        ("loc-2", date(2025, 1, 3), 22),
        ("loc-2", date(2025, 1, 5), None),
        ("loc-2", date(2025, 1, 6), None),
    ]

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

    null_grouped_providers_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", "nmdsid_1", 13.0, 13.0, 4, 3.25, AscwdsFilteringRule.populated, 1.0),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", None, None, None,  None, 4, None, AscwdsFilteringRule.missing_data, 1.0),
        ("loc 3", "prov 1", date(2024, 1, 1), "Y", None, None, None, None, 4, None, AscwdsFilteringRule.missing_data, 1.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", "nmdsid_1", 12.0, 12.0, 4, 3.0, AscwdsFilteringRule.populated, 1.0),
        ("loc 2", "prov 1", date(2024, 2, 1), "Y", None, None, None, None, 4, None, AscwdsFilteringRule.missing_data, 1.0),
    ] # fmt: skip

    input_grouped_provider_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", None, 4, 12.0),
        ("loc 2", "prov 2", date(2024, 1, 1), "Y", None, None, 5, None),
        ("loc 3", "prov 3", date(2024, 1, 1), "N", "estab 3", 10.0, None, 15.0),
    ] # fmt: skip
    expected_grouped_provider_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0, 11.0, 1, 1, 1, 4, 1, 11.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", None, 4, 12.0, 11.0, 1, 1, 0, 4, 1, 11.0),
        ("loc 2", "prov 2", date(2024, 1, 1), "Y", None, None, 5, None, None, 1, 0, 0, 5, 0, 0),
        ("loc 3", "prov 3", date(2024, 1, 1), "N", "estab 3", 10.0, None, 15.0, 15.0, 1, 1, 1, 0, 1, 15.0),
    ] # fmt: skip

    calculate_data_for_grouped_provider_identification_where_provider_has_multiple_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", 13.0, 4, 20.0),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", "estab 2", 14.0, 3, 15.0),
        ("loc 2", "prov 1", date(2024, 2, 1), "Y", None, None, 5, 25.0),
        ("loc 3", "prov 2", date(2024, 1, 1), "Y", None, None, 6, 10.0),
        ("loc 4", "prov 2", date(2024, 1, 1), "N", "estab 3", None, None, None),
        ("loc 5", "prov 3", date(2024, 1, 1), "N", None, None, None, None),
        ("loc 6", "prov 3", date(2024, 1, 1), "N", None, None, None, None),
    ] # fmt: skip
    expected_calculate_data_for_grouped_provider_identification_where_provider_has_multiple_location_rows = [
        ("loc 1", "prov 1", date(2024, 1, 1), "Y", "estab 1", 13.0, 4, 10.0, 15.0, 2, 2, 2, 7, 2, 35.0),
        ("loc 1", "prov 1", date(2024, 2, 1), "Y", "estab 1", 13.0, 4, 20.0, 15.0, 2, 1, 1, 9, 2, 35.0),
        ("loc 2", "prov 1", date(2024, 1, 1), "Y", "estab 2", 14.0, 3, 15.0, 20.0, 2, 2, 2, 7, 2, 35.0),
        ("loc 2", "prov 1", date(2024, 2, 1), "Y", None, None, 5, 25.0, 20.0, 2, 1, 1, 9, 2, 35.0),
        ("loc 3", "prov 2", date(2024, 1, 1), "Y", None, None, 6, 10.0, 10.0, 2, 1, 0, 6, 1, 10.0),
        ("loc 4", "prov 2", date(2024, 1, 1), "N", "estab 3", None, None, None, None, 2, 1, 0, 6, 1, 10.0),
        ("loc 5", "prov 3", date(2024, 1, 1), "N", None, None, None, None, None, 2, 0, 0, None, 0, None),
        ("loc 6", "prov 3", date(2024, 1, 1), "N", None, None, None, None, None, 2, 0, 0, None, 0, None),
    ] # fmt: skip

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

    null_care_home_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.care_home, 25.0, 25.0, 2, 2, 12.5, True, AscwdsFilteringRule.populated),
        ("1-002", CareHome.care_home, 60.0, 60.0, 2, 2, 30.0, True, AscwdsFilteringRule.populated),
    ] # fmt: skip
    expected_null_care_home_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.care_home, 25.0, None, 2, 2, None, True, AscwdsFilteringRule.care_home_location_was_grouped_provider),
        ("1-002", CareHome.care_home, 60.0, None, 2, 2, None, True, AscwdsFilteringRule.care_home_location_was_grouped_provider),
    ] # fmt: skip
    null_care_home_grouped_providers_where_location_does_not_meet_criteria_rows = [
        ("1-001", CareHome.not_care_home, 25.0, 25.0, None, 2, None, True, AscwdsFilteringRule.populated),  # non res location
        ("1-002", CareHome.care_home, 25.0, 25.0, 2, 3, 12.5, False, AscwdsFilteringRule.populated),  # not identified as potential grouped provider
        ("1-003", CareHome.care_home, 24.0, 24.0, 2, 2, 12.0, True, AscwdsFilteringRule.populated),  # below minimum size
        ("1-004", CareHome.care_home, 25.0, 25.0, 20, 22, 1.25, True, AscwdsFilteringRule.populated), # below location and provider threshold
    ] # fmt: skip

    null_non_res_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.not_care_home, True, 50.0, 50.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),
        ("1-002", CareHome.not_care_home, True, None, None, 15.0, 2, 25.0, None),
    ] # fmt: skip
    expected_null_non_res_grouped_providers_when_meets_criteria_rows = [
        ("1-001", CareHome.not_care_home, True, 50.0, None, 10.0, 2, 25.0, AscwdsFilteringRule.non_res_location_was_grouped_provider),
        ("1-002", CareHome.not_care_home, True, None, None, 15.0, 2, 25.0, None),
    ] # fmt: skip
    null_non_res_grouped_providers_when_does_not_meet_criteria_rows = [
        ("1-001", CareHome.care_home, True, 50.0, 50.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),  # care home location
        ("1-002", CareHome.not_care_home, False, 50.0, 50.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),  # not identified as potential grouped provider
        ("1-003", CareHome.not_care_home, True, 49.0, 49.0, 10.0, 2, 25.0, AscwdsFilteringRule.populated),  # below minimum size
        ("1-004", CareHome.not_care_home, True, 50.0, 50.0, None, 2, 25.0, AscwdsFilteringRule.populated),  # no PIR data for location
        ("1-005", CareHome.not_care_home, True, 50.0, 50.0, 40.0, 2, 45.0, AscwdsFilteringRule.populated),  # below location threshold and provider sum
        ("1-006", CareHome.not_care_home, True, 50.0, 50.0, 40.0, 1, 40.0, AscwdsFilteringRule.populated),  # below location threshold and provider count
        ("1-008", CareHome.not_care_home, True, 50.0, None, 10.0, 2, 25.0, AscwdsFilteringRule.contained_invalid_missing_data_code),  # already filtered
    ] # fmt: skip

    select_grouped_providers_rows = [
        ("1-001", "prov-1", date(2026, 1, 1), "nmdsid_1", True, 1.0), # Grouped provider but undesired import date.
        ("1-002", "prov-1", date(2026, 1, 1), "nmdsid_2", False, 1.0), # Not grouped provider and undesired import date.
        ("1-003", "prov-1", date(2026, 2, 1), "nmdsid_3", False, 1.0), # Desired date but not grouped provider.
        ("1-004", "prov-1", date(2026, 2, 1), "nmdsid_4", True, 1.0), # Keep
        ("1-005", "prov-2", date(2026, 2, 1), "nmdsid_5", True, 1.0), # Keep (different provider)
        ("1-006", "prov-1", date(2025, 1, 1), "nmdsid_6", True, 1.0), # Incorrect date with lower year and month than desired.
        ("1-008", "prov-1", date(2025, 3, 1), "nmdsid_7", True, 1.0), # Incorrect date with lower year and higher month than desired.
    ] # fmt: skip

    expected_select_grouped_providers_rows = [
        ("1-004", "prov-1", date(2026, 2, 1), "nmdsid_4", True, 1.0, "problem", date(2026, 2, 1)),
        ("1-005", "prov-2", date(2026, 2, 1), "nmdsid_5", True, 1.0, "problem", date(2026, 2, 1)),
    ] # fmt: skip

    # All rows have grouped_provider_status = "problem" and last_update_date = their import date.
    new_grouped_providers_rows = [
        ("1-001", "prov-1", date(2026, 2, 1), "nmds_1", True, 10.0, "problem", date(2026, 2, 1)),
        ("1-003", "prov-2", date(2026, 2, 1), "nmds_3", True, 30.0, "problem", date(2026, 2, 1)),
        ("1-004", "prov-3", date(2026, 2, 1), "nmds_4", True, 40.0, "problem", date(2026, 2, 1)),
    ]  # fmt: skip

    # historical_grouped_providers_rows: what was previously saved to the grouped providers dataset.
    historical_grouped_providers_rows = [
        ("1-001", "prov-1", date(2026, 1, 1), "nmds_1", True, 10.0, "problem", date(2026, 1, 1)), # Still active problem — should be retained as-is (oldest kept, last_update_date stays same).
        ("1-002", "prov-1", date(2026, 1, 1), "nmds_2", True, 20.0, "problem", date(2026, 1, 1)), # Dropped off — not in new snapshot, should be flipped to "fixed".
        ("1-003", "prov-2", date(2026, 1, 1), "nmds_3", True, 30.0, "fixed", date(2026, 1, 1)), # Re-appearing — was fixed, now back as "problem" in new snapshot; new row appended.
    ]  # fmt: skip

    # expected_update_grouped_providers_history_rows: full history after update.
    expected_update_grouped_providers_history_rows = [
        ("1-001", "prov-1", date(2026, 1, 1), "nmds_1", True, 10.0, "problem", date(2026, 1, 1)), # Retained — oldest "problem" record kept, last_update_date unchanged.
        ("1-002", "prov-1", date(2026, 1, 1), "nmds_2", True, 20.0, "fixed", date(2026, 2, 1)), # Flipped — was "problem", now "fixed" with last_update_date = new snapshot date.
        ("1-003", "prov-2", date(2026, 1, 1), "nmds_3", True, 30.0, "fixed", date(2026, 1, 1)), # Preserved — old "fixed" row kept as part of full history.
        ("1-003", "prov-2", date(2026, 2, 1), "nmds_3", True, 30.0, "problem", date(2026, 2, 1)), # Re-appeared — new "problem" row appended alongside the old "fixed" row.
        ("1-004", "prov-3", date(2026, 2, 1), "nmds_4", True, 40.0, "problem", date(2026, 2, 1)), # New — first time seen, added with "problem" status.
    ]  # fmt: skip


@dataclass
class CleanAscwdsFilledPostOutliersData:

    unfiltered_ind_cqc_rows = [
        ("01", "prov 1", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 30.0),
        ("02", "prov 1", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 35.0),
        ("03", "prov 1", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, None, 8.0),
    ] # fmt: skip


@dataclass
class WinsorizeCareHomeFilledPostsPerBedRatioOutliersData:

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
    ] # fmt: skip

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
    ] # fmt: skip

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

    select_data_not_in_subset_rows = [
        ("1-000000001", "data"),
        ("1-000000002", "data"),
        ("1-000000003", "data"),
    ]
    subset_rows = [
        ("1-000000002", "data"),
    ]

    expected_select_data_not_in_subset_rows = [
        ("1-000000001", "data"),
        ("1-000000003", "data"),
    ]

    calculate_average_filled_posts_rows = [
        ("1", 0.0, 1.1357),
        ("2", 0.0, 1.3579),
        ("3", 1.0, 1.123456789),
    ]

    expected_calculate_average_filled_posts_rows = [
        (0.0, 1.2468),
        (1.0, 1.123456789),
    ]

    base_filled_posts_rows = [
        ("1", 7, 0.0),
        ("2", 75, 1.0),
    ]

    join_filled_posts_rows = [
        (0.0, 1.11111),
        (1.0, 1.0101),
    ]

    expected_filled_posts_rows = [
        ("1", 7, 0.0, 1.11111, 7.77777),
        ("2", 75, 1.0, 1.0101, 75.7575),
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
        ("1", PrimaryServiceType.care_home_with_nursing, 0.54321, -3.45444, 6.933232),
        ("2", PrimaryServiceType.care_home_with_nursing, -3.2545, -3.45444, 6.933232),
        ("3", PrimaryServiceType.care_home_with_nursing, -4.2542, -3.45444, 6.933232),
        ("4", PrimaryServiceType.care_home_with_nursing, 2.41654, -3.45444, 6.933232),
        ("5", PrimaryServiceType.care_home_with_nursing, 25.0, -3.45444, 6.933232),
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
        ("01", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 6.0, 1.0, 1.0, None, 0.04, AscwdsFilteringRule.populated, 10.0),
        ("02", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 6.0, 2.0, 2.0, 2.0, 0.08, AscwdsFilteringRule.populated, 20.0),
    ] # fmt: skip

    combine_dataframes_non_care_home_rows = [
        ("03", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, None, None, 3.0, 3.0, 3.0, None, AscwdsFilteringRule.populated),
    ] # fmt: skip

    expected_combined_dataframes_rows = [
        ("01", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 6.0, 1.0, 1.0, None, 0.04, AscwdsFilteringRule.populated),
        ("02", date(2023, 1, 1), "Y", PrimaryServiceType.care_home_only, 25, 6.0, 2.0, 2.0, 2.0, 0.08, AscwdsFilteringRule.populated),
        ("03", date(2023, 1, 1), "N", PrimaryServiceType.non_residential, None, None, 3.0, 3.0, 3.0, None, AscwdsFilteringRule.populated),
    ] # fmt: skip


@dataclass
class ConvertPirPeopleToFilledPostsData:
    valid_rows = [
        (CareHome.care_home,     10.0,   10.0),  # invalid (care home)
        (CareHome.not_care_home, None,   10.0),  # invalid (people is null)
        (CareHome.not_care_home, 0.0,    10.0),  # invalid (people is zero)
        (CareHome.not_care_home, 10.0,   None),  # invalid (posts in null)
        (CareHome.not_care_home, 10.0,    0.0),  # invalid (posts is zero)
        (CareHome.not_care_home, 100.0,  10.0),  # invalid (ratio too low, outside abs_diff cutoff)
        (CareHome.not_care_home, 10.0,  100.0),  # invalid (ratio too high, outside abs_diff cutoff)
        (CareHome.not_care_home, 20.0,   35.0),  # valid (outside abs_diff cutoff but ratio ok)
        (CareHome.not_care_home, 10.0,    1.0),  # valid (ratio too low but inside abs_diff cutoff)
        (CareHome.not_care_home, 1.0,    10.0),  # valid (ratio too high but inside abs_diff cutoff)
        (CareHome.not_care_home, 10.0,   10.0),  # valid (ratio and abs_diff within limits)
    ] # fmt: skip
    expected_valid_rows = [
        (CareHome.not_care_home, 20.0,   35.0),
        (CareHome.not_care_home, 10.0,    1.0),
        (CareHome.not_care_home, 1.0,    10.0),
        (CareHome.not_care_home, 10.0,   10.0),
    ] # fmt: skip


@dataclass
class NullCtPostsToBedsOutliers:
    null_ct_posts_to_beds_outliers_rows = [
        ("1-001", 1, 1.00, 1, CTFilteringRule.populated),
        ("1-002", 1, None, 1, CTFilteringRule.populated),
        ("1-003", None, 1.00, None, CTFilteringRule.missing_data),
        ("1-004", None, None, None, CTFilteringRule.missing_data),
        ("1-005", 1, 0.65, 1, CTFilteringRule.populated),
        ("1-006", 1, 6.01, 1, CTFilteringRule.populated),
    ]
    expected_null_ct_posts_to_beds_outliers_rows = [
        ("1-001", 1, 1.00, 1, CTFilteringRule.populated),
        ("1-002", 1, None, 1, CTFilteringRule.populated),
        ("1-003", None, 1.00, None, CTFilteringRule.missing_data),
        ("1-004", None, None, None, CTFilteringRule.missing_data),
        ("1-005", 1, 0.65, None, CTFilteringRule.beds_ratio_outlier),
        ("1-006", 1, 6.01, None, CTFilteringRule.beds_ratio_outlier),
    ]


@dataclass
class NullValuesExceedingRepetitionLimitTestCase:
    id: str
    input_data: list[tuple]
    expected_data: list[tuple]


null_values_exceeding_repetition_limit_test_cases = [
    NullValuesExceedingRepetitionLimitTestCase(
        id="values_repeat_after_a_missing_value",
        input_data=[
            ("1-001", date(2025, 1, 1), 1, CTFilteringRule.populated),
            ("1-001", date(2025, 2, 1), 2, CTFilteringRule.populated),
            ("1-001", date(2025, 3, 1), 2, CTFilteringRule.populated),
            ("1-001", date(2025, 4, 1), None, CTFilteringRule.missing_data),
            ("1-001", date(2025, 11, 7), 2, CTFilteringRule.populated),
            ("1-001", date(2025, 12, 1), 3, CTFilteringRule.populated),
        ],
        expected_data=[
            ("1-001", date(2025, 1, 1), 1, CTFilteringRule.populated),
            ("1-001", date(2025, 2, 1), 2, CTFilteringRule.populated),
            ("1-001", date(2025, 3, 1), 2, CTFilteringRule.populated),
            ("1-001", date(2025, 4, 1), None, CTFilteringRule.missing_data),
            ("1-001", date(2025, 11, 7), None, CTFilteringRule.location_repeats_total_posts),
            ("1-001", date(2025, 12, 1), 3, CTFilteringRule.populated),
        ],
    ),
    NullValuesExceedingRepetitionLimitTestCase(
        id="values_repeat_but_not_consecutively",
        input_data=[
            ("1-001", date(2025, 1, 1), 1, CTFilteringRule.populated),
            ("1-001", date(2025, 2, 1), 2, CTFilteringRule.populated),
            ("1-001", date(2025, 3, 1), 2, CTFilteringRule.populated),
            ("1-001", date(2025, 4, 1), 500, CTFilteringRule.populated),
            ("1-001", date(2025, 5, 1), 500, CTFilteringRule.populated),
            ("1-001", date(2025, 12, 1), 2, CTFilteringRule.populated),
        ],
        expected_data=[
            ("1-001", date(2025, 1, 1), 1, CTFilteringRule.populated),
            ("1-001", date(2025, 2, 1), 2, CTFilteringRule.populated),
            ("1-001", date(2025, 3, 1), 2, CTFilteringRule.populated),
            ("1-001", date(2025, 4, 1), 500, CTFilteringRule.populated),
            ("1-001", date(2025, 5, 1), 500, CTFilteringRule.populated),
            ("1-001", date(2025, 12, 1), 2, CTFilteringRule.populated),
        ],
    ),
    NullValuesExceedingRepetitionLimitTestCase(
        id="micro_location_repeats_after_too_long",
        input_data=[
            ("1-001", date(2025, 1, 1), 1, CTFilteringRule.populated),
            ("1-001", date(2026, 10, 1), 1, CTFilteringRule.populated),
        ],
        expected_data=[
            ("1-001", date(2025, 1, 1), 1, CTFilteringRule.populated),
            ("1-001", date(2026, 10, 1), None, CTFilteringRule.location_repeats_total_posts),
        ],
    ),
    NullValuesExceedingRepetitionLimitTestCase(
        id="small_location_repeats_after_too_long",
        input_data=[
            ("1-001", date(2025, 1, 1), 10, CTFilteringRule.populated),
            ("1-001", date(2026, 10, 1), 10, CTFilteringRule.populated),
        ],
        expected_data=[
            ("1-001", date(2025, 1, 1), 10, CTFilteringRule.populated),
            ("1-001", date(2026, 10, 1), None, CTFilteringRule.location_repeats_total_posts),
        ],
    ),
    NullValuesExceedingRepetitionLimitTestCase(
        id="medium_location_repeats_after_too_long",
        input_data=[
            ("1-001", date(2025, 1, 1), 50, CTFilteringRule.populated),
            ("1-001", date(2026, 10, 1), 50, CTFilteringRule.populated),
        ],
        expected_data=[
            ("1-001", date(2025, 1, 1), 50, CTFilteringRule.populated),
            ("1-001", date(2026, 10, 1), None, CTFilteringRule.location_repeats_total_posts),
        ],
    ),
    NullValuesExceedingRepetitionLimitTestCase(
        id="large_location_repeats_after_too_long",
        input_data=[
            ("1-001", date(2025, 1, 1), 250, CTFilteringRule.populated),
            ("1-001", date(2026, 10, 1), 250, CTFilteringRule.populated),
        ],
        expected_data=[
            ("1-001", date(2025, 1, 1), 250, CTFilteringRule.populated),
            ("1-001", date(2026, 10, 1), None, CTFilteringRule.location_repeats_total_posts),
        ],
    ),
] # fmt: skip


@dataclass
class NullLongitudinalOutliersData:
    null_longitudinal_outliers_input_rows = [
        ("1-001", 5, CTFilteringRule.populated),
        ("1-001", 10, CTFilteringRule.populated),
        ("1-001", 15, CTFilteringRule.populated),
        ("1-001", 80, CTFilteringRule.populated),
        ("1-002", 95, CTFilteringRule.populated),
        ("1-002", 20, CTFilteringRule.populated),
        ("1-002", 90, CTFilteringRule.populated),
        ("1-003", 40, CTFilteringRule.populated),
        ("1-003", 45, CTFilteringRule.populated),
        ("1-003", 50, CTFilteringRule.populated),
        ("1-004", 5, CTFilteringRule.populated),
        ("1-004", 10, CTFilteringRule.populated),
        ("1-004", 15, CTFilteringRule.populated),
        ("1-004", 80, CTFilteringRule.populated),
        ("1-004", 94, CTFilteringRule.populated),
        ("1-004", 20, CTFilteringRule.populated),
        ("1-004", 90, CTFilteringRule.populated),
        ("1-004", 40, CTFilteringRule.populated),
        ("1-004", 45, CTFilteringRule.populated),
        ("1-004", 50, CTFilteringRule.populated),
    ]

    expected_null_longitudinal_outliers_remove_value_only_rows = [
        ("1-001", 5, CTFilteringRule.populated),
        ("1-001", 10, CTFilteringRule.populated),
        ("1-001", 15, CTFilteringRule.populated),
        ("1-001", None, CTFilteringRule.longitudinal_outliers),
        ("1-002", 95, CTFilteringRule.populated),
        ("1-002", None, CTFilteringRule.longitudinal_outliers),
        ("1-002", 90, CTFilteringRule.populated),
        ("1-003", 40, CTFilteringRule.populated),
        ("1-003", 45, CTFilteringRule.populated),
        ("1-003", 50, CTFilteringRule.populated),
        ("1-004", 5, CTFilteringRule.populated),
        ("1-004", 10, CTFilteringRule.populated),
        ("1-004", 15, CTFilteringRule.populated),
        ("1-004", 80, CTFilteringRule.populated),
        ("1-004", 94, CTFilteringRule.populated),
        ("1-004", 20, CTFilteringRule.populated),
        ("1-004", 90, CTFilteringRule.populated),
        ("1-004", 40, CTFilteringRule.populated),
        ("1-004", 45, CTFilteringRule.populated),
        ("1-004", 50, CTFilteringRule.populated),
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

    expected_enrich_model_ind_cqc_care_home_rows = [
        ("1-001", date(2025, 1, 1), CareHome.not_care_home, None, None, None),  # no prediction expected
        ("1-002", date(2025, 1, 1), CareHome.not_care_home, None, None, None),  # no prediction expected
        ("1-003", date(2025, 1, 1), CareHome.care_home, 2, -1.0, "v1_r1"),  # prediction (converted to posts) joined in (maintains negative)
        ("1-004", date(2025, 1, 1), CareHome.care_home, 2, 5.0, "v1_r1"),  # prediction (converted to posts) joined in
    ] # fmt: skip

    enrich_model_predictions_non_res_rows = [
        ("1-001", date(2025, 1, 1), 2, -5.0, "v1_r1"),
        ("1-002", date(2025, 1, 1), 2, 2.5, "v1_r1"),
    ]

    expected_enrich_model_ind_cqc_non_res_rows = [
        ("1-001", date(2025, 1, 1), CareHome.not_care_home, None, -5.0, "v1_r1"),  # prediction joined in (maintains negative)
        ("1-002", date(2025, 1, 1), CareHome.not_care_home, None, 2.5, "v1_r1"),  # prediction joined in
        ("1-003", date(2025, 1, 1), CareHome.care_home, 2, None, None),  # no prediction expected
        ("1-004", date(2025, 1, 1), CareHome.care_home, 2, None, None),  # no prediction expected
    ] # fmt: skip
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
class ModelNonResWithAndWithoutDormancyCombinedRows:
    estimated_posts_rows = [
        ("1-001", date(2021, 1, 1), "N", "Y", 1, 1.0, None, 2.702127659574468),
        ("1-001", date(2022, 2, 1), "N", "Y", 2, 3.0, None, 4.23404255319149),
        ("1-001", date(2023, 3, 1), "N", "Y", 3, 4.0, 5.0, 5.0),
        ("1-001", date(2024, 4, 1), "N", "Y", 4, 5.0, 5.5, 5.5),
        ("1-001", date(2025, 5, 1), "N", "Y", 5, 6.0, 6.0, 6.0),
        ("1-001", date(2025, 6, 1), "N", "Y", 6, 7.0, 6.5, 6.5),
        ("1-002", date(2021, 1, 1), "N", "Y", 3, 8.0, None, 4.0),
        ("1-002", date(2022, 2, 1), "N", "Y", 4, 8.0, None, 4.0),
        ("1-002", date(2023, 3, 1), "N", "Y", 5, 8.0, 4.0, 4.0),
        ("1-002", date(2024, 4, 1), "N", "Y", 6, 8.0, 4.5, 4.5),
        ("1-002", date(2025, 5, 1), "N", "Y", 7, 8.0, 5.0, 5.0),
        ("1-002", date(2025, 6, 1), "N", "Y", 8, 8.0, 5.5, 5.5),
        ("1-003", date(2021, 1, 1), "N", "N", 1, 2.0, None, 3.25),
        ("1-003", date(2022, 2, 1), "N", "N", 2, 2.0, None, 3.25),
        ("1-003", date(2021, 3, 1), "N", "N", 3, 4.0, None, 5.625),
        ("1-003", date(2022, 4, 1), "N", "N", 4, 4.0, None, 5.625),
        ("1-003", date(2023, 5, 1), "N", "N", 5, 6.0, 8.0, 8.0),
        ("1-003", date(2024, 6, 1), "N", "N", 6, 6.0, 9.0, 9.0),
        ("1-004", date(2024, 4, 1), "Y", "Y", 1, None, None, None),
        ("1-005", date(2024, 5, 1), "N", "Y", 1, 4.0, 2.0, 2.0),
        ("1-005", date(2024, 6, 1), "N", "Y", 2, 5.0, 2.5, 2.5),
        ("1-006", date(2024, 5, 1), "N", "N", 1, 3.0, 2.5, 2.5),
        ("1-006", date(2024, 6, 1), "N", "N", 2, 3.0, 3.0, 3.0),
        ("1-006", date(2024, 7, 1), "N", "N", 3, 3.0, 3.0, 3.0),
        ("1-006", date(2024, 8, 1), "N", "N", 4, 3.0, 3.0, 3.0),
    ]

    expected_group_time_registered_to_six_month_bands_rows = [
        ("1-001", 6, 0),
        ("1-002", 7, 1),
        ("1-003", 200, 20),
    ]

    calculate_and_apply_model_ratios_rows = [
        # CASE 1: "Y" + band 2 → BOTH populated
        ("Y", 2, 4.0, 2.0, 2.25, 4.5, 0.5, 2.0),
        ("Y", 2, 5.0, 2.5, 2.25, 4.5, 0.5, 2.5),

        # CASE 2: "N" + band 2 → BOTH populated (different values)
        ("N", 2, 6.0, 8.0, 8.5, 6.0, 1.4166666666666667, 8.5),
        ("N", 2, 6.0, 9.0, 8.5, 6.0, 1.4166666666666667, 8.5),

        # CASE 3: "Y" + band 3 → with_dormancy NULL → NO valid rows
        ("Y", 3, 3.0, None, None, None, None, None),
        ("Y", 3, 4.0, None, None, None, None, None),

        # CASE 4: "Y" + band 3 → without_dormancy NULL → NO valid rows
        ("Y", 3, None, 4.0, None, None, None, None),
        ("Y", 3, None, 5.0, None, None, None, None),

        # CASE 5: "Y" + band 3 → BOTH NULL → NO valid rows
        ("Y", 3, None, None, None, None, None, None),
        ("Y", 3, None, None, None, None, None, None),
    ]  # fmt: skip

    calculate_and_apply_residuals_rows = [
        ("1-001", date(2025, 2, 1), 20.0, 15.0, 5.0, 20.0),  # dates match, both models not null, residual calculated
        ("1-002", date(2025, 1, 1), None, 16.0, -5.0, 11.0),  # "1-002" - with_dormancy is null, residual added from date(2025, 2, 1) but not applied
        ("1-002", date(2025, 2, 1), 10.0, 15.0, -5.0, 10.0),  # "1-002" - first period with both models present, take the residual
        ("1-002", date(2025, 3, 1), 11.0, 14.0, -5.0, 9.0),  # "1-002" - residual added from date(2025, 2, 1)
        ("1-002", date(2025, 4, 1), 12.0, None, -5.0, None),  # "1-002" - without_dormancy is null, residual added from date(2025, 2, 1) but not applied
        ("1-003", date(2025, 2, 1), 30.0, None, None, None),  # doesn't pass filter, no residual, keep original model value
        ("1-004", date(2025, 2, 1), None, 15.0, None, 15.0),  # doesn't pass filter, no residual, keep original model value
        ("1-005", date(2025, 2, 1), None, None, None, None),  # doesn't pass filter, no residual, keep original model value
    ] # fmt: skip


@dataclass
class EstimateNonResCapacityTrackerFilledPostsData:
    expected_estimate_non_res_capacity_tracker_filled_posts_rows = [
        ("1-001", date(2026, 1, 1), CareHome.not_care_home, 1.0, 2.0, 2.0, 2.0, IndCQC.ct_non_res_all_posts),
        ("1-002", date(2026, 1, 1), CareHome.not_care_home, 2.0, 4.0, 4.0, 4.0, IndCQC.ct_non_res_all_posts),
        ("1-003", date(2026, 1, 1), CareHome.not_care_home, 3.0, 6.0, 6.0, 6.0, IndCQC.ct_non_res_all_posts),
        ("1-004", date(2026, 1, 1), CareHome.not_care_home, None, 100.0, None, 100.0, IndCQC.estimate_filled_posts), # Value derived from filled posts since CT is null.
        ("1-005", date(2026, 1, 1), CareHome.not_care_home, 100.0, None, 200.0, 200.0, IndCQC.ct_non_res_all_posts), # Value derived from CT and adjusted.
        ("1-006", date(2026, 1, 1), CareHome.not_care_home, None, None, None, None, None), # Nulled because no CT value or estimate filled posts.
        ("1-007", date(2026, 1, 1), CareHome.care_home, 100.0, 100.0, None, None, None), # Nulled because care home.
        ("1-008", date(2026, 1, 1), CareHome.not_care_home, 0.49, 0.98, 0.98, 1.0, IndCQC.ct_non_res_all_posts), # ct_non_res_filled_post_estimate is 1.0 because of clip.
        ("1-009", date(2021, 4, 30), CareHome.not_care_home, 1.0, 2.0, 2.0, None, None), # Nulled because import date prior to 2021-5-1
    ] # fmt: skip


@dataclass
class JoinEstimatesCase:
    id: str
    estimates_data: list[tuple]
    ascwds_data: list[tuple]
    expected_data: list[tuple]


@dataclass
class TestJoinEstimatesToAscwds:
    join_estimates_test_cases = [
        JoinEstimatesCase(
            id="basic_match",
            estimates_data=[
                (1, "2024-01-01", "loc1"),
            ],
            ascwds_data=[
                ("2024-01-01", "loc1", "role_a", 10.0),
                ("2024-01-01", "loc1", "role_b", 20.0),
            ],
            expected_data=[
                (1, "role_a", 10.0),
                (1, "role_b", 20.0),
            ],
        ),
        JoinEstimatesCase(
            id="missing_role_returns_null",
            estimates_data=[
                (1, "2024-01-01", "loc1"),
            ],
            ascwds_data=[
                ("2024-01-01", "loc1", "role_a", 10.0),
            ],
            expected_data=[
                (1, "role_a", 10.0),
                (1, "role_b", None),
            ],
        ),
        JoinEstimatesCase(
            id="multiple_rows_expand_correctly",
            estimates_data=[
                (1, "2024-01-01", "loc1"),
                (2, "2024-01-01", "loc2"),
            ],
            ascwds_data=[
                ("2024-01-01", "loc1", "role_a", 5.0),
                ("2024-01-01", "loc2", "role_b", 7.0),
            ],
            expected_data=[
                (1, "role_a", 5.0),
                (1, "role_b", None),
                (2, "role_a", None),
                (2, "role_b", 7.0),
            ],
        ),
    ] # fmt: skip


@dataclass
class ReducedDataFilterCase:
    id: str
    today: date | None
    fy_start_month: int
    lookback_fy_years: int
    quarter_months: tuple[int, ...]
    input_data: list[date]
    expected: list[bool]


@dataclass
class TestReducedDataFilter:
    reduced_data_filter_test_cases = [
        ReducedDataFilterCase(
            id="test reduced_data_filter with default args",
            today=date(2024, 6, 15),
            fy_start_month=4,
            lookback_fy_years=2,
            quarter_months=(1, 4, 7, 10),
            input_data=[
                date(2021, 4, 1), # before monthly_start but quarterly rule matches -> included
                date(2021, 5, 1), # before monthly_start, non-quarter -> excluded
                date(2022, 3, 31), # before monthly_start and quarterly rule does not match -> excluded
                date(2022, 4, 1), # at boundary (monthly_start) -> included
                date(2023, 6, 1), # within range -> included
            ],
            expected=[True, False, False, True, True],
        ),
        ReducedDataFilterCase(
            id="test reduced_data_filter with non-default args",
            today=date(2024, 6, 15),
            fy_start_month=1,
            lookback_fy_years=1,
            quarter_months=(3, 6, 9, 12),
            input_data=[
                date(2022, 1, 1), # before monthly_start, non-quarter -> excluded
                date(2022, 2, 1), # before monthly_start, non-quarter -> excluded
                date(2022, 12, 1), # before monthly_start but quarterly rule matches -> included
                date(2023, 3, 1), # before monthly_start and quarterly rule matches -> included
                date(2024, 6, 1), # within range -> included
            ],
            expected=[False, False, True, True, True],
        ),
        ReducedDataFilterCase(
            id="test reduced_data_filter with today=None",
            today=None,
            fy_start_month=4,
            lookback_fy_years=2,
            quarter_months=(1, 4, 7, 10),
            input_data=[
                date.today(),  # should be included as it's the current date
                date(2021, 4, 1), # before monthly_start but quarterly rule matches -> included
                date(2021, 5, 1), # before monthly_start, non-quarter -> excluded
            ],
            expected=[True, True, False],
        ),
    ]  # fmt: skip


@dataclass
class ImputeJobRoleTestCase:
    id: str
    data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.data, id=self.id)


@dataclass
class ImputeJobRoleData:
    create_imputed_ascwds_job_role_counts_test_cases = [
        ImputeJobRoleTestCase(
            id="when_sufficient_data_present_to_impute",
            data=[
                ("1", "1", "job_role_a", date(2026, 1, 1), 1, 1.0, 1.0, 1.0, 1.0),
                ("2", "1", "job_role_a", date(2026, 1, 2), None, 2.0, None, 0.7, 1.4),
                ("3", "1", "job_role_a", date(2026, 1, 3), 4, 4.0, 0.4, 0.4, 1.6),
                ("4", "1", "job_role_b", date(2026, 1, 1), None, 1.0, None, 1.0, 1.0),
                ("5", "1", "job_role_b", date(2026, 1, 2), 2, 2.0, 1.0, 1.0, 2.0),
                ("6", "1", "job_role_b", date(2026, 1, 3), 6, 6.0, 0.6, 0.6, 3.6),
                ("7", "2", "job_role_a", date(2026, 1, 1), 1, 1.0, 1.0, 1.0, 1.0),
                ("8", "2", "job_role_a", date(2026, 1, 2), 9, 9.0, 1.0, 1.0, 9.0),
                ("9", "2", "job_role_a", date(2026, 1, 3), None, 1.0, None, 1.0, 1.0),
            ],
        ),
        ImputeJobRoleTestCase(
            id="when_all_nones",
            data=[
                ("1", "1", "job_role_a", date(2026, 1, 1), None, 1.0, None, None, None),
                ("2", "1", "job_role_a", date(2026, 1, 2), None, 2.0, None, None, None),
                ("3", "1", "job_role_a", date(2026, 1, 3), None, 4.0, None, None, None),
                ("4", "1", "job_role_b", date(2026, 1, 1), None, 1.0, None, None, None),
                ("5", "1", "job_role_b", date(2026, 1, 2), None, 2.0, None, None, None),
                ("6", "1", "job_role_b", date(2026, 1, 3), None, 6.0, None, None, None),
                ("7", "2", "job_role_a", date(2026, 1, 1), None, 1.0, None, None, None),
                ("8", "2", "job_role_a", date(2026, 1, 2), None, 9.0, None, None, None),
                ("9", "2", "job_role_a", date(2026, 1, 3), None, 1.0, None, None, None),
            ],
        ),
    ]  # fmt: skip

    create_ascwds_job_role_rolling_ratio_test_cases = [
        ImputeJobRoleTestCase(
            id="when_one_primary_service_present",
            data=[
                (1, "1000", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 4.0, "CHWN 1 to 19", 1.0, 0.4),
                (2, "1000", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 4.0, "CHWN 1 to 19", 2.0, 0.6),
                (3, "2000", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 4.0, "CHWN 1 to 19", 3.0, 0.4),
                (4, "2000", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 4.0, "CHWN 1 to 19", 4.0, 0.6),
                (5, "1000", date(2024, 1, 2), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 4.0, "CHWN 1 to 19", None, 0.4),
                (6, "1000", date(2024, 1, 2), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 4.0, "CHWN 1 to 19", None, 0.6),
                (7, "2000", date(2024, 1, 2), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 4.0, "CHWN 1 to 19", None, 0.4),
                (8, "2000", date(2024, 1, 2), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 4.0, "CHWN 1 to 19", None, 0.6),
                (9, "1000", date(2024, 1, 3), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 4.0, "CHWN 1 to 19", 5.0, 0.44444),
                (10, "1000", date(2024, 1, 3), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 4.0, "CHWN 1 to 19", 6.0, 0.55556),
                (11, "2000", date(2024, 1, 3), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 4.0, "CHWN 1 to 19", 7.0, 0.44444),
                (12, "2000", date(2024, 1, 3), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 4.0, "CHWN 1 to 19", 8.0, 0.55556),
            ],
        ),
        ImputeJobRoleTestCase(
            id="when_multiple_primary_services_present",
            data=[
                (1, "1000", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 4.0, "CHWN 1 to 19", 1.0, 0.33333),
                (2, "1000", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 4.0, "CHWN 1 to 19", 2.0, 0.66667),
                (3, "2000", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 8.0, "COH 1 to 9", 3.0, 0.428571),
                (4, "2000", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 8.0, "COH 1 to 9", 4.0, 0.571429),
                (5, "1000", date(2024, 1, 2), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 4.0, "CHWN 1 to 19", None, 0.333333),
                (6, "1000", date(2024, 1, 2), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 4.0, "CHWN 1 to 19", None, 0.666667),
                (7, "2000", date(2024, 1, 2), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 8.0, "COH 1 to 9", None, 0.428571),
                (8, "2000", date(2024, 1, 2), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 8.0, "COH 1 to 9", None, 0.571429),
                (9, "1000", date(2024, 1, 3), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 4.0, "CHWN 1 to 19", 5.0, 0.428571),
                (10, "1000", date(2024, 1, 3), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 4.0, "CHWN 1 to 19", 6.0, 0.571429),
                (11, "2000", date(2024, 1, 3), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, 8.0, "COH 1 to 9", 7.0, 0.454545 ),
                (12, "2000", date(2024, 1, 3), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, 8.0, "COH 1 to 9", 8.0, 0.545455),
            ],
        ),
        ImputeJobRoleTestCase(
            id="when_days_not_within_rolling_window",
            data=[
                (1, "1000", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 8.0, "CHWN 1 to 19", 1.0, 0.1),
                (2, "1000", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 8.0, "CHWN 1 to 19", 2.0, 0.2),
                (3, "1000", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.senior_care_worker, 8.0, "CHWN 1 to 19", 3.0, 0.3),
                (4, "1000", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.senior_management, 8.0, "CHWN 1 to 19", 4.0, 0.4),
                (5, "1000", date(2024, 7, 5), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, 8.0, "CHWN 1 to 19", 5.0, 0.192308),
                (6, "1000", date(2024, 7, 5), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, 8.0, "CHWN 1 to 19", 6.0, 0.230769),
                (7, "1000", date(2024, 7, 5), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.senior_care_worker, 8.0, "CHWN 1 to 19", 7.0, 0.269231),
                (8, "1000", date(2024, 7, 5), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.senior_management, 8.0, "CHWN 1 to 19", 8.0, 0.307692),
            ],
        ),
    ]  # fmt: skip


@dataclass
class EstimateFilledPostsByJobRoleEstimateUtilsTestCases:
    id: str
    expected_data: list[tuple]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.expected_data, id=self.id)


@dataclass
class EstimateFilledPostsByJobRoleEstimateUtilsData:
    calculate_estimated_filled_posts_by_job_role_rows = [
        (0, 10.0, 0.2, 0.4, 0.6, IndCQC.ascwds_job_role_ratios, 0.2, 2.0),
        (1, 10.0, None, 0.4, 0.6, IndCQC.imputed_ascwds_job_role_ratios, 0.4, 4.0),
        (2, 10.0, None, None, 0.6, IndCQC.ascwds_job_role_rolling_ratio, 0.6, 6.0),
        (3, 10.0, 0.2, 0.4, None, IndCQC.ascwds_job_role_ratios, 0.2, 2.0),
        (4, 10.0, 0.2, None, 0.6, IndCQC.ascwds_job_role_ratios, 0.2, 2.0),
        (6, None, 0.2, 0.4, 0.6, IndCQC.ascwds_job_role_ratios, 0.2, None),
        (7, 10.0, None, None, None, None, None, None),
    ]

    has_rm_in_cqc_rm_name_list_flag_rows = [
        (["name_1"], 1),
        (["name_1", "name_2"], 1),
        ([], 0),
        (None, 0),
    ]

    test_manager_roles = [
        MainJobRoleLabels.supervisor,
        MainJobRoleLabels.first_line_manager,
        MainJobRoleLabels.registered_manager,
    ]

    adjust_managerial_roles_rows = [
        (0, MainJobRoleLabels.care_worker, 10.0, 1.0),
        (0, MainJobRoleLabels.supervisor, 20.0, 1.0),
        (0, MainJobRoleLabels.registered_manager, 0.0, 1.0),
    ]
    expected_adjust_managerial_roles_rows = [
        (0, MainJobRoleLabels.care_worker, 10.0, 10.0),
        (0, MainJobRoleLabels.supervisor, 20.0, 19.0),
        (0, MainJobRoleLabels.registered_manager, 0.0, 1.0),
    ]

    calculate_reg_man_difference_test_cases = [
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="calculates_difference_between_rm_estimate_and_cqc_count",
            expected_data=[
                (0, MainJobRoleLabels.registered_manager, 5.0, 1.0, 4.0),
                (1, MainJobRoleLabels.registered_manager, 0.0, 5.0, -5.0),
            ],
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="rm_difference_is_copied_to_all_rows_in_group",
            expected_data=[
                (0, MainJobRoleLabels.supervisor, 20.0, 0.0, 1.0),
                (0, MainJobRoleLabels.registered_manager, 1.0, 0.0, 1.0),
            ],
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="rm_difference_is_calculated_per_group",
            expected_data=[
                (0, MainJobRoleLabels.supervisor, 20.0, 0.0, 1.0),
                (0, MainJobRoleLabels.registered_manager, 1.0, 0.0, 1.0),
                (1, MainJobRoleLabels.supervisor, 20.0, 0.0, 5.0),
                (1, MainJobRoleLabels.registered_manager, 5.0, 0.0, 5.0),
            ],
        ),
    ]

    calculate_non_rm_managerial_distribution_test_cases = [
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="calculates_non_rm_managerial_proportions_from_estimated_posts",
            expected_data=[
                (0, MainJobRoleLabels.care_worker, 10.0, None),
                (0, MainJobRoleLabels.supervisor, 60.0, 0.6),
                (0, MainJobRoleLabels.first_line_manager, 40.0, 0.4),
                (0, MainJobRoleLabels.registered_manager, 10.0, None),
            ],
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="distributes_evenly_when_non_rm_managerial_total_is_zero",
            expected_data=[
                (0, MainJobRoleLabels.supervisor, 0.0, 0.5),
                (0, MainJobRoleLabels.first_line_manager, 0.0, 0.5),
                (0, MainJobRoleLabels.registered_manager, 10.0, None),
            ],
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="calculates_non_rm_managerial_proportions_per_group",
            expected_data=[
                (0, MainJobRoleLabels.supervisor, 60.0, 0.6),
                (0, MainJobRoleLabels.first_line_manager, 40.0, 0.4),
                (0, MainJobRoleLabels.registered_manager, 10.0, None),
                (1, MainJobRoleLabels.supervisor, 0.0, 0.5),
                (1, MainJobRoleLabels.first_line_manager, 0.0, 0.5),
                (1, MainJobRoleLabels.registered_manager, 10.0, None),
            ],
        ),
    ]

    distribute_rm_difference_test_cases = [
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="overides_estimated_rm_posts_with_cqc_count",
            expected_data=[
                (0, MainJobRoleLabels.registered_manager, 0.0, 4, 4.0, None, 4.0),
                (1, MainJobRoleLabels.registered_manager, None, 0, -4.0, None, 0.0),
                (2, MainJobRoleLabels.registered_manager, 4.0, None, None, None, None),
            ],
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="redistributes_rm_difference_across_other_manager_roles",
            expected_data=[
                (0, MainJobRoleLabels.care_worker, 1.0, 0, 1.0, None, 1.0),
                (0, MainJobRoleLabels.supervisor, 1.0, 0, 1.0, 0.5, 1.5),
                (0, MainJobRoleLabels.first_line_manager, 1.0, 0, 1.0, 0.5, 1.5),
                (0, MainJobRoleLabels.registered_manager, 1.0, 0, 1.0, None, 0.0),
            ],
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="does_not_reduce_other_manager_roles_below_zero",
            expected_data=[
                (0, MainJobRoleLabels.supervisor, 0.1, 1, -1.0, 0.5, 0.0),
                (0, MainJobRoleLabels.first_line_manager, 0.1, 1, -1.0, 0.5, 0.0),
                (0, MainJobRoleLabels.registered_manager, 0.0, 1, -1.0, None, 1.0),
            ],
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="allows_total_manager_posts_mismatch_when_cqc_override_exceeds_available_posts",
            expected_data=[
                (0, MainJobRoleLabels.supervisor, 0.0, 5, -5.0, 0.5, 0.0),
                (0, MainJobRoleLabels.first_line_manager, 0.0, 5, -5.0, 0.5, 0.0),
                (0, MainJobRoleLabels.registered_manager, 0.0, 5, -5.0, None, 5.0),
            ],
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="leaves_values_unchanged_when_prediction_matches_cqc",
            expected_data=[
                (0, MainJobRoleLabels.supervisor, 10.0, 1, 0.0, 0.5, 10.0),
                (0, MainJobRoleLabels.first_line_manager, 10.0, 1, 0.0, 0.5, 10.0),
                (0, MainJobRoleLabels.registered_manager, 1.0, 1, 0.0, None, 1.0),
            ],
        ),
    ]

    expected_calc_diff_estimate_filled_posts_and_from_all_job_roles_rows = [
        (0, 10.0, MainJobRoleLabels.care_worker, 5.0, 10.0, 0.0), # All job roles have filled posts.
        (0, 10.0, MainJobRoleLabels.senior_care_worker, 5.0, 10.0, 0.0),
        (1, 10.0, MainJobRoleLabels.care_worker, 5.0, 5.0, -5.0), # Job roles have mix of value and null.
        (1, 10.0, MainJobRoleLabels.senior_care_worker, None, 5.0, -5.0),
        (2, 10.0, MainJobRoleLabels.care_worker, None, None, None), # All job role posts are null.
        (2, 10.0, MainJobRoleLabels.senior_care_worker, None, None, None),
    ]  # fmt: skip

    reallocate_historical_filled_posts_by_job_role_test_cases = [
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="reallocates_roles_pre_20230801",
            expected_data=[
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.deputy_manager, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.learning_and_development_lead, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.team_leader, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.data_analyst, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.data_governance_manager, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.it_and_digital_support, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.it_manager, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.it_service_desk_manager, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.software_developer, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.support_worker, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.activites_worker, 1.0, 1.0078),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.care_worker, 1.0, 2.2914),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.community_support_and_outreach, 1.0, 1.2537),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.first_line_manager, 1.0, 1.6039),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.senior_care_worker, 1.0, 1.4977),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.other_managerial_staff, 1.0, 4.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.other_non_care_related_staff, 1.0, 5.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.supervisor, 1.0, 1.3455),
            ], # fmt: skip
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="reallocates_roles_pre_20240601",
            expected_data=[
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.deputy_manager, 1.0, 1.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.learning_and_development_lead, 1.0, 1.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.team_leader, 1.0, 1.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.data_analyst, 1.0, 0.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.data_governance_manager, 1.0, 0.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.it_and_digital_support, 1.0, 0.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.it_manager, 1.0, 0.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.it_service_desk_manager, 1.0, 0.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.software_developer, 1.0, 0.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.support_worker, 1.0, 0.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.activites_worker, 1.0, 1.0078),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.care_worker, 1.0, 1.7219),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.community_support_and_outreach, 1.0, 1.2537),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.first_line_manager, 1.0, 1.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.senior_care_worker, 1.0, 1.0166),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.other_managerial_staff, 1.0, 4.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.other_non_care_related_staff, 1.0, 4.0),
                (1, "1-001", date(2024, 5, 31), MainJobRoleLabels.supervisor, 1.0, 1.0),
            ], # fmt: skip
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="handles_different_import_dates",
            expected_data=[
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.deputy_manager, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.learning_and_development_lead, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.team_leader, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.data_analyst, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.data_governance_manager, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.it_and_digital_support, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.it_manager, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.it_service_desk_manager, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.software_developer, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.support_worker, 1.0, 0.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.activites_worker, 1.0, 1.0078),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.care_worker, 1.0, 2.2914),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.community_support_and_outreach, 1.0, 1.2537),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.first_line_manager, 1.0, 1.6039),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.senior_care_worker, 1.0, 1.4977),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.other_managerial_staff, 1.0, 4.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.other_non_care_related_staff, 1.0, 5.0),
                (1, "1-001", date(2023, 7, 31), MainJobRoleLabels.supervisor, 1.0, 1.3455),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.deputy_manager, 1.0, 0.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.learning_and_development_lead, 1.0, 0.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.team_leader, 1.0, 0.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.data_analyst, 1.0, 0.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.data_governance_manager, 1.0, 0.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.it_and_digital_support, 1.0, 0.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.it_manager, 1.0, 0.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.it_service_desk_manager, 1.0, 0.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.software_developer, 1.0, 0.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.support_worker, 1.0, 0.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.activites_worker, 2.0, 2.0078),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.care_worker, 2.0, 3.2914),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.community_support_and_outreach, 2.0, 2.2537),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.first_line_manager, 2.0, 2.6039),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.senior_care_worker, 2.0, 2.4977),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.other_managerial_staff, 2.0, 5.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.other_non_care_related_staff, 2.0, 6.0),
                (2, "1-001", date(2023, 7, 30), MainJobRoleLabels.supervisor, 2.0, 2.3455),
            ], # fmt: skip
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="does_not_change_values_for_roles_not_involved_in_reallocation",
            expected_data=[
                (1,"1-001", date(2023, 7, 31), MainJobRoleLabels.care_worker, 1.0, 1.0),
                (2,"1-001", date(2023, 8, 1), MainJobRoleLabels.care_worker, 1.0, 1.0),
                (3,"1-001", date(2024, 5, 31), MainJobRoleLabels.care_worker, 1.0, 1.0),
                (4,"1-001", date(2024, 6, 1), MainJobRoleLabels.care_worker, 1.0, 1.0),
            ], # fmt: skip
        ),
        EstimateFilledPostsByJobRoleEstimateUtilsTestCases(
            id="duplicates_filled_posts_when_date_is_after_reallocation_cutoff",
            expected_data=[
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.deputy_manager, 1.0, 1.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.learning_and_development_lead, 1.0, 1.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.team_leader, 1.0, 1.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.data_analyst, 1.0, 0.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.data_governance_manager, 1.0, 0.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.it_and_digital_support, 1.0, 0.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.it_manager, 1.0, 0.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.it_service_desk_manager, 1.0, 0.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.software_developer, 1.0, 0.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.support_worker, 1.0, 0.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.activites_worker, 1.0, 1.0078),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.care_worker, 1.0, 1.7219),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.community_support_and_outreach, 1.0, 1.2537),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.first_line_manager, 1.0, 1.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.senior_care_worker, 1.0, 1.0166),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.other_managerial_staff, 1.0, 4.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.other_non_care_related_staff, 1.0, 4.0),
                (1, "1-001", date(2023, 8, 1), MainJobRoleLabels.supervisor, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.deputy_manager, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.learning_and_development_lead, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.team_leader, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.data_analyst, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.data_governance_manager, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.it_and_digital_support, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.it_manager, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.it_service_desk_manager, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.software_developer, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.support_worker, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.activites_worker, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.care_worker, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.community_support_and_outreach, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.first_line_manager, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.senior_care_worker, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.other_managerial_staff, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.other_non_care_related_staff, 1.0, 1.0),
                (2, "1-001", date(2024, 6, 1), MainJobRoleLabels.supervisor, 1.0, 1.0),
            ], # fmt: skip
        ),
    ]
    reallocate_historical_filled_posts_by_job_role_raise_error_rows = [
        (1, "1-001", date(2024, 4, 1), MainJobRoleLabels.data_governance_manager, None, 1.0),
        (1, "1-001", date(2024, 4, 1), MainJobRoleLabels.it_manager, 1.0, 1.0),
        (1, "1-001", date(2024, 4, 1), MainJobRoleLabels.it_service_desk_manager, 1.0, 1.0),
        (1, "1-001", date(2024, 4, 1), MainJobRoleLabels.other_managerial_staff, 1.0, 1.0),
    ] # fmt: skip


@dataclass
class ModelInterpolationTestCase:
    id: str
    data: list[Any]
    method: str = None
    max_days_between_submissions: int = None


@dataclass
class InterpolationData:
    interpolation_test_cases = [
        ModelInterpolationTestCase(
            id="trend_interpolation_single_gap",
            data=[
                ("1-001", date(2023, 3, 1), 40.0, None, None),
                ("1-001", date(2023, 6, 1), None, 46.0, 34.185792),
                ("1-001", date(2024, 3, 1), 5.0, 52.0, None),
            ],
            method="trend",
        ),
        ModelInterpolationTestCase(
            id="trend_interpolation_not_possible_with_single_point",
            data=[
                ("1-001", date(2023, 1, 1), None, None, None),
                ("1-001", date(2023, 2, 1), 10.0, None, None),
                ("1-001", date(2023, 3, 1), None, None, None),
            ],
            method="trend",
        ),
        ModelInterpolationTestCase(
            id="straight_interpolation_single_gap",
            data=[
                ("1-001", date(2023, 3, 1), 40.0, None, None),
                ("1-001", date(2023, 4, 1), None, 42.0, 37.035519),
                ("1-001", date(2024, 3, 1), 5.0, 52.0, None),
            ],
            method="straight",
        ),
        ModelInterpolationTestCase(
            id="straight_interpolation_not_possible_with_single_point",
            data=[
                ("1-001", date(2023, 1, 1), None, None, None),
                ("1-001", date(2023, 2, 1), 10.0, None, None),
                ("1-001", date(2023, 3, 1), None, None, None),
            ],
            method="straight",
        ),
    ]  # fmt: skip

    calculate_residual_test_cases = [
        ModelInterpolationTestCase(
            id="extrapolation_forwards_is_known_rows",
            data=[
                ("1-001", date(2023, 5, 1), None, 44.0, -47.0),
                ("1-001", date(2023, 4, 1), None, 42.0, -47.0),
                ("1-001", date(2024, 3, 1), 5.0, 52.0, -47.0),
                ("1-001", date(2024, 4, 1), None, 5.1, 9.8),
                ("1-001", date(2024, 5, 1), 15.0, 5.2, 9.8),
            ],
        ),
        ModelInterpolationTestCase(
            id="extrapolation_forwards_is_none_rows",
            data=[
                ("1-001", date(2023, 1, 1), None, None, None),
                ("1-001", date(2023, 3, 1), 40.0, None, None),
            ],
        ),
        ModelInterpolationTestCase(
            id="returns_none_date_after_final_non_null_submission_rows",
            data=[
                ("1-001", date(2024, 5, 1), 15.0, 5.2, 9.8),
                ("1-001", date(2024, 6, 1), None, 15.3, None),
            ],
        ),
    ]  # fmt: skip
    calculate_days_between_submissions_test_cases = [
        ModelInterpolationTestCase(
            id="one_location_data",
            data=[
                ("1-001", date(2024, 2, 1), None, None, None),
                ("1-001", date(2024, 3, 1), 5.0, None, None),
                ("1-001", date(2024, 4, 1), None, 122, 0.25),
                ("1-001", date(2024, 5, 1), None, 122, 0.5),
                ("1-001", date(2024, 6, 1), None, 122, 0.75),
                ("1-001", date(2024, 7, 1), 15.0, None, None),
                ("1-001", date(2023, 8, 1), None, None, None),
            ],
        ),
        ModelInterpolationTestCase(
            id="multiple_locations_single_submission_each",
            data=[
                ("1-001", date(2024, 1, 1), 5.0, None, None),
                ("1-001", date(2024, 2, 1), None, None, None),
                ("1-002", date(2024, 1, 5), 3.0, None, None),
                ("1-002", date(2024, 2, 5), None, None, None),
                ("1-003", date(2024, 3, 1), 8.0, None, None),
                ("1-003", date(2024, 4, 1), None, None, None),
            ],
        ),
        ModelInterpolationTestCase(
            id="two_locations_interleaved",
            data=[
                ("1-001", date(2024, 1, 1), 10.0, None, None),
                ("1-002", date(2024, 1, 2), 10.0, None, None),
                ("1-001", date(2024, 2, 1), None, 60, 0.51),
                ("1-002", date(2024, 3, 2), None, 90, 0.66),
                ("1-001", date(2024, 3, 1), 7.0, None, None),
                ("1-002", date(2024, 4, 1), 7.0, None, None),
            ],
        ),
    ]  # fmt: skip
    calculate_interpolated_values_test_cases = [
        ModelInterpolationTestCase(
            id="test_function_returns_expected_values_when_within_max_days",
            data=[
                ("1-001", date(2025, 1, 3), 20.0, None, None, None, None, None),
                ("1-001", date(2025, 1, 4), None, 20.0, 10.0, 4, 0.25, 22.5),
                ("1-001", date(2025, 1, 5), None, 20.0, 10.0, 4, 0.5, 25.0),
                ("1-001", date(2025, 1, 6), None, 20.0, 10.0, 4, 0.75, 27.5),
                ("1-001", date(2025, 1, 7), 30.0, 20.0, 10.0, None, None, None),
                ("1-001", date(2025, 1, 8), None, None, None, None, None, None),
            ],
            max_days_between_submissions=4,
        ),
        ModelInterpolationTestCase(
            id="test_function_returns_expected_values_when_outside_max_days",
            data=[
                ("1-001", date(2025, 1, 3), 20.0, None, None, None, None, None),
                ("1-001", date(2025, 1, 4), None, 20.0, 10.0, 3, 0.25, None),
                ("1-001", date(2025, 1, 5), None, 20.0, 10.0, 3, 0.5, None),
                ("1-001", date(2025, 1, 6), None, 20.0, 10.0, 3, 0.75, None),
                ("1-001", date(2025, 1, 7), 30.0, 20.0, 10.0, None, None, None),
                ("1-001", date(2025, 1, 8), None, None, None, None, None, None),
            ],
            max_days_between_submissions=2,
        ),
    ]  # fmt: skip


@dataclass
class ExtrapolationTestCase:
    id: str
    data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.data, id=self.id)


@dataclass
class ModelExtrapolation:
    extrapolation_when_nominal_test_cases = [
        ExtrapolationTestCase(
            id="when_one_later_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 20.0, 100.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, -60.0, -60.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_later_data_points_are_missing",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, 20.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 90.0, 90.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_earlier_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_earlier_data_points_are_missing",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_intermediate_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 15.0, 10.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, 25.0, None),
                ("1-001", date(2026, 3, 1), 30.0, 30.0, 35.0, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_only_one_observed_value_exists",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 30.0, 20.0, 20.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_model_extrapolation_only_applies_after_last_submission",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, None),
                ("1-001", date(2026, 3, 1), 50.0, 40.0, 30.0, None),
                ("1-001", date(2026, 4, 1), None, 50.0, 60.0, 60.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_gaps_exist_across_full_time_series",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 30.0, 20.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-001", date(2026, 4, 1), 20.0, 80.0, 70.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-001", date(2026, 5, 1), None, 100.0, 40.0, 40.0), # Forwards extrapolation from previously known value (20.0)
            ],
        ),
        ExtrapolationTestCase(
            id="when_more_than_one_location_needs_extrapolating",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, 20.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 90.0, 90.0),
                ("1-002", date(2026, 1, 1), None, 10.0, None, 0.0),
                ("1-002", date(2026, 2, 1), None, 10.0, None, 0.0),
                ("1-002", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_no_data_points_are_available",
            data=[
                ("1-001", date(2026, 3, 1), None, None, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_dates_are_not_sorted_within_group",
            data=[
                ("1-001", date(2026, 2, 1), None, 30.0, 20.0, 20.0),
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 100.0, 90.0, 90.0),
            ],
)
    ] # fmt: skip
    extrapolation_when_ratio_test_cases = [
        ExtrapolationTestCase(
            id="when_one_later_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 20.0, 100.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, 4.0, 4.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_later_data_points_are_missing",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, 15.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 50.0, 50.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_earlier_data_point_is_missing",
            data=[
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_earlier_data_points_are_missing",
            data=[
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_one_intermediate_data_point_is_missing",
            data=[
                ("1-001", date(2026, 1, 1), 15.0, 10.0, None, None),
                ("1-001", date(2026, 2, 1), None, 20.0, 30.0, None),
                ("1-001", date(2026, 3, 1), 30.0, 30.0, 45.0, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_only_one_observed_value_exists",
            data=[
                ("1-001", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-001", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 30.0, 15.0, 15.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_model_extrapolation_only_applies_after_last_submission",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, None),
                ("1-001", date(2026, 3, 1), 50.0, 40.0, 20.0, None),
                ("1-001", date(2026, 4, 1), None, 20.0, 25.0, 25.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_gaps_exist_across_full_time_series",
            data=[
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), 10.0, 20.0, None, None),
                ("1-002", date(2026, 3, 1), None, 30.0, 15.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-002", date(2026, 4, 1), 20.0, 80.0, 40.0, None), # Forwards extrapolation from previously known value (10.0)
                ("1-002", date(2026, 5, 1), None, 100.0, 25.0, 25.0), # Forwards extrapolation from previously known value (20.0)
            ],
        ),
        ExtrapolationTestCase(
            id="when_more_than_one_location_needs_extrapolating",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, 15.0),
                ("1-001", date(2026, 3, 1), None, 100.0, 50.0, 50.0),
                ("1-002", date(2026, 1, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 2, 1), None, 10.0, None, 5.0),
                ("1-002", date(2026, 3, 1), 10.0, 20.0, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_no_data_points_are_available",
            data=[
                ("1-003", date(2026, 3, 1), None, None, None, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_dates_are_not_sorted_within_group",
            data=[
                ("1-001", date(2026, 2, 1), None, 30.0, 15.0, 15.0),
                ("1-001", date(2026, 1, 1), 10.0, 20.0, None, None),
                ("1-001", date(2026, 3, 1), None, 100.0, 50.0, 50.0),
            ],
        ),
    ] # fmt: skip
    expected_extrapolation_when_error_rows = [
        ("1-001", date(2026, 1, 1), None, 10.0, None, None),
    ]

    extrapolation_aggregates_rows = [
        ("1-001", date(2026, 1, 1), None, 10.0),
        ("1-001", date(2026, 2, 1), 10.0, 20.0),
        ("1-001", date(2026, 3, 1), 20.0, 30.0),
        ("1-002", date(2026, 1, 1), 15.0, 40.0),
        ("1-002", date(2026, 2, 1), None, 50.0),
        ("1-003", date(2026, 1, 1), None, None),
    ]
    expected_extrapolation_aggregates_rows = [
        ("1-001", date(2026, 2, 1), date(2026, 3, 1), 10.0, 20.0),
        ("1-002", date(2026, 1, 1), date(2026, 1, 1), 15.0, 40.0),
    ]

    get_previous_value_test_cases = [
        ExtrapolationTestCase(
            id="when_values_are_sequential_no_nulls",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 2, 1), 20.0, 10.0),
                ("1-001", date(2026, 3, 1), 30.0, 20.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_values_have_null_gap",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 2, 1), None, 10.0),
                ("1-001", date(2026, 3, 1), None, 10.0),
                ("1-001", date(2026, 4, 1), 40.0, 10.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_values_have_leading_nulls",
            data=[
                ("1-001", date(2026, 1, 1), None, None),
                ("1-001", date(2026, 2, 1), None, None),
                ("1-001", date(2026, 3, 1), 30.0, None),
            ],
        ),
        ExtrapolationTestCase(
            id="when_multiple_groups_are_present",
            data=[
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 2, 1), None, 10.0),
                ("1-002", date(2026, 1, 1), 5.0, None),
                ("1-002", date(2026, 2, 1), None, 5.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_input_is_unsorted_within_group",
            data=[
                ("1-001", date(2026, 2, 1), None, 10.0),
                ("1-001", date(2026, 1, 1), 10.0, None),
                ("1-001", date(2026, 3, 1), 30.0, 10.0),
            ],
        ),
        ExtrapolationTestCase(
            id="when_all_values_are_null",
            data=[
                ("1-001", date(2026, 1, 1), None, None),
                ("1-001", date(2026, 2, 1), None, None),
            ],
        ),
    ]


@dataclass
class ModelImputationTestCase:
    id: str
    expected_data: list[Any]

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.expected_data, id=self.id)


@dataclass
class ModelImputation:
    column_with_null_values_name: str = "null_values"
    model_column_name: str = "trend_model"
    imputed_values_column_name: str = "imputed_values"

    expected_model_imputation_test_cases = [
        ModelImputationTestCase(
            id="when_values_will_be_extrapolated",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, None, 1.0, 19.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, 20.0, 2.0, 20.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, None, 3.0, 21.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_be_interpolated",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, 20.0, 1.0, 20.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, None, 2.0, 21.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, 22.0, 3.0, 22.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_be_extrapolated_and_interpolated",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, 20.0, 1.0, 20.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, None, 2.0, 21.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, 22.0, 3.0, 22.0),
                ("1-001", date(2023, 4, 1), CareHome.not_care_home, None, 4.0, 23.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_not_be_imputed_because_all_present",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, 30.0, 1.0, 30.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, 31.0, 2.0, 31.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, 32.0, 3.0, 32.0),
            ],
        ),
        ModelImputationTestCase(
            id="when_values_will_not_be_imputed_because_all_null",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, None, 1.0, None),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, None, 2.0, None),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, None, 3.0, None),
            ],
        ),
        ModelImputationTestCase(
            id="when_given_multiple_service_types_only_non_res_gets_imputed_due_to_function_argument",
            expected_data=[
                ("1-001", date(2023, 1, 1), CareHome.not_care_home, None, 1.0, 19.0),
                ("1-001", date(2023, 2, 1), CareHome.not_care_home, 20.0, 2.0, 20.0),
                ("1-001", date(2023, 3, 1), CareHome.not_care_home, None, 3.0, 21.0),
                ("1-002", date(2023, 1, 1), CareHome.care_home, None, 1.0, None),
                ("1-002", date(2023, 2, 1), CareHome.care_home, 20.0, 2.0, None),
                ("1-002", date(2023, 3, 1), CareHome.care_home, None, 3.0, None),
            ],
        ),
    ] # fmt: skip


@dataclass
class ModelRateOfChangeInputOutputTestCase:
    id: str
    input_data: list[Any]
    expected_data: list[Any]

    def as_pytest_param(self):
        return pytest.param(self.input_data, self.expected_data, id=self.id)


@dataclass
class ModelRateOfChangeOutputOnlyTestCase:
    id: str
    data: list[Any]

    def as_pytest_param(self):
        return pytest.param(self.data, id=self.id)


@dataclass
class ModelRateOfChangeData:
    model_roc_trendline_test_cases = [
        ModelRateOfChangeOutputOnlyTestCase(
            id="single_group_produces_expected_trendline",
            data=[
                ("1-001", date(2026, 1, 1), CareHome.care_home, 10, "CHO", 3.0, 1, 1.0),
                ("1-001", date(2026, 1, 2), CareHome.care_home, 10, "CHO", 2.7, 1, 0.9),
                ("1-001", date(2026, 1, 3), CareHome.care_home, 10, "CHO", 3.3, 1, 0.947),
            ],
        ),
        ModelRateOfChangeOutputOnlyTestCase(
            id="multiple_groups_aggregate_independently",
            data=[
                ("1-001", date(2026, 1, 1), CareHome.care_home,     10, "CHO", 3.0,  1, 1.0),
                ("1-001", date(2026, 1, 2), CareHome.care_home,     10, "CHO", 2.8,  1, 1.04),
                ("1-001", date(2026, 1, 3), CareHome.care_home,     10, "CHO", 3.4,  1, 1.162),
                ("1-002", date(2026, 1, 1), CareHome.care_home,     10, "CHO", 2.0,  1, 1.0),
                ("1-002", date(2026, 1, 2), CareHome.care_home,     10, "CHO", 2.4,  1, 1.04),
                ("1-002", date(2026, 1, 3), CareHome.care_home,     10, "CHO", 2.8,  1, 1.162),
                ("1-003", date(2026, 1, 1), CareHome.not_care_home, 10, "NR",  40.0, 1, 1.0),
                ("1-003", date(2026, 1, 2), CareHome.not_care_home, 10, "NR",  50.0, 1, 1.25),
            ],
        ),
        ModelRateOfChangeOutputOnlyTestCase(
            id="unsorted_input_produces_correct_trendline",
            data=[
                ("1-001", date(2026, 1, 3), CareHome.care_home, 10, "CHO", 3.3, 1, 0.947),
                ("1-001", date(2026, 1, 1), CareHome.care_home, 10, "CHO", 3.0, 1, 1.0),
                ("1-001", date(2026, 1, 2), CareHome.care_home, 10, "CHO", 2.7, 1, 0.9),
            ],
        ),
        ModelRateOfChangeOutputOnlyTestCase(
            id="missing_values_are_interpolated_before_trendline",
            data=[
                ("1-001", date(2026, 1, 1), CareHome.care_home, 10, "CHO", 3.0,  1, 1.0),
                ("1-001", date(2026, 1, 2), CareHome.care_home, 10, "CHO", None, 1, 1.1),
                ("1-001", date(2026, 1, 3), CareHome.care_home, 10, "CHO", 3.6,  1, 1.205),
            ],
        ),
        ModelRateOfChangeOutputOnlyTestCase(
            id="locations_with_single_submission_do_not_influence_aggregate",
            data=[
                ("1-001", date(2026, 1, 1), CareHome.care_home, 10, "CHO", 3.0, 1, 1.0),
                ("1-001", date(2026, 1, 2), CareHome.care_home, 10, "CHO", 2.7, 1, 0.9),
                ("1-001", date(2026, 1, 3), CareHome.care_home, 10, "CHO", 3.3, 1, 0.947),
                ("1-002", date(2026, 1, 2), CareHome.care_home, 10, "CHO", 9.9, 1, 0.9), # single submission - shouldn't contribute
            ],
        ),
        ModelRateOfChangeOutputOnlyTestCase(
            id="locations_with_inconsistent_status_do_not_influence_aggregate",
            data=[
                ("1-001", date(2026, 1, 1), CareHome.care_home,     10, "CHO", 3.0, 1, 1.0),
                ("1-001", date(2026, 1, 2), CareHome.care_home,     10, "CHO", 2.7, 1, 0.9),
                ("1-001", date(2026, 1, 3), CareHome.care_home,     10, "CHO", 3.3, 1, 0.947),
                ("1-002", date(2026, 1, 2), CareHome.care_home,     10, "CHO", 9.9, 2, 0.9), # main service changed - shouldn't contribute
                ("1-002", date(2026, 1, 3), CareHome.not_care_home, 10, "NR",  9.9, 2, 1.0), # main service changed - shouldn't contribute
            ],
        ),
        ModelRateOfChangeOutputOnlyTestCase(
            id="first_time_period_is_always_1",
            data=[
                ("1-001", date(2026, 1, 1), CareHome.care_home, 10, "CHO", 3.0, 1, 1.0),
            ],
        ),
    ] # fmt: skip

    calculate_rolling_sums_test_cases = [
        ModelRateOfChangeInputOutputTestCase(
            id="single_group_within_time_window",
            input_data=[
                ("1-001", "CHO", 10, date(2026, 1, 1), 3.0, 2.0),
                ("1-001", "CHO", 10, date(2026, 1, 2), 2.7, 3.0),
                ("1-001", "CHO", 10, date(2026, 1, 3), 3.3, 2.7),
            ],
            expected_data=[
                ("1-001", "CHO", 10, date(2026, 1, 1), 3.0, 2.0),
                ("1-001", "CHO", 10, date(2026, 1, 2), 5.7, 5.0),
                ("1-001", "CHO", 10, date(2026, 1, 3), 9.0, 7.7),
            ],
        ),
        ModelRateOfChangeInputOutputTestCase(
            id="multiple_groups_within_time_window",
            input_data=[
                ("1-001", "CHO", 10, date(2026, 1, 1), 3.0, 2.0),
                ("1-001", "CHO", 10, date(2026, 1, 2), 2.7, 3.0),
                ("1-002", "NR",  20, date(2026, 1, 1), 1.0, 1.5),
                ("1-002", "NR",  20, date(2026, 1, 2), 1.2, 1.0),
            ],
            expected_data=[
                ("1-001", "CHO", 10, date(2026, 1, 1), 3.0, 2.0),
                ("1-001", "CHO", 10, date(2026, 1, 2), 5.7, 5.0),
                ("1-002", "NR",  20, date(2026, 1, 1), 1.0, 1.5),
                ("1-002", "NR",  20, date(2026, 1, 2), 2.2, 2.5),
            ],
        ),
        ModelRateOfChangeInputOutputTestCase(
            id="time_window_taken_into_account",
            input_data=[
                ("1-001", "CHO", 10, date(2026, 1, 1), 3.0, 2.0),
                ("1-001", "CHO", 10, date(2026, 1, 2), 2.9, 3.0),
                ("1-001", "CHO", 10, date(2026, 1, 3), 2.8, 4.0),
                ("1-001", "CHO", 10, date(2026, 1, 4), 2.7, 5.0),
                ("1-001", "CHO", 10, date(2026, 1, 5), 2.6, 6.0),
            ],
            expected_data=[
                ("1-001", "CHO", 10, date(2026, 1, 1), 3.0,  2.0),
                ("1-001", "CHO", 10, date(2026, 1, 2), 5.9,  5.0), # sum of 1st - 2nd Jan (tests based on a 3 day window)
                ("1-001", "CHO", 10, date(2026, 1, 3), 8.7,  9.0), # sum of 1st - 3rd Jan
                ("1-001", "CHO", 10, date(2026, 1, 4), 8.4, 12.0), # sum of 2nd - 4th Jan
                ("1-001", "CHO", 10, date(2026, 1, 5), 8.1, 15.0), # sum of 3rd - 5th Jan
            ],
        ),
    ] # fmt: skip

    clean_non_residential_rate_of_change_test_cases = [
        ModelRateOfChangeOutputOnlyTestCase(
            id="no_threshold_population_filters_non_residential_rows",
            data=[
                # non-residential → falls into "small" condition → kept
                ("1-001", CareHome.not_care_home, date(2026, 1, 1), 2.0, 3.0, 2.0, 3.0),
                ("1-001", CareHome.not_care_home, date(2026, 1, 2), 3.0, 2.0, 3.0, 2.0),
                # care home always kept
                ("1-002", CareHome.care_home,     date(2026, 1, 1), 2.0, 3.0, 2.0, 3.0),
            ],
        ),
        ModelRateOfChangeOutputOnlyTestCase(
            id="null_inputs_produce_null_outputs",
            data=[
                ("1-001", CareHome.not_care_home, date(2026, 1, 1), None, 3.0, None, None),
                ("1-001", CareHome.not_care_home, date(2026, 1, 2), 3.0, None, None, None),
            ],
        ),
        ModelRateOfChangeOutputOnlyTestCase(
            id="care_home_rows_bypass_cleaning_rules",
            data=[
                ("1-001", CareHome.care_home, date(2026, 1, 1), 10.0, 1000.0, 10.0, 1000.0),
            ],
        ),
    ] # fmt: skip

    calculate_trendline_test_cases = [
        ModelRateOfChangeInputOutputTestCase(
            id="trendline_with_single_group",
            input_data=[
                ("CHO", 1, date(2026, 1, 1), 1.0),
                ("CHO", 1, date(2026, 1, 2), 1.2),
                ("CHO", 1, date(2026, 1, 3), 1.0),
                ("CHO", 1, date(2026, 1, 4), 0.8),
            ],
            expected_data=[
                ("CHO", 1, date(2026, 1, 1), 1.0),
                ("CHO", 1, date(2026, 1, 2), 1.2),
                ("CHO", 1, date(2026, 1, 3), 1.2),
                ("CHO", 1, date(2026, 1, 4), 0.96),
            ],
        ),
        ModelRateOfChangeInputOutputTestCase(
            id="trendline_with_multiple_groups",
            input_data=[
                ("CHO", 1, date(2026, 1, 1), 1.0),
                ("CHO", 1, date(2026, 1, 2), 1.2),
                ("CHO", 2, date(2026, 1, 1), 1.0),
                ("CHO", 2, date(2026, 1, 2), 1.1),
                ("NR", 1, date(2026, 1, 1), 1.0),
                ("NR", 1, date(2026, 1, 2), 0.8),
            ],
            expected_data=[
                ("CHO", 1, date(2026, 1, 1), 1.0),
                ("CHO", 1, date(2026, 1, 2), 1.2),
                ("CHO", 2, date(2026, 1, 1), 1.0),
                ("CHO", 2, date(2026, 1, 2), 1.1),
                ("NR", 1, date(2026, 1, 1), 1.0),
                ("NR", 1, date(2026, 1, 2), 0.8),
            ],
        ),
    ]


@dataclass
class EstimateFilledPostsByJobRoleCleanUtilsTestCase:
    id: str
    test_data: list[Any]
    expected_data: list[Any]
    expected_brand_prov_data: Optional[list[Any]] = None
    min_workers_threshold: Optional[int] = None

    def __post_init__(self):
        if self.expected_brand_prov_data is None:
            self.expected_brand_prov_data = self.expected_data


@dataclass
class EstimateFilledPostsByJobRoleCleanUtilsData:
    filter_when_job_group_ratio_outside_percentile_bounds_test_cases = [
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="filters_by_id_column_when_outside_bounds_when_one_job_role_per_job_group",
            test_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 300, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 3, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
            ],
            min_workers_threshold=1,
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="filters_by_id_column_when_outside_bounds_when_multiple_job_role_per_job_group",
            test_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 150, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 150, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 4, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 5, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 4, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 5, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
            ],
            min_workers_threshold=1,
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="retains_values_when_ratio_within_bounds",
            test_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 6, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 6, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
            ],
            min_workers_threshold=1,
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="filters_by_id_column_when_outside_bounds_when_multiple_primary_service_types_present_at_different_ids",
            test_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 300, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 3, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
            ],
            min_workers_threshold=1,
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="filters_by_id_column_when_outside_bounds_when_multiple_primary_service_types_present_at_same_id",
            test_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 300, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 3, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
            ],
            expected_brand_prov_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 300, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_with_nursing, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
            ],
            min_workers_threshold=1,
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="filters_by_id_column_when_outside_bounds_when_multiple_locations_and_dates_present",
            test_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 990, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc1", date(2025, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc1", date(2025, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc2", date(2025, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                ("loc2", date(2025, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 3, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc1", date(2025, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc1", date(2025, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc2", date(2025, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc2", date(2025, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
            ],
            min_workers_threshold=1,
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="filters_by_id_column_when_outside_bounds_when_null_values_present",
            test_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 990, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc4", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_raw_data),
                ("loc4", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_raw_data),
            ],
            expected_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc4", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_raw_data),
                ("loc4", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_raw_data),
            ],
            min_workers_threshold=1,
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="filtered_by_id_column_when_above_min_workers_threshold",
            test_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 3, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
            ],
            min_workers_threshold=3,
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="filtered_by_id_column_when_on_min_workers_threshold",
            test_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 3, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
            ],
            min_workers_threshold=4,
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="not_filtered_by_id_column_when_below_min_workers_threshold",
            test_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 3, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                ("loc1", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 3, JobRoleFilteringRule.populated),
            ],
            min_workers_threshold=5,
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="does_not_filter_when_id_column_is_null", # brand id can be null in our dataset
            test_data=[
                (None, date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 300, JobRoleFilteringRule.populated),
                (None, date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 3, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                (None, date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 300, JobRoleFilteringRule.populated),
                (None, date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 9, JobRoleFilteringRule.populated),
                ("loc2", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
                ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.job_role_group_is_outlier_at_location_level),
            ],
            min_workers_threshold=1,
        ),
    ]  # fmt: skip

    test_error_handling_data =[
        ("loc3", date(2024, 1, 1), PrimaryServiceType.care_home_only, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
    ]# fmt: skip

    test_id_column_sum_expr_rows = [
        (1, 3, 5, 7, 16),
        (2, 4, 6, 8, 20),
        (None, None, None, None, 0),  # Test handling of null values
    ]
    test_job_group_percentage_rows = [
        (0.0625, 0.1875, 0.3125, 0.4375, 16),
        (0.1,    0.2,    0.3,    0.4,    20),
        (None, None, None, None, 0), # Test handling of null values
    ] # fmt: skip
    test_evaluation_expr_rows = [
        (0.1, 0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, 1), # All within bounds
        (0.3, 0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, 1), # Direct care above upper bound
        (0.1, 0.3, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, 1), # Managers above upper bound
        (0.1, 0.1, 0.3, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, 1), # Regulated professionals above upper bound
        (0.1, 0.1, 0.1, 0.3, 0.2, 0.2, 0.2, 0.2, 0.05, 1), # Other above upper bound
        (0.01, 0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, 1), # Direct care below lower bound
        (0.01, 0.3, 0.3, 0.3, 0.2, 0.2, 0.2, 0.2, 0.05, 1), # All out of bounds
        (None, None, None, None, None, None, None, None, None, None), # Test handling of null values
    ] # fmt: skip
    expected_evaluation_expr_rows = [
        (0.1, 0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, 1), # All within bounds
        (0.3, 0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, None), # Direct care above upper bound
        (0.1, 0.3, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, None), # Managers above upper bound
        (0.1, 0.1, 0.3, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, None), # Regulated professionals above upper bound
        (0.1, 0.1, 0.1, 0.3, 0.2, 0.2, 0.2, 0.2, 0.05, None), # Other above upper bound
        (0.01, 0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, None), # Direct care below lower bound
        (0.01, 0.3, 0.3, 0.3, 0.2, 0.2, 0.2, 0.2, 0.05, None), # All out of bounds
        (None, None, None, None, None, None, None, None, None, None), # Test handling of null values
    ] # fmt: skip

    filter_job_role_group_equal_zero_test_cases = [
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="when_all_groups_not_zero",
            test_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 0, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 0, JobRoleFilteringRule.populated),
            ],
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="when_direct_care_equal_zero",
            test_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
            ],
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="when_man_and_reg_prof_equal_zero",
            test_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 0, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
            ],
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="when_man_only_equal_zero",
            test_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
            ],
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="when_reg_prof_only_equal_zero",
            test_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 0, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 0, JobRoleFilteringRule.populated),
            ],
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="when_other_only_equal_zero",
            test_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
            ],
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="when_direct_care_and_man_and_reg_prof_equal_zero",
            test_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 0, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
            ],
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="when_location_has_null_worker_count",
            test_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_raw_data),
            ],
            expected_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_raw_data),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_raw_data),
            ],
        ),
        EstimateFilledPostsByJobRoleCleanUtilsTestCase(
            id="when_locations_have_different_results",
            test_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 0, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
            ],
            expected_data=[
                (1, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.admin_staff, JobGroupLabels.other, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.data_analyst, JobGroupLabels.other, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.supervisor, JobGroupLabels.managers, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (1, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, None, JobRoleFilteringRule.missing_direct_care_or_managers_and_profs),
                (2, MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.senior_care_worker, JobGroupLabels.direct_care, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.admin_staff, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.data_analyst, JobGroupLabels.other, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.first_line_manager, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.supervisor, JobGroupLabels.managers, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
                (2, MainJobRoleLabels.occupational_therapist, JobGroupLabels.regulated_professions, 1, JobRoleFilteringRule.populated),
            ],
        ),
    ] # fmt: skip


@dataclass
class EstimateFilledPostsByJobRoleCleanData:
    test_data = {
        IndCQC.id_per_locationid_import_date: [1] * 4,
        IndCQC.location_id: ["loc1"] * 4,
        IndCQC.cqc_location_import_date: [date(2024, 1, 1)] * 4,
        IndCQC.primary_service_type: [PrimaryServiceType.care_home_with_nursing] * 4,
        IndCQC.ascwds_filled_posts_dedup_clean: [10.0] * 4,
        IndCQC.estimate_filled_posts: [10.0] * 4,
        IndCQC.estimate_filled_posts_source: [
            EstimateFilledPostsSource.ascwds_pir_merged
        ]
        * 4,
        IndCQC.ascwds_job_role_counts: [1] * 4,
        IndCQC.main_job_role_clean_labelled: [
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.admin_staff,
            MainJobRoleLabels.deputy_manager,
            MainJobRoleLabels.registered_nurse,
        ],
        IndCQC.registered_manager_names: [["Manager 1", "Manager 2"]] * 4,
    }

    expected_data = {
        IndCQC.id_per_locationid_import_date: [1] * 4,
        IndCQC.id_per_locationid_import_date_job_role: [0, 1, 2, 3],
        IndCQC.location_id: ["loc1"] * 4,
        IndCQC.cqc_location_import_date: [date(2024, 1, 1)] * 4,
        IndCQC.primary_service_type: [PrimaryServiceType.care_home_with_nursing] * 4,
        IndCQC.estimate_filled_posts: [10.0] * 4,
        IndCQC.ascwds_job_role_counts: [1] * 4,
        IndCQC.main_job_role_clean_labelled: [
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.admin_staff,
            MainJobRoleLabels.deputy_manager,
            MainJobRoleLabels.registered_nurse,
        ],
        IndCQC.registered_manager_names: [["Manager 1", "Manager 2"]] * 4,
        IndCQC.job_role_filtering_rule: [JobRoleFilteringRule.populated] * 4,
    }

    metadata = {
        IndCQC.id_per_locationid_import_date: [1],
        IndCQC.provider_id: ["provider1"],
        IndCQC.brand_id: ["brand1"],
    }
