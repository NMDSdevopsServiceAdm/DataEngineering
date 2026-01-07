from dataclasses import dataclass
from datetime import date

import numpy as np
import polars as pl

from utils.column_values.categorical_column_values import CareHome, MainJobRoleLabels


@dataclass
class PrepareJobRoleCountsUtilsData:
    aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_many_in_same_role_rows = [
        ("101", "101"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        (MainJobRoleLabels.care_worker, MainJobRoleLabels.care_worker),
        ("2025", "2025"),
        ("01", "01"),
        ("01", "01"),
        ("20250101", "20250101"),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_many_in_same_role_rows = [
        ("101", "101"),
        (date(2025, 1, 1), date(2025, 1, 1)),
        ("2025", "2025"),
        ("01", "01"),
        ("01", "01"),
        ("20250101", "20250101"),
        (MainJobRoleLabels.care_worker, MainJobRoleLabels.senior_care_worker),
        (2, 0),
    ]

    aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_multiple_import_dates_rows = [
        ("101", "101"),
        (date(2025, 1, 1), date(2025, 1, 2)),
        (MainJobRoleLabels.care_worker, MainJobRoleLabels.care_worker),
        ("2025", "2025"),
        ("01", "01"),
        ("01", "02"),
        ("20250101", "20250102"),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_multiple_import_dates_rows = [
        ("101", "101", "101", "101"),
        (date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 2)),
        ("2025", "2025", "2025", "2025"),
        ("01", "01", "01", "01"),
        ("01", "01", "02", "02"),
        ("20250101", "20250101", "20250102", "20250102"),
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
        ("2025", "2025"),
        ("01", "01"),
        ("01", "01"),
        ("20250101", "20250101"),
    ]
    expected_aggregate_ascwds_worker_job_roles_per_establishment_when_multiple_estabs_have_same_import_date_rows = [
        ("101", "101", "102", "102"),
        (date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1)),
        ("2025", "2025", "2025", "2025"),
        ("01", "01", "01", "01"),
        ("01", "01", "01", "01"),
        ("20250101", "20250101", "20250101", "20250101"),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
        ),
        (1, 0, 1, 0),
    ]


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

    add_squared_column_rows = [
        ("1-001", None),
        ("1-002", 0),
        ("1-003", 2),
        ("1-004", 4),
    ]
    expected_add_squared_column_rows = [
        ("1-001", None, None),
        ("1-002", 0, 0),
        ("1-003", 2, 4),
        ("1-004", 4, 16),
    ]

    select_and_filter_features_rows = [
        ("1-001", date(2025, 1, 1), "20250101", "Y", 10, 5, 1, 100.0),
        ("1-002", date(2025, 1, 2), "20250102", "N", None, 10, 1, 150.0),
        ("1-003", date(2025, 1, 3), "20250103", "Y", 30, None, 0, 200.0),
    ]
    expected_select_and_filter_features_rows = [
        ("1-001", date(2025, 1, 1), 100.0, 10, 5, 1, "20250101"),
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

    predictions = pl.Series([10.5, 11.0, 12.3])
    mismatch_predictions = pl.Series([10.5, 11.0])

    expected_predictions_dataframe_rows = [
        ("1-001", date(2025, 1, 1), 10.5, "model_A_v1.2.0_r7"),
        ("1-002", date(2025, 1, 1), 11.0, "model_A_v1.2.0_r7"),
        ("1-003", date(2025, 1, 1), 12.3, "model_A_v1.2.0_r7"),
    ]


@dataclass
class EstimateIndCqcFilledPostsByJobRoleUtilsData:
    estimates_df_before_join_rows = [
        (
            "1-001",
            "1-001",
            "1-002",
        ),
        (
            "1001",
            "1001",
            "1002",
        ),
        (date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1)),
    ]
    worker_df_before_join_rows = [
        ("1001", "1001", "1002"),
        (date(2025, 1, 1), date(2025, 1, 1), date(2025, 1, 1)),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_management,
            MainJobRoleLabels.care_worker,
        ),
        (10, 5, 20),
    ]
    expected_join_worker_to_estimates_dataframe_rows = [
        (
            "1-001",
            "1-001",
            "1-001",
            "1-002",
        ),
        (
            "1001",
            "1001",
            "1001",
            "1002",
        ),
        (
            date(2025, 1, 1),
            date(2025, 1, 1),
            date(2025, 1, 2),
            date(2025, 1, 1),
        ),
        (
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_management,
            None,
            MainJobRoleLabels.care_worker,
        ),
        (10, 5, None, 20),
    ]
