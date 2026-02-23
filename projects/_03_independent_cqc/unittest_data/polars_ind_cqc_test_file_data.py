from dataclasses import dataclass
from datetime import date

import numpy as np

from projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator.difference_within_range import (
    ascwds_filled_posts_difference_within_range_source_description,
)
from projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator.total_staff_equals_worker_records import (
    ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description,
)
from utils.column_values.categorical_column_values import (
    AscwdsFilteringRule,
    CareHome,
    Dormancy,
    MainJobRoleLabels,
    PrimaryServiceType,
    Sector,
)


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
        ("1-001", date(2025, 1, 1), "20250101", "Y", 10, 5, 1, 100.0, 1),
        ("1-002", date(2025, 1, 2), "20250102", "N", None, 10, 1, 150.0, 1),
        ("1-003", date(2025, 1, 3), "20250103", "Y", 30, None, 0, 200.0, 1),
    ]
    expected_select_and_filter_features_rows = [
        ("1-001", date(2025, 1, 1), 1, 100.0, 10, 5, 1, "20250101"),
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
        (
            "1-001",
            date(2025, 1, 1),
            "N",
            None,
            ["activity 1"],
            12.0,
            ["service 1"],
            ["specialism 1"],
            "rui",
            "region",
            "Y",
            10,
            None,
        ),
        (
            "1-002",
            date(2025, 1, 1),
            "Y",
            "Y",
            ["activity 1"],
            12.0,
            ["service 1"],
            ["specialism 1"],
            "rui",
            "region",
            "Y",
            10,
            5,
        ),
        (
            "1-003",
            date(2025, 1, 1),
            "N",
            "Y",
            ["activity 1"],
            12.0,
            None,
            ["specialism 1"],
            "rui",
            "region",
            "Y",
            10,
            5,
        ),
        (
            "1-004",
            date(2025, 1, 1),
            "N",
            "Y",
            ["activity 1"],
            12.0,
            ["service 1"],
            ["specialism 1"],
            "rui",
            "region",
            "Y",
            10,
            5,
        ),
        (
            "1-005",
            date(2024, 1, 1),
            "N",
            None,
            ["activity 1"],
            12.0,
            ["service 1"],
            ["specialism 1"],
            "rui",
            "region",
            "Y",
            10,
            None,
        ),
    ]
    expected_get_expected_row_count_rows = 1


@dataclass
class ValidateModel01FeaturesData:
    validation_rows = [
        ("1-001", date(2025, 1, 1), "Y", "Y", "20250101", "feature", "feature", None),
    ]
    expected_get_expected_row_count_rows = 2


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
    # fmt: off
    expected_data = [
        ("1-001", date(2024, 1, 1), "Y", Sector.independent, date(2024, 1, 1), "pir_value", date(2024, 1, 1), "ascwds_value", date(2024, 1, 1), "ct_non_res_value", date(2024, 1, 1), "ct_care_home_value"),
        ("1-003", date(2024, 1, 1), "N", Sector.independent, date(2024, 1, 1), "pir_value", date(2024, 1, 1), "ascwds_value", date(2024, 1, 1), "ct_non_res_value", date(2024, 1, 1), "ct_care_home_value"),
    ]
    # fmt: on


@dataclass
class MergeUtilsData:
    # fmt: off
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
    ]
    # fmt: on

    data_to_merge_without_care_home_col_rows = [
        ("1-001", date(2024, 1, 1), "1", 1),
        ("1-003", date(2024, 1, 1), "3", 2),
        ("1-001", date(2024, 1, 5), "1", 3),
        ("1-001", date(2024, 1, 9), "1", 4),
        ("1-003", date(2024, 1, 9), "3", 5),
        ("1-003", date(2024, 3, 1), "4", 6),
    ]

    # fmt: off
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
        ("1-001", date(2024, 1, 1), Sector.independent, "Y", 10, date(2024, 1, 1), 10),
        ("1-002", date(2024, 1, 1), Sector.independent, "N", None, date(2024, 1, 1), 20),
        ("1-003", date(2024, 1, 1), Sector.independent, "N", None, date(2024, 1, 1), None),
        ("1-001", date(2024, 2, 1), Sector.independent, "Y", 10, date(2024, 2, 1), 1),
        ("1-002", date(2024, 2, 1), Sector.independent, "N", None, date(2024, 2, 1), 4),
        ("1-003", date(2024, 2, 1), Sector.independent, "N", None, date(2024, 2, 1), None),
        ("1-001", date(2024, 3, 1), Sector.independent, "Y", 10, date(2024, 2, 1), 1),
        ("1-002", date(2024, 3, 1), Sector.independent, "N", None, date(2024, 2, 1), 4),
        ("1-003", date(2024, 3, 1), Sector.independent, "N", None, date(2024, 2, 1), None),
    ]
    # fmt: on


@dataclass
class ValidateMergeIndCQCData:
    # fmt: off
    merged_ind_cqc_data_rows = [
        (
            "1-001", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1),
            "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1),
            "Y", 5, ["service"], PrimaryServiceType.care_home_only,
            date(2024, 1, 1), "cssr", "region",
            date(2024, 1, 1), "cssr", "region",
            "RUI", "lsoa", "msoa", 5,
            "estab_1", "org_1", 5, 5,"Y"
         ),

        (
            "1-002", date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1),
            "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1),
            "Y", 5, ["service"], PrimaryServiceType.care_home_only,
            date(2024, 1, 1), "cssr", "region",
            date(2024, 1, 1), "cssr", "region",
            "RUI", "lsoa", "msoa", 5,
            "estab_1", "org_1", 5, 5,"N"
         ),

        (
            "1-001", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1),
            "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1),
            "Y", 5, ["service"], PrimaryServiceType.care_home_only,
            date(2024, 1, 1), "cssr", "region",
            date(2024, 1, 1), "cssr", "region",
            "RUI", "lsoa", "msoa", 5,
            "estab_1", "org_1", 5, 5,"Y"
         ),

        (
            "1-002", date(2024, 1, 9), date(2024, 1, 1), date(2024, 1, 1),
            "Y", "name", "prov_1", Sector.independent, date(2024, 1, 1),
            "Y", 5, ["service"], PrimaryServiceType.care_home_only,
            date(2024, 1, 1), "cssr", "region",
            date(2024, 1, 1), "cssr", "region",
            "RUI", "lsoa", "msoa", 5,
            "estab_1", "org_1", 5, 5,"N"
         ),
    ]
    # fmt: on

    # fmt: off
    cqc_locations_cleaned_data_rows = [
        (date(2024, 1, 1), "1-001", Sector.independent, "Y", 10),
        (date(2024, 1, 1), "1-002", Sector.independent, "N", None),
        (date(2024, 2, 1), "1-001", Sector.independent, "Y", 10),
        (date(2024, 2, 1), "1-002", Sector.independent, "N", None),
    ]
    # fmt: on


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
class CalculateAscwdsFilledPostsDifferenceInRangeData:
    # fmt: off
    test_difference_within_range_rows = [
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

     # fmt: off
    expected_ascwds_filled_posts_rows = [
        ("1-000001", 0, None, None, None),
        ("1-000002", 500, 500, 500.0, ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description),
        ("1-000003", 10, None, None, None),
        ("1-000004", 23, 1, None, None),
        ("1-000005", None, 100, None, None),
        ("1-000006", 900, 600, None, None),
        ("1-000007", 12, 11, None, None),
        ("1-000008", 500, 475, None, None),
        ("1-000009", 10, 10, 8.0, "already populated"),
    ]


@dataclass
class CalculateAscwdsFilledPostsUtilsData:
    common_checks_rows = [
        ("1-000000001", 9, 2, None),
        ("1-000000002", 2, 2, 2.0),
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

    test_two_cols_are_equal_rows = [
        ("1-000000001", 9, 2, None),
        ("1-000000002", 2, 2, 2.0),
        ("1-000000003", 8, 8, 8.0),
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

    # fmt: off
    calculate_time_registered_same_day_rows = [
        ("1-0001", date(2025, 1, 1), date(2025, 1, 1)),
    ]
    # fmt: on
    # fmt: off
    expected_calculate_time_registered_same_day_rows = [
        ("1-0001", date(2025, 1, 1), date(2025, 1, 1), 1),
    ]

    # fmt: on
    # fmt: off
    calculate_time_registered_exact_months_apart_rows = [
        ("1-0001", date(2024, 2, 1), date(2024, 1, 1)),
        ("1-0002", date(2020, 1, 1), date(2019, 1, 1)),
    ]
    # fmt: on
    # fmt: off
    expected_calculate_time_registered_exact_months_apart_rows = [
        ("1-0001", date(2024, 2, 1), date(2024, 1, 1), 2),
        ("1-0002", date(2020, 1, 1), date(2019, 1, 1), 13),
    ]

    # fmt: on
    # fmt: off
    calculate_time_registered_one_day_less_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 1), date(2024, 12, 2)),
        ("1-0002", date(2025, 6, 8), date(2025, 1, 9)),
    ]
    # fmt: on
    # fmt: off
    expected_calculate_time_registered_one_day_less_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 1), date(2024, 12, 2), 1),
        ("1-0002", date(2025, 6, 8), date(2025, 1, 9), 5),
    ]
    # fmt: on
    # fmt: off

    calculate_time_registered_one_day_more_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 2), date(2024, 12, 1)),
        ("1-0002", date(2025, 6, 1), date(2025, 1, 31)),
    ]
    # fmt: on
    # fmt: off
    expected_calculate_time_registered_one_day_more_than_a_full_month_apart_rows = [
        ("1-0001", date(2025, 1, 2), date(2024, 12, 1), 2),
        ("1-0002", date(2025, 6, 1), date(2025, 1, 31), 5),
    ]
    # fmt: on
    # fmt: off
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
    ]
    # fmt: on
    # fmt: off
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
    ]
    # fmt: on
    remove_cqc_dual_registrations_when_carehome_and_asc_data_populated_rows = [
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
    expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_populated_rows = [
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

    remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_earlier_reg_date_rows = [
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
    expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_earlier_reg_date_rows = [
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

    remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_later_reg_date_rows = [
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
    expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_later_reg_date_rows = [
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

    remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_all_reg_dates_rows = [
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
    expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_missing_on_all_reg_dates_rows = [
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

    remove_cqc_dual_registrations_when_carehome_and_asc_data_different_on_all_reg_dates_rows = [
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
    expected_remove_cqc_dual_registrations_when_carehome_and_asc_data_different_on_all_reg_dates_rows = [
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

    remove_cqc_dual_registrations_when_carehome_and_registration_dates_the_same_rows = [
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
            "loc 2",
            date(2024, 1, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            None,
            None,
            date(2022, 1, 1),
        ),
    ]
    expected_remove_cqc_dual_registrations_when_carehome_and_registration_dates_the_same_rows = [
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

    remove_cqc_dual_registrations_when_locations_not_sorted_numerically = [
        (
            "loc 3",
            date(2024, 2, 1),
            "care home",
            "AB1 2CD",
            CareHome.care_home,
            10,
            10,
            date(2018, 1, 1),
        ),
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
    expected_remove_cqc_dual_registrations_when_locations_not_sorted_numerically = [
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

    remove_cqc_dual_registrations_when_non_res_rows = [
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
    expected_remove_cqc_dual_registrations_when_non_res_rows = (
        remove_cqc_dual_registrations_when_non_res_rows
    )


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
    add_filtering_column_rows = [
        ("loc 1", 10.0),
        ("loc 2", None),
    ]
    expected_add_filtering_column_rows = [
        ("loc 1", 10.0, AscwdsFilteringRule.populated),
        ("loc 2", None, AscwdsFilteringRule.missing_data),
    ]

    # fmt: off
    update_filtering_rule_populated_to_nulled_rows = [
        ("loc 1", 10.0, 10.0, AscwdsFilteringRule.populated),
        ("loc 2", 10.0, None, AscwdsFilteringRule.populated),
        ("loc 3", 10.0, None, AscwdsFilteringRule.missing_data),
    ]
    expected_update_filtering_rule_populated_to_nulled_rows = [
        ("loc 1", 10.0, 10.0, AscwdsFilteringRule.populated),
        ("loc 2", 10.0, None, AscwdsFilteringRule.contained_invalid_missing_data_code),
        ("loc 3", 10.0, None, AscwdsFilteringRule.missing_data),
    ]
    # fmt: on

    # fmt: off
    update_filtering_rule_populated_to_winsorized_rows = [
        ("loc 1", 10.0, 9.0, AscwdsFilteringRule.populated),
        ("loc 2", 10.0, 11.0, AscwdsFilteringRule.populated),
        ("loc 3", 10.0, 10.0, AscwdsFilteringRule.populated),
    ]
    expected_update_filtering_rule_populated_to_winsorized_rows = [
        ("loc 1", 10.0, 9.0, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("loc 2", 10.0, 11.0, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("loc 3", 10.0, 10.0, AscwdsFilteringRule.populated),
    ]
    # fmt: on

    # fmt: off
    update_filtering_rule_winsorized_to_nulled_rows = [
        ("loc 1", 10.0, 9.0, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("loc 2", 10.0, None, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
    ]
    expected_update_filtering_rule_winsorized_to_nulled_rows = [
        ("loc 1", 10.0, 9.0, AscwdsFilteringRule.winsorized_beds_ratio_outlier),
        ("loc 2", 10.0, None, AscwdsFilteringRule.contained_invalid_missing_data_code),
    ]
    # fmt: on

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
        -float("inf"): 65,
    }

    TEST_SIZE_BASED_FORWARD_FILL_DAYS = {
        -float("inf"): 1,
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
