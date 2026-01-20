from dataclasses import dataclass

import polars as pl

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


@dataclass
class PrepareJobRoleCountsUtilsSchemas:
    aggregate_ascwds_worker_job_roles_per_establishment_schema = pl.Schema(
        [
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_worker_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (Keys.year, pl.String()),
            (Keys.month, pl.String()),
            (Keys.day, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )
    expected_aggregate_ascwds_worker_job_roles_per_establishment_schema = pl.Schema(
        [
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_worker_import_date, pl.Date()),
            (Keys.year, pl.String()),
            (Keys.month, pl.String()),
            (Keys.day, pl.String()),
            (Keys.import_date, pl.String()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.ascwds_job_role_counts, pl.UInt32()),
        ]
    )


@dataclass
class FeaturesEngineeringUtilsSchemas:
    add_array_column_count_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.services_offered, pl.List(pl.String())),
        ]
    )
    expected_add_array_column_count_schema = pl.Schema(
        list(add_array_column_count_schema.items())
        + [(IndCQC.service_count, pl.UInt32())]
    )

    add_date_index_column_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
        ]
    )
    expected_add_date_index_column_schema = pl.Schema(
        list(add_date_index_column_schema.items())
        + [(IndCQC.cqc_location_import_date_indexed, pl.UInt32())]
    )

    cap_integer_at_max_value_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.service_count, pl.Int32()),
        ]
    )
    expected_cap_integer_at_max_value_schema = pl.Schema(
        list(cap_integer_at_max_value_schema.items())
        + [(IndCQC.service_count_capped, pl.Int32())]
    )

    col_with_categories: str = "categories"

    expand_encode_and_extract_features_when_not_array_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (col_with_categories, pl.String()),
        ]
    )
    expected_expand_encode_and_extract_features_when_not_array_schema = pl.Schema(
        list(expand_encode_and_extract_features_when_not_array_schema.items())
        + [
            ("has_A", pl.Int8()),
            ("has_B", pl.Int8()),
            ("has_C", pl.Int8()),
        ]
    )

    expand_encode_and_extract_features_when_is_array_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (col_with_categories, pl.List(pl.String())),
        ]
    )
    expected_expand_encode_and_extract_features_when_is_array_schema = pl.Schema(
        list(expand_encode_and_extract_features_when_is_array_schema.items())
        + [
            ("has_A", pl.Int8()),
            ("has_B", pl.Int8()),
            ("has_C", pl.Int8()),
        ]
    )

    group_rural_urban_sparse_categories_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.current_rural_urban_indicator_2011, pl.String()),
        ]
    )
    expected_group_rural_urban_sparse_categories_schema = pl.Schema(
        list(group_rural_urban_sparse_categories_schema.items())
        + [(IndCQC.current_rural_urban_indicator_2011_for_non_res_model, pl.String())]
    )

    add_squared_column_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date_indexed, pl.UInt32()),
        ]
    )
    expected_add_squared_column_schema = pl.Schema(
        list(add_squared_column_schema.items())
        + [(IndCQC.cqc_location_import_date_indexed_squared, pl.UInt32())]
    )

    select_and_filter_features_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            ("import_date", pl.String()),
            ("other_col", pl.String()),
            ("feature_1", pl.UInt32()),
            ("feature_2", pl.UInt32()),
            ("feature_3", pl.UInt32()),
            ("dependent", pl.Float64()),
        ]
    )
    expected_select_and_filter_features_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            ("dependent", pl.Float64()),
            ("feature_1", pl.Int32()),
            ("feature_2", pl.Int32()),
            ("feature_3", pl.Int32()),
            ("import_date", pl.String()),
        ]
    )


@dataclass
class ModelTrainingUtilsSchemas:
    split_train_test_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ascwds_filled_posts, pl.Float32()),
        ]
    )

    multiple_feature_cols = ["feature_1", "feature_2"]
    single_feature_col = ["feature_1"]
    dependent_col = "dependent"
    convert_dataframe_to_numpy_basic_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            ("feature_1", pl.Int32()),
            ("feature_2", pl.Int32()),
            ("dependent", pl.Float64()),
        ]
    )


@dataclass
class ModelUtilsSchemas:
    features_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            ("feature_1", pl.Int32()),
            ("feature_2", pl.Int32()),
        ]
    )

    expected_predictions_dataframe_schema = pl.Schema(
        list(features_schema.items())
        + [
            (IndCQC.prediction, pl.Float64()),
            (IndCQC.prediction_run_id, pl.String()),
        ]
    )


@dataclass
class ValidateModelsSchemas:
    get_expected_row_count_comapre_df_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.dormancy, pl.String()),
            ("feature 1", pl.String()),
            ("feature 2", pl.String()),
        ]
    )


@dataclass
class ValidateModel01FeaturesNonResWithDormancySchemas:
    validation_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.dormancy, pl.String()),
            (Keys.import_date, pl.String()),
            ("feature 1", pl.String()),
            ("feature 2", pl.String()),
            (IndCQC.imputed_filled_post_model, pl.Float32),
        ]
    )


@dataclass
class EstimateIndCqcFilledPostsByJobRoleUtilsSchemas:
    estimates_df_before_join_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_workplace_import_date, pl.Date()),
        ]
    )
    worker_df_before_join_schema = pl.Schema(
        [
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_worker_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.ascwds_job_role_counts, pl.Int64()),
        ]
    )
    expected_join_worker_to_estimates_dataframe_schema = pl.Schema(
        list(estimates_df_before_join_schema.items())
        + [
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.ascwds_job_role_counts, pl.Int64()),
        ]
    )


@dataclass
class MergeIndCQCSchemas:
    cqc_location_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.care_home, pl.String()),
            (CQCLClean.cqc_sector, pl.String()),
        ]
    )
    cqc_pir_schema = pl.Schema(
        [
            (CQCPIRClean.location_id, pl.String()),
            (CQCPIRClean.cqc_pir_import_date, pl.Date()),
            (CQCPIRClean.care_home, pl.String()),
            ("pir_col", pl.String()),
        ]
    )
    ascwds_workplace_schema = pl.Schema(
        [
            (AWPClean.location_id, pl.String()),
            (AWPClean.ascwds_workplace_import_date, pl.Date()),
            ("ascwds_col", pl.String()),
        ]
    )
    ct_non_res_schema = pl.Schema(
        [
            (CTNRClean.cqc_id, pl.String()),
            (CTNRClean.ct_non_res_import_date, pl.Date()),
            (CQCPIRClean.care_home, pl.String()),
            ("ct_non_res_col", pl.String()),
        ]
    )
    ct_care_home_schema = pl.Schema(
        [
            (CTCHClean.cqc_id, pl.String()),
            (CTCHClean.ct_care_home_import_date, pl.Date()),
            (CQCPIRClean.care_home, pl.String()),
            ("ct_care_home_col", pl.String()),
        ]
    )
    expected_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.cqc_sector, pl.String()),
            (IndCQC.cqc_pir_import_date, pl.Date()),
            ("pir_col", pl.String()),
            (IndCQC.ascwds_workplace_import_date, pl.Date()),
            ("ascwds_col", pl.String()),
            (IndCQC.ct_non_res_import_date, pl.Date()),
            ("ct_non_res_col", pl.String()),
            (IndCQC.ct_care_home_import_date, pl.Date()),
            ("ct_care_home_col", pl.String()),
        ]
    )


@dataclass
class MergeUtilsSchemas:
    clean_cqc_location_for_merge_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.cqc_sector, pl.String()),
            (CQCLClean.care_home, pl.String()),
            (CQCLClean.number_of_beds, pl.Int64()),
        ]
    )

    data_to_merge_without_care_home_col_schema = pl.Schema(
        [
            (AWPClean.location_id, pl.String()),
            (AWPClean.ascwds_workplace_import_date, pl.Date()),
            (AWPClean.establishment_id, pl.String()),
            (AWPClean.total_staff, pl.Int64()),
        ]
    )

    expected_merged_without_care_home_col_schema = pl.Schema(
        list(clean_cqc_location_for_merge_schema.items())
        + [
            (AWPClean.ascwds_workplace_import_date, pl.Date()),
            (AWPClean.establishment_id, pl.String()),
            (AWPClean.total_staff, pl.Int64()),
        ]
    )

    data_to_merge_with_care_home_col_schema = pl.Schema(
        [
            (CQCPIRClean.location_id, pl.String()),
            (CQCPIRClean.care_home, pl.String()),
            (CQCPIRClean.cqc_pir_import_date, pl.Date()),
            (CQCPIRClean.pir_people_directly_employed_cleaned, pl.Int64()),
        ]
    )

    expected_merged_with_care_home_col_schema = pl.Schema(
        list(clean_cqc_location_for_merge_schema.items())
        + [
            (CQCPIRClean.cqc_pir_import_date, pl.Date()),
            (CQCPIRClean.pir_people_directly_employed_cleaned, pl.Int64()),
        ]
    )
