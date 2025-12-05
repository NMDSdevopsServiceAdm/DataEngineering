from dataclasses import dataclass

import polars as pl

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

    select_and_filter_features_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            ("import_date", pl.Int64()),
            ("other_col", pl.String()),
            ("feature_1", pl.UInt32()),
            ("feature_2", pl.UInt32()),
            ("feature_3", pl.UInt32()),
            ("dependent", pl.UInt32()),
        ]
    )
    expected_select_and_filter_features_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.DataType()),
            ("dependent", pl.UInt32()),
            ("feature_1", pl.UInt32()),
            ("feature_2", pl.UInt32()),
            ("feature_3", pl.UInt32()),
            ("import_date", pl.String()),
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
