from dataclasses import dataclass

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import MainJobRoleLabels


@dataclass
class EstimateIndCQCFilledPostsByJobRoleUtilsSchemas:
    new_columns_suffix = "_ascwds_counts"
    aggregate_ascwds_worker_job_roles_per_establishment_schema = pl.Schema(
        [
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_worker_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
        ]
    )
    expected_aggregate_ascwds_worker_job_roles_per_establishment_schema = pl.Schema(
        [
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_worker_import_date, pl.Date()),
            (MainJobRoleLabels.care_worker + new_columns_suffix, pl.UInt32()),
            (MainJobRoleLabels.senior_care_worker + new_columns_suffix, pl.UInt32()),
            (MainJobRoleLabels.registered_nurse + new_columns_suffix, pl.UInt32()),
        ]
    )

    estimates_df_before_join_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_workplace_import_date, pl.Date()),
        ]
    )
    worker_df_before_join_schema = (
        expected_aggregate_ascwds_worker_job_roles_per_establishment_schema
    )
    expected_join_worker_to_estimates_dataframe_schema = pl.Schema(
        list(estimates_df_before_join_schema.items())
        + [
            (MainJobRoleLabels.care_worker + new_columns_suffix, pl.UInt32()),
            (MainJobRoleLabels.senior_care_worker + new_columns_suffix, pl.UInt32()),
            (MainJobRoleLabels.registered_nurse + new_columns_suffix, pl.UInt32()),
        ]
    )
