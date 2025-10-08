from dataclasses import dataclass

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


@dataclass
class EstimateIndCQCFilledPostsByJobRoleUtilsSchemas:
    aggregate_ascwds_worker_job_roles_per_establishment_schema = pl.Schema(
        [
            (IndCQC.establishment_id, pl.String()),
            (IndCQC.ascwds_worker_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
        ]
    )
    expected_aggregate_ascwds_worker_job_roles_per_establishment_schema = pl.Schema(
        list(aggregate_ascwds_worker_job_roles_per_establishment_schema.items())
        + [(IndCQC.ascwds_job_role_counts, pl.UInt32())]
    )

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

    test_writing_to_parquet_schema = [
        (IndCQC.location_id, pl.String()),
        (IndCQC.establishment_id, pl.String()),
        (IndCQC.ascwds_workplace_import_date, pl.Date()),
        (IndCQC.main_job_role_clean_labelled, pl.String()),
        (IndCQC.ascwds_job_role_counts, pl.Int64()),
        (Keys.year, pl.String()),
        (Keys.month, pl.String()),
        (Keys.day, pl.String()),
        (Keys.import_date, pl.String()),
    ]
