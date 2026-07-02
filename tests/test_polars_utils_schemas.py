from dataclasses import dataclass

import polars as pl

from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    CategoricalColumnTypes as CatColType,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.data_labels_columns import DataLabelsColumns as DLC
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)


@dataclass
class CleaningUtilsSchemas:
    align_dates_primary_schema = pl.Schema(
        [
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.location_id, pl.String()),
        ]
    )
    align_dates_secondary_schema = pl.Schema(
        [
            (AWPClean.ascwds_workplace_import_date, pl.Date()),
            (AWPClean.establishment_id, pl.String()),
        ]
    )
    expected_merged_dates_schema = pl.Schema(
        [
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.location_id, pl.String()),
            (AWPClean.ascwds_workplace_import_date, pl.Date()),
        ]
    )

    labels_schema = pl.Schema(
        [
            (DLC.column_name, pl.String()),
            (DLC.code, pl.String()),
            (DLC.label, pl.String()),
        ]
    )

    col_to_date_string_schema = pl.Schema([("date_col", pl.String())])
    col_to_date_integer_schema = pl.Schema([("date_col", pl.Int64())])
    expected_col_to_date_schema = pl.Schema([("date_col", pl.Date())])
    expected_col_to_date_with_new_col_schema = pl.Schema(
        [("date_col", pl.String()), ("new_date_col", pl.Date())]
    )
    filled_posts_per_bed_ratio_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup, pl.Float64()),
            (IndCQC.number_of_beds, pl.Int64()),
            (IndCQC.care_home, pl.String()),
        ]
    )
    expected_filled_posts_per_bed_ratio_schema = pl.Schema(
        list(filled_posts_per_bed_ratio_schema.items())
        + [
            (IndCQC.filled_posts_per_bed_ratio, pl.Float64()),
        ]
    )

    reduce_dataset_to_earliest_file_per_month_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
        ]
    )

    create_banded_bed_count_column_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.care_home, pl.String()),
            (IndCQC.number_of_beds, pl.Int64()),
        ]
    )
    expected_create_banded_bed_count_column_schema = pl.Schema(
        [
            *create_banded_bed_count_column_schema.items(),
            (IndCQC.number_of_beds_banded, pl.Float64()),
        ]
    )


@dataclass
class RawDataAdjustmentsSchemas:
    locations_data_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            ("other_column", pl.String()),
        ]
    )


@dataclass
class FilteringUtilsSchemas:
    add_filtering_column_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
        ]
    )
    expected_add_filtering_column_schema = pl.Schema(
        list(add_filtering_column_schema.items())
        + [
            (IndCQC.ascwds_filtering_rule, pl.String()),
        ]
    )
    update_filtering_rule_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup, pl.Float64()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.ascwds_filtering_rule, pl.String()),
        ]
    )
    update_filtering_rule_schema_categorical = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ascwds_filled_posts_dedup, pl.Float64()),
            (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
            (IndCQC.ascwds_filtering_rule, CatColType.JobRoleFilteringRuleCatType),
        ]
    )
    returns_categorical_col_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.ascwds_job_role_counts, pl.Float64()),
        ]
    )
    expected_returns_categorical_col_schema = pl.Schema(
        list(returns_categorical_col_schema.items())
        + [
            (
                IndCQC.job_role_filtering_rule,
                CatColType.JobRoleFilteringRuleCatType,
            ),
        ]
    )
