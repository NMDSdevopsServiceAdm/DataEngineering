from dataclasses import dataclass

import polars as pl

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


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
            (CQCLClean.import_date, pl.String()),
            (Keys.year, pl.String()),
            (Keys.month, pl.String()),
            (Keys.day, pl.String()),
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
