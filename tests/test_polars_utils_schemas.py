from dataclasses import dataclass

import polars as pl

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
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

    reduce_dataset_to_earliest_file_per_month_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.import_date, pl.String()),
            (Keys.year, pl.String()),
            (Keys.month, pl.String()),
            (Keys.day, pl.String()),
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
