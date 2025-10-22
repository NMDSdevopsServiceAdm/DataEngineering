from dataclasses import dataclass

import polars as pl

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
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

    worker_schema = pl.Schema(
        [
            (AWK.worker_id, pl.String()),
            (AWK.gender, pl.String()),
            (AWK.nationality, pl.String()),
        ]
    )
    expected_schema_with_new_columns = pl.Schema(
        [
            (AWK.worker_id, pl.String()),
            (AWK.gender, pl.String()),
            (AWK.nationality, pl.String()),
            ("gender_labels", pl.String()),
            ("nationality_labels", pl.String()),
        ]
    )
    expected_schema_with_new_1_column_and_1_custom_column_name = pl.Schema(
        [
            (AWK.worker_id, pl.String()),
            (AWK.gender, pl.String()),
            (AWK.nationality, pl.String()),
            ("custom_gender_column_name", pl.String()),
        ]
    )
    expected_schema_with_new_2_columns_and_2_custom_column_names = pl.Schema(
        [
            (AWK.worker_id, pl.String()),
            (AWK.gender, pl.String()),
            (AWK.nationality, pl.String()),
            ("custom_gender_column_name", pl.String()),
            ("custom_nationality_column_name", pl.String()),
        ]
    )
