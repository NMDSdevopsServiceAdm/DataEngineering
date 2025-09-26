from dataclasses import dataclass

import polars as pl

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)


@dataclass
class CleaningUtilsSchemas:
    align_dates_primary_schema = pl.Schema(
        [
            (AWPClean.ascwds_workplace_import_date, pl.Date()),
            (AWPClean.location_id, pl.String()),
        ]
    )
    align_dates_secondary_schema = pl.Schema(
        [
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.location_id, pl.String()),
        ]
    )
    primary_dates_schema = pl.Schema(
        [(AWPClean.ascwds_workplace_import_date, pl.Date())]
    )
    secondary_dates_schema = pl.Schema(
        [(CQCLClean.cqc_location_import_date, pl.Date())]
    )
    expected_aligned_dates_schema = pl.Schema(
        [
            (AWPClean.ascwds_workplace_import_date, pl.Date()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
        ]
    )
    expected_merged_dates_schema = pl.Schema(
        [
            (AWPClean.ascwds_workplace_import_date, pl.Date()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (
                AWPClean.location_id,
                pl.String(),
            ),  # Note - This was CQCL.location_id in pyspark verion, which has capital I in locationId. The returned df has lower case i in locationid. Pyspark is not case sensitive when selecting columns, but polars is.
        ]
    )
