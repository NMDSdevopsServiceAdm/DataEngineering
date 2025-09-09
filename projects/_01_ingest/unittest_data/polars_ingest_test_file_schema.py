from dataclasses import dataclass

import polars as pl

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)

from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


@dataclass
class CQCLocationsSchema:
    clean_provider_id_column_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCL.provider_id, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    clean_registration_date_column_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCL.registration_date, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    clean_registration_date_column_output_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCL.registration_date, pl.String()),
            (Keys.import_date, pl.String()),
            (CQCLClean.imputed_registration_date, pl.String()),
        ]
    )
