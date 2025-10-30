import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

# TODO - finalise once all transformations are done
LOCATIONS_FLATTENED_SCHEMA = pl.Schema(
    [
        (CQCLClean.location_id, pl.String()),
        (CQCLClean.provider_id, pl.String()),
        (CQCLClean.name, pl.String()),
        (CQCLClean.postal_address_line1, pl.String()),
        (CQCLClean.postal_code, pl.String()),
        (CQCLClean.registration_status, pl.String()),
        (CQCLClean.registration_date, pl.Date()),
        (CQCLClean.deregistration_date, pl.Date()),
        (CQCLClean.type, pl.String()),
        (CQCLClean.care_home, pl.String()),
        (CQCLClean.number_of_beds, pl.Int32()),
        (CQCLClean.dormancy, pl.String()),
        (CQCLClean.cqc_location_import_date, pl.Date()),
        (CQCLClean.services_offered, pl.String()),
        (CQCLClean.specialisms_offered, pl.String()),
        (CQCLClean.regulated_activities_offered, pl.String()),
        (CQCLClean.registered_manager_names, pl.List(pl.String())),
        (Keys.import_date, pl.String()),
        (Keys.year, pl.String()),
        (Keys.month, pl.String()),
        (Keys.day, pl.String()),
    ]
)
