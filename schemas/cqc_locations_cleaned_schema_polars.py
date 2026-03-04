import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CleanedColNames,
)

POLARS_CLEANED_LOCATIONS_SCHEMA = pl.Schema(
    [
        (CleanedColNames.cqc_location_import_date, pl.Date()),
        (CleanedColNames.location_id, pl.String()),
        (CleanedColNames.provider_id, pl.String()),
        (CleanedColNames.name, pl.String()),
        (CleanedColNames.registration_status, pl.String()),
        (CleanedColNames.registration_date, pl.Date()),
        (CleanedColNames.deregistration_date, pl.Date()),
        (CleanedColNames.type, pl.String()),
        (
            CleanedColNames.relationships,
            pl.List(
                pl.Struct(
                    {
                        CleanedColNames.related_location_id: pl.String(),
                        CleanedColNames.related_location_name: pl.String(),
                        CleanedColNames.type: pl.String(),
                        CleanedColNames.reason: pl.String(),
                    }
                )
            ),
        ),
        (CleanedColNames.number_of_beds, pl.Int32()),
        (CleanedColNames.dormancy, pl.String()),
        (CleanedColNames.imputed_registration_date, pl.Date()),
        (CleanedColNames.cqc_sector, pl.String()),
        (
            CleanedColNames.imputed_relationships,
            pl.List(
                pl.Struct(
                    {
                        CleanedColNames.related_location_id: pl.String(),
                        CleanedColNames.related_location_name: pl.String(),
                        CleanedColNames.type: pl.String(),
                        CleanedColNames.reason: pl.String(),
                    }
                )
            ),
        ),
        (CleanedColNames.related_location, pl.String()),
    ]
)
