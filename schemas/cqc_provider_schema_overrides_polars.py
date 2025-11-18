import polars as pl

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as ColNames,
)

POLARS_PROVIDER_SCHEMA_OVERRIDES = {
    ColNames.onspd_latitude: pl.Utf8,
    ColNames.onspd_longitude: pl.Utf8,
    ColNames.inspection_areas: pl.List(
        pl.Struct(
            {
                ColNames.inspection_area_id: pl.Utf8,
                ColNames.inspection_area_name: pl.Utf8,
                ColNames.inspection_area_type: pl.Utf8,
                ColNames.status: pl.Utf8,
                ColNames.end_date: pl.Utf8,
                ColNames.superseded_by: pl.List(pl.Utf8),
            }
        )
    ),
}
