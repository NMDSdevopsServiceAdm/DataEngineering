from dataclasses import dataclass

import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols


@dataclass
class WorkerTurnoverSchemas:
    filter_worker_schema = pl.Schema(
        [
            (AWKClean.location_id, pl.String()),
            (AWKClean.worker_id, pl.String()),
            (AWKClean.ascwds_worker_import_date, pl.Date()),
            (
                AWKClean.employment_status_clean_labelled,
                CatColType.EmploymentStatusCatType,
            ),
        ]
    )

    is_leaver_input_schema = pl.Schema(
        [
            (AWKClean.establishment_id, CatColType.EstablishmentCatType),
            (AWKClean.worker_id, pl.String()),
            (AWKClean.ascwds_worker_import_date, pl.Date()),
        ]
    )

    is_leaver_output_schema = pl.Schema(
        [
            *is_leaver_input_schema.items(),
            (HackCols.establishment_in_both_snapshots, pl.Boolean()),
            (HackCols.is_leaver, pl.Boolean()),
        ]
    )

    main_service_input_schema = pl.Schema(
        [(AWKClean.main_service_type_id, pl.String())]
    )

    main_service_type_schema = pl.Schema(
        [
            *main_service_input_schema.items(),
            (HackCols.main_service_type, CatColType.PrimaryServiceEnumType),
        ]
    )

    raw_worker_join_input_schema = pl.Schema(
        [
            (AWKClean.establishment_id, CatColType.EstablishmentCatType),
            (AWKClean.worker_id, pl.String()),
            (AWKClean.ascwds_worker_import_date, pl.Date()),
        ]
    )
