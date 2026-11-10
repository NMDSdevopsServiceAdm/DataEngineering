from dataclasses import dataclass

import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.data_labels_columns import DataLabelsColumns as DLC
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
        [(AWPClean.main_service_id_clean, CatColType.MainServiceIdCatType)]
    )

    main_service_type_schema = pl.Schema(
        [
            *main_service_input_schema.items(),
            (HackCols.main_service_type, CatColType.PrimaryServiceEnumType),
        ]
    )

    pay_input_schema = pl.Schema(
        [
            (AWKClean.worker_id, pl.String()),
            (AWKClean.hourly_rate, pl.Float32()),
            (AWKClean.salary, pl.Float32()),
            (AWKClean.contracted_hours, pl.Float32()),
            (AWKClean.average_hours, pl.Float32()),
        ]
    )

    pay_output_schema = pl.Schema(
        [
            (AWKClean.worker_id, pl.String()),
            (HackCols.hours, pl.Float32()),
            (HackCols.hourly_pay, pl.Float32()),
        ]
    )

    nlw_input_schema = pl.Schema(
        [
            (AWKClean.ascwds_worker_import_date, pl.Date()),
            (f"{AWKClean.apprentice}_{DLC.label}s", pl.Categorical()),
            (AWKClean.age, pl.Float32()),
            (HackCols.hourly_pay, pl.Float32()),
        ]
    )

    nlw_output_schema = pl.Schema(
        [
            *nlw_input_schema.items(),
            (HackCols.nlw_hourly_rate, pl.Float32()),
            (HackCols.hourly_pay_minus_nlw, pl.Float32()),
        ]
    )

    latest_save_date_input_schema = pl.Schema(
        [
            (AWKClean.establishment_id, CatColType.EstablishmentCatType),
            (AWKClean.ascwds_worker_import_date, pl.Date()),
            (AWKClean.ascwds_worker_save_date, pl.Date()),
        ]
    )

    latest_save_date_schema = pl.Schema(
        [
            (AWKClean.establishment_id, CatColType.EstablishmentCatType),
            (AWKClean.ascwds_worker_import_date, pl.Date()),
            (HackCols.latest_save_date, pl.Date()),
        ]
    )

    active_input_schema = pl.Schema(
        [
            (AWKClean.establishment_id, CatColType.EstablishmentCatType),
            (AWKClean.worker_id, pl.String()),
            (AWKClean.ascwds_worker_import_date, pl.Date()),
            (HackCols.is_leaver, pl.Boolean()),
        ]
    )

    active_output_schema = pl.Schema(
        [
            *active_input_schema.items(),
            (HackCols.months_since_save_start, pl.Int32()),
            (HackCols.months_since_save_end, pl.Int32()),
            (HackCols.establishment_active, pl.Boolean()),
        ]
    )

    raw_worker_join_input_schema = pl.Schema(
        [
            (AWKClean.establishment_id, CatColType.EstablishmentCatType),
            (AWKClean.worker_id, pl.String()),
            (AWKClean.ascwds_worker_import_date, pl.Date()),
        ]
    )
