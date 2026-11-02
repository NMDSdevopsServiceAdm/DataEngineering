from dataclasses import dataclass

import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


@dataclass
class WorkerTurnoverSchemas:
    cleaned_worker_schema = pl.Schema(
        [
            (AWKClean.location_id, pl.String()),
            (AWKClean.establishment_id, pl.String()),
            (AWKClean.worker_id, pl.String()),
            (AWKClean.main_job_role_id, pl.String()),
            (AWKClean.employment_status, pl.String()),
            (AWKClean.ascwds_worker_import_date, pl.Date()),
            (AWKClean.main_job_role_clean, CatColType.MainJobRoleIdCatType),
            (AWKClean.main_job_role_clean_labelled, CatColType.JobRoleCatType),
            (AWKClean.employment_status_clean, CatColType.EmploymentStatusIdCatType),
            (
                AWKClean.employment_status_clean_labelled,
                CatColType.EmploymentStatusCatType,
            ),
        ]
    )
