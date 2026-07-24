from dataclasses import dataclass

import polars as pl

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_job_roles import (
    AscwdsWorkplaceJobRolesColumns as AWPJobRoles,
)

INDEX_SCHEMA = {
    AWPClean.establishment_id: pl.String,
    AWPClean.ascwds_workplace_import_date: pl.Date,
}


@dataclass
class Schemas:
    wide_two_job_roles_schema = {
        **INDEX_SCHEMA,
        "jr01emp": pl.Int32,
        "jr01strt": pl.Int32,
        "jr01stop": pl.Int32,
        "jr01vacy": pl.Int32,
        "jr45emp": pl.Int32,
        "jr45strt": pl.Int32,
        "jr45stop": pl.Int32,
        "jr45vacy": pl.Int32,
    }

    wide_one_job_role_all_null_schema = {
        **INDEX_SCHEMA,
        "jr01emp": pl.Int32,
        "jr01strt": pl.Int32,
        "jr01stop": pl.Int32,
        "jr01vacy": pl.Int32,
    }

    long_job_roles_schema = {
        **INDEX_SCHEMA,
        AWPJobRoles.job_role_code: pl.String,
        AWPJobRoles.employees: pl.Int16,
        AWPJobRoles.starters: pl.Int16,
        AWPJobRoles.leavers: pl.Int16,
        AWPJobRoles.vacancies: pl.Int16,
    }
