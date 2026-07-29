import polars as pl

from polars_utils.categorical_types import EstablishmentCatType, JobRoleCatType
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.slv_job_role_columns import SlvJobRoleColumns as SLVJR

REAL_JOB_ROLE_CODES = [
    "01",
    "04",
    "06",
    "07",
    "08",
    "09",
    "15",
    "16",
    "17",
    "43",
    "1001",
    "1002",
    "1003",
    "1004",
]


def _wide_job_role_schema(codes: list[str]) -> dict[str, pl.DataType]:
    """Builds a wide ASC-WDS job-role schema: grain columns plus
    jrNN{emp,strt,stop,vacy} for each given code."""
    schema = {
        AWPClean.establishment_id: pl.String,
        AWPClean.ascwds_workplace_import_date: pl.Date,
    }
    for code in codes:
        schema[f"jr{code}emp"] = pl.Int32
        schema[f"jr{code}strt"] = pl.Int32
        schema[f"jr{code}stop"] = pl.Int32
        schema[f"jr{code}vacy"] = pl.Int32
    return schema


class PivotJobRoleColsToRowsSchemas:
    synthetic_input_schema = _wide_job_role_schema(["02", "10", "20"])
    realistic_input_schema = _wide_job_role_schema(REAL_JOB_ROLE_CODES)
    partial_null_input_schema = _wide_job_role_schema(["05", "06"])
    all_null_input_schema = _wide_job_role_schema(["09"])
    zero_codes_input_schema = {
        AWPClean.establishment_id: pl.String,
        AWPClean.ascwds_workplace_import_date: pl.Date,
        "region": pl.String,
    }
    column_scope_input_schema = _wide_job_role_schema(["01"]) | {
        "region": pl.String,
        "sector": pl.String,
    }

    expected_schema = {
        AWPClean.establishment_id: EstablishmentCatType,
        AWPClean.ascwds_workplace_import_date: pl.Date,
        SLVJR.job_role_code: JobRoleCatType,
        SLVJR.employees: pl.Int16,
        SLVJR.starters: pl.Int16,
        SLVJR.leavers: pl.Int16,
        SLVJR.vacancies: pl.Int16,
    }
