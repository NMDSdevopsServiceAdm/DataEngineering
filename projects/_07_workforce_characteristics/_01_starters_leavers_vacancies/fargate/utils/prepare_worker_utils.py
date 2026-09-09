import polars as pl

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.slv_worker_columns import SLVWorkerColumns as SLVWorker

GROUP_COLUMNS = [
    AWKClean.location_id,
    AWKClean.establishment_id,
    AWKClean.ascwds_worker_import_date,
    AWKClean.main_job_role_clean,
    AWKClean.main_job_role_clean_labelled,
    AWKClean.employment_status_clean,
    AWKClean.employment_status_clean_labelled,
]


def aggregate_employment_status_data(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Aggregates worker-level rows down to one row per employment status.

    Collapses cleaned ASC-WDS worker data to one row per location, import
    date, job role and employment status, with a count of the workers in
    each group. Relies on mainjrid_clean/emplstat_clean never being null, so
    no null-filtering is needed before grouping.

    Args:
        worker_lf (pl.LazyFrame): cleaned ASC-WDS worker LazyFrame.

    Returns:
        pl.LazyFrame: one row per group in GROUP_COLUMNS, with a new
            emplstat_count column counting workers in that group.
    """
    return worker_lf.group_by(GROUP_COLUMNS).agg(
        pl.len().alias(SLVWorker.employment_status_count)
    )


def reshape_employment_status_data(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Placeholder - will reshape employment status data. Currently a
    pass-through; real logic to follow in a later ticket.

    Args:
        worker_lf (pl.LazyFrame): cleaned ASC-WDS worker LazyFrame.

    Returns:
        pl.LazyFrame: worker_lf, unchanged.
    """
    return worker_lf
