import polars as pl

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.slv_job_role_columns import (
    SLVEmploymentStatusColumns as SLVEmpStatus,
)
from utils.column_values.categorical_column_values import EmploymentStatusLabels

GROUP_COLUMNS = [
    AWKClean.location_id,
    AWKClean.establishment_id,
    AWKClean.ascwds_worker_import_date,
    AWKClean.main_job_role_clean_labelled,
    AWKClean.employment_status_clean_labelled,
]

RESHAPED_GROUP_COLUMNS = [
    column
    for column in GROUP_COLUMNS
    if column != AWKClean.employment_status_clean_labelled
]

EMPLOYMENT_STATUS_LABEL_TO_COLUMN = {
    EmploymentStatusLabels.permanent: SLVEmpStatus.emplstat_perm_count,
    EmploymentStatusLabels.temporary: SLVEmpStatus.emplstat_temp_count,
    EmploymentStatusLabels.bank_or_pool: SLVEmpStatus.emplstat_bank_or_pool_count,
    EmploymentStatusLabels.agency: SLVEmpStatus.emplstat_agency_count,
    EmploymentStatusLabels.other: SLVEmpStatus.emplstat_other_count,
}


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
        pl.len().alias(SLVEmpStatus.employment_status_count)
    )


def reshape_employment_status_data(
    employment_status_summary_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Pivots employment status counts from one row per status to one row
    per group, with a count column per status.

    Args:
        employment_status_summary_lf (pl.LazyFrame): output of
            aggregate_employment_status_data, one row per group in
            GROUP_COLUMNS with an emplstat_count column.

    Returns:
        pl.LazyFrame: one row per group in RESHAPED_GROUP_COLUMNS, with 5
            emplstat_*_count columns (one per employment status label).
            Groups with no workers of a given status are 0 in that column.
    """
    return employment_status_summary_lf.pivot(
        on=AWKClean.employment_status_clean_labelled,
        on_columns=list(EMPLOYMENT_STATUS_LABEL_TO_COLUMN.keys()),
        index=RESHAPED_GROUP_COLUMNS,
        values=SLVEmpStatus.employment_status_count,
        aggregate_function="sum",
    ).rename(EMPLOYMENT_STATUS_LABEL_TO_COLUMN)
