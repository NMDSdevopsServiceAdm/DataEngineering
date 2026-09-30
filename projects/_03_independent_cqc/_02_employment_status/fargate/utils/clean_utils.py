import polars as pl

import projects._03_independent_cqc.utils.cleaning_utils as cleaningUtils
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def deduplicate_employment_status_counts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls the 5 employment status count columns where a workplace's counts are stale.

    Staleness is judged at workplace level (location_id + import date), not per
    job role. Each role's counts are compared with its own prior row, and all
    roles' rows for a workplace/date are kept if any role changed (a true repeat
    = no role changed).

    Args:
        lf (pl.LazyFrame): dataset containing the merged employment status count
            columns.

    Returns:
        pl.LazyFrame: dataset with 5 "<count>_dedup" columns added.
    """
    return cleaningUtils.remove_repeated_values_over_time_as_group(
        lf,
        columns_to_clean=[
            EmpStatus.permanent_count,
            EmpStatus.temporary_count,
            EmpStatus.bank_or_pool_count,
            EmpStatus.agency_count,
            EmpStatus.other_count,
        ],
        partition_by_columns=[IndCQC.location_id, IndCQC.published_job_role_label],
        date_column=IndCQC.cqc_location_import_date,
        workplace_columns=[IndCQC.location_id, IndCQC.cqc_location_import_date],
    )


def create_employment_status_percentage_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Deduplicates the 5 employment status count columns, then adds a
    percentage-share column per employment status.

    Counts are nulled only when no job role changed for that workplace and date
    (see `deduplicate_employment_status_counts`). Percentage-share columns are
    computed from the deduplicated counts, so a stale (nulled) row's percentages
    are also null rather than carrying forward a stale share.

    Args:
        lf (pl.LazyFrame): dataset containing the merged employment status count
            columns.

    Returns:
        pl.LazyFrame: dataset with 5 "<count>_dedup" and 5
            "emplstat_<status>_percentage" columns added.
    """
    lf = deduplicate_employment_status_counts(lf)

    lf = cleaningUtils.percentage_share_horizontal(
        lf,
        columns=[
            EmpStatus.permanent_count_dedup,
            EmpStatus.temporary_count_dedup,
            EmpStatus.bank_or_pool_count_dedup,
            EmpStatus.agency_count_dedup,
            EmpStatus.other_count_dedup,
        ],
        output_columns=[
            EmpStatus.permanent_percentage,
            EmpStatus.temporary_percentage,
            EmpStatus.bank_or_pool_percentage,
            EmpStatus.agency_percentage,
            EmpStatus.other_percentage,
        ],
    )

    return lf
