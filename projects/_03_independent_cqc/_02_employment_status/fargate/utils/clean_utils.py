import polars as pl

import projects._03_independent_cqc.utils.cleaning_utils as cleaningUtils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import (
    SLVEmploymentStatusColumns as SLVEmpStatus,
)
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols


def create_employment_status_percentage_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Deduplicates the 5 employment status count columns as a single unit, then
    adds a percentage-share column per employment status.

    A row's counts are only treated as a repeat of the prior row in its
    location/job-role timeline (and nulled) if all 5 are unchanged; if any one
    changes, all 5 survive. Percentage-share columns are then computed from the
    deduplicated counts, so a repeated (nulled) row's percentages are also null
    rather than carrying forward a stale share.

    Args:
        lf (pl.LazyFrame): dataset containing the merged employment status count
            columns.

    Returns:
        pl.LazyFrame: dataset with 5 "<count>_dedup" and 5
            "emplstat_<status>_percentage" columns added.
    """
    lf = cleaningUtils.remove_repeated_values_over_time_as_group(
        lf,
        columns_to_clean=[
            SLVEmpStatus.permanent_count,
            SLVEmpStatus.temporary_count,
            SLVEmpStatus.bank_or_pool_count,
            SLVEmpStatus.agency_count,
            SLVEmpStatus.other_count,
        ],
        partition_by_columns=[IndCQC.location_id, SLVCols.published_job_role_label],
        date_column=IndCQC.cqc_location_import_date,
    )

    lf = cleaningUtils.percentage_share_horizontal(
        lf,
        columns=[
            SLVEmpStatus.permanent_count_dedup,
            SLVEmpStatus.temporary_count_dedup,
            SLVEmpStatus.bank_or_pool_count_dedup,
            SLVEmpStatus.agency_count_dedup,
            SLVEmpStatus.other_count_dedup,
        ],
        output_columns=[
            SLVEmpStatus.permanent_percentage,
            SLVEmpStatus.temporary_percentage,
            SLVEmpStatus.bank_or_pool_percentage,
            SLVEmpStatus.agency_percentage,
            SLVEmpStatus.other_percentage,
        ],
    )

    return lf
