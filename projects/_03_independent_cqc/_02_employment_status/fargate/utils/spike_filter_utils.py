import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

# Ticket 2110 spike: both the L1 and L2 prototypes read exactly these columns, so
# the shape comparison isn't skewed by L1's reshape copying extra columns 5 times.
SPIKE_INPUT_COLUMNS: list[str] = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.published_job_role_label,
    IndCQC.primary_service_type,
    IndCQC.estimate_filled_posts_by_job_role,
    EmpStatus.permanent_percentage,
    EmpStatus.temporary_percentage,
    EmpStatus.bank_or_pool_percentage,
    EmpStatus.agency_percentage,
    EmpStatus.other_percentage,
]


def filter_out_zero_job_role_filled_posts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Removes rows where the job role has zero estimated filled posts.

    Ticket 2110 spike: employment status shares are meaningless for a job role
    with no filled posts, so these rows (~48% of the reduced dataset) are dropped
    before imputation. Null estimates are kept explicitly, because a plain
    `!= 0` filter would also drop them.

    Args:
        lf (pl.LazyFrame): Data containing `estimate_filled_posts_by_job_role`.

    Returns:
        pl.LazyFrame: `lf` without the zero-filled-post rows.
    """
    metric = pl.col(IndCQC.estimate_filled_posts_by_job_role)
    return lf.filter(metric.is_null() | (metric != 0))
