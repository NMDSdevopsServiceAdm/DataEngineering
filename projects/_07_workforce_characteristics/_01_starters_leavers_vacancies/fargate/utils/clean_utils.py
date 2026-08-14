import polars as pl

import polars_utils.cleaning_utils as pUtils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols


def deduplicate_slv_over_time(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls out consecutive repeated starters/leavers/vacancies values over time.

    Args:
        lf (pl.LazyFrame): The merged SLV LazyFrame.

    Returns:
        pl.LazyFrame: The input LazyFrame with a deduplicated column added for each
            of starters, leavers and vacancies.
    """
    return pUtils.remove_repeated_values_over_time(
        lf,
        columns_to_clean=[SLVCols.starters, SLVCols.leavers, SLVCols.vacancies],
        partition_by_column=AWPClean.establishment_id,
        date_column=AWPClean.ascwds_workplace_import_date,
    )
