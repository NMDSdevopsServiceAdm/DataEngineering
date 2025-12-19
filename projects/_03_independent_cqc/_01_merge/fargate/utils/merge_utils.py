from typing import Optional

import polars as pl

from polars_utils.cleaning_utils import add_aligned_date_column
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)


def join_data_into_cqc_lf(
    cqc_df: pl.LazyFrame,
    join_df: pl.LazyFrame,
    join_location_id_col: str,
    join_import_date_col: str,
    join_care_home_col: Optional[str] = None,
) -> pl.LazyFrame:
    """
    Function to join a data file into the CQC locations data set.

    Some data needs to be matched on the care home column as well as location ID and import date, so
    there is an option to specify that. Other data doesn't require that match, so this option defaults
    to None (not required for matching).

    Args:
        cqc_df (LazyFrame): The CQC location LazyFrame.
        join_df (LazyFrame): The LazyFrame to join in.
        join_location_id_col (str): The name of the location ID column in the LazyFrame to join in.
        join_import_date_col (str): The name of the import date column in the LazyFrame to join in.
        join_care_home_col (Optional[str]): The name of the care home column if required for the join.

    Returns:
        LazyFrame: Original CQC locations LazyFrame with the second LazyFrame joined in.
    """
    cqc_df_with_join_import_date = add_aligned_date_column(
        cqc_df,
        join_df,
        CQCLClean.cqc_location_import_date,
        join_import_date_col,
    )

    join_df = join_df.rename({join_location_id_col: CQCLClean.location_id})

    cols_to_join_on = [join_import_date_col, CQCLClean.location_id]
    if join_care_home_col:
        cols_to_join_on = cols_to_join_on + [join_care_home_col]

    cqc_df_with_join_data = cqc_df_with_join_import_date.join(
        join_df,
        on=cols_to_join_on,
        how="left",
    )

    return cqc_df_with_join_data
