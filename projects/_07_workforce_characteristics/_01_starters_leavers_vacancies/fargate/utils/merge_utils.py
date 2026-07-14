import warnings

import polars as pl
import polars.selectors as cs

import polars_utils.expressions as expr


def slv_cols_selector() -> cs.Selector:
    """
    Returns a Selector for columns that are:
     - numeric datatype
     - start with 'jr'
     - end with either 'emp', 'strt', 'stop' or 'vacy
     - do not end with 'temp'
    """

    return cs.numeric() & expr.is_slv_job_role_column()


def convert_ascwds_job_role_columns_to_rows():
    """
    Placeholder function to convert ASCWDS job role columns to rows."""
    pass


def join_datasets():
    """
    Placeholder function to join the datasets."""
    pass


def apply_employment_status_magic_numbers():
    """
    Placeholder function to apply employment status magic numbers."""
    pass
