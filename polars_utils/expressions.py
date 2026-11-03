import polars as pl
import polars.selectors as cs

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome, Dormancy


def percentage_share(column: str | pl.Expr) -> pl.Expr:
    """Calculate the percentage share of a column across all values.

    If the column is a string, turn it into a column expression; otherwise, use as-is. Can be used in conjunction with `.group_by` and `.over` methods to get
    proportions within groups.

    Args:
        column(str|pl.Expr): a string or column expression on which to calculate

    Returns:
        pl.Expr: the percentage share of the column accross all values
    """
    col = pl.col(column) if isinstance(column, str) else column
    return col / col.sum()


def str_length_cols(columns: list[str]) -> list[pl.Expr]:
    """Creates a `<column>_length` expression of string lengths for each column.

    Categorical columns are cast to String first, as `.str` can't be used on them.

    Args:
        columns (list[str]): String or Categorical columns to get lengths for.

    Returns:
        list[pl.Expr]: One length expression per column.
    """
    return [
        pl.col(column).cast(pl.String).str.len_chars().alias(f"{column}_length")
        for column in columns
    ]


def is_care_home() -> pl.Expr:
    """Expression to identify care home records."""
    return pl.col(IndCQC.care_home) == CareHome.care_home


def is_not_care_home() -> pl.Expr:
    """Expression to identify non-care home records."""
    return pl.col(IndCQC.care_home) == CareHome.not_care_home


def is_dormant() -> pl.Expr:
    """Expression to identify dormant records."""
    return pl.col(IndCQC.dormancy) == Dormancy.dormant


def is_slv_job_role_column() -> cs.Selector:
    """
    Returns a Selector for starters/leavers/vacancies job role columns: columns
    that start with 'jr' and end with either 'strt', 'stop' or 'vacy'.
    """

    return cs.starts_with("jr") & cs.ends_with("strt", "stop", "vacy")
