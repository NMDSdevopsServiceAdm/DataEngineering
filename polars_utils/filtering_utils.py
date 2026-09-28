from datetime import date

import polars as pl
from dateutil.relativedelta import relativedelta

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def add_filtering_rule_column(
    lf: pl.LazyFrame,
    filter_rule_col_name: str,
    col_to_filter: str,
    populated_rule: str,
    missing_rule: str,
    categorical_type: pl.Categorical | None = None,
) -> pl.LazyFrame:
    """
    Adds a column which flags if data is present or missing.

    This function adds a new column which identifies if the `col_to_filter`
    is "populated" or "missing data".

    Args:
        lf (pl.LazyFrame): A LazyFrame containing the `col_to_filter`
            before any filters have been applied to the column.
        filter_rule_col_name (str): The name of the new filtering rule column.
        col_to_filter (str): The name of the column to check for nulls.
        populated_rule (str): The value to assign when data is present.
        missing_rule (str): The value to assign when data is null.
        categorical_type (pl.Categorical | None, optional): If provided, creates a pl.Categorical col. Defaults to None.

    Returns:
        pl.LazyFrame: A LazyFrame with an additional column indicating
        whether data is present or missing.
    """
    expr = (
        pl.when(pl.col(col_to_filter).is_not_null())
        .then(pl.lit(populated_rule))
        .otherwise(pl.lit(missing_rule))
    )

    if categorical_type:
        expr = expr.cast(categorical_type)

    return lf.with_columns(expr.alias(filter_rule_col_name))


def update_filtering_rule(
    lf: pl.LazyFrame,
    filter_rule_col_name: str,
    raw_col_name: str,
    clean_col_name: str,
    populated_rule: str,
    new_rule_name: str,
    winsorized_rule: str | None = None,
    categorical_type: pl.Categorical | None = None,
) -> pl.LazyFrame:
    """
    Updates the text in the filtering rule column to reflect the change.

    This function updates the filtering rule in 2 cases:
    1) Where the cleaned data has been nulled but the rule says "populated" or "winsorized"
    2) Where the cleaned data has been changed (winsorized) but the rule says "populated"

    Args:
        lf (pl.LazyFrame): A LazyFrame containing the raw column,
            cleaned column, and filtering rule column.
        filter_rule_col_name (str): The name of the filtering rule column.
        raw_col_name (str): The name of the original column with values.
        clean_col_name (str): The name of the cleaned column with values.
        populated_rule (str): The rule name when original data is being used.
        new_rule_name (str): The name of the new rule to add.
        winsorized_rule (str | None, optional): The rule name assigned if data
            has been winsorized (capped). Defaults to None.
        categorical_type (pl.Categorical | None, optional): If provided, creates a pl.Categorical col. Defaults to None.

    Returns:
        pl.LazyFrame: A LazyFrame with the filtering rule column updated.
    """
    clean_col_is_null = pl.col(clean_col_name).is_null()
    clean_col_has_changed = pl.col(clean_col_name) != pl.col(raw_col_name)
    rule_is_populated = pl.col(filter_rule_col_name) == populated_rule
    rule_is_winsorised = pl.col(filter_rule_col_name) == winsorized_rule

    expr = (
        pl.when(
            (clean_col_is_null & (rule_is_populated | rule_is_winsorised))
            | (clean_col_has_changed & rule_is_populated)
        )
        .then(pl.lit(new_rule_name))
        .otherwise(pl.col(filter_rule_col_name))
    )

    if categorical_type is not None:
        expr = expr.cast(categorical_type)

    return lf.with_columns(expr.alias(filter_rule_col_name))


def lookback_cap_filter_expr(
    today: date | None = None,
    fy_start_month: int = 4,
    lookback_fy_years: int = 12,
    cutoff_date: date | None = None,
) -> pl.Expr:
    """
    Build a Polars expression capping a dataset to a historic financial-year lookback window.

    Retains roughly `lookback_fy_years` years of history, measured back from the start of
    the current financial year, plus an extra 6-month lead-in so rolling/windowed
    calculations still have a full window at the earliest date this retains.

    Args:
        today (date | None): Reference date for financial year boundaries. Defaults to today.
        fy_start_month (int): Month the financial year starts in. Defaults to April.
        lookback_fy_years (int): Financial years to retain before the current one.
        cutoff_date (date | None): Optional hard floor below the lookback window.

    Returns:
        pl.Expr: Boolean expression for `.filter()`.
    """
    today: date = today or date.today()

    fy_year = today.year if today.month >= fy_start_month else today.year - 1

    rolling_window_warm_up_months = 6
    lookback_start = date(
        fy_year - lookback_fy_years, fy_start_month, 1
    ) - relativedelta(months=rolling_window_warm_up_months)

    dt = pl.col(IndCQC.cqc_location_import_date)

    expr = dt >= lookback_start

    if cutoff_date is not None:
        expr = expr & (dt >= cutoff_date)

    return expr


def earliest_file_per_month_filter_expr(
    date_col: str = IndCQC.cqc_location_import_date,
) -> pl.Expr:
    """
    Build a Polars expression selecting only the earliest-dated row(s) of each calendar month.

    This identifies the earliest date within each (year, month) group and keeps only rows
    matching that date, reducing a dataset carrying multiple files per month down to one.

    Returning an expression rather than a filtered LazyFrame lets callers attach it
    directly to a `.filter()` chain alongside other predicates (e.g. lookback_cap_filter_expr),
    keeping the query lazy end-to-end.

    Args:
        date_col (str): Name of the date column to reduce on. Defaults to the CQC
            location import date; datasets keyed on a different date column (e.g. the
            SLV pipeline's ASCWDS workplace import date) pass their own.

    Returns:
        pl.Expr: A Polars boolean expression that can be used inside `.filter()` to select
            rows whose date matches the minimum date within their (year, month) group.
    """
    dt = pl.col(date_col)

    return dt == dt.min().over(dt.dt.year(), dt.dt.month())


def not_null_filter_expr(column: str) -> pl.Expr:
    """
    Build a Polars expression selecting only rows where `column` is not null.

    Returning an expression rather than a filtered LazyFrame lets callers attach it
    directly to a `.filter()` chain alongside other predicates (e.g.
    lookback_cap_filter_expr), keeping the query lazy end-to-end.

    Args:
        column (str): Name of the column to check for non-null values.

    Returns:
        pl.Expr: A Polars boolean expression that can be used inside `.filter()` to
            select rows where `column` is populated.
    """
    return pl.col(column).is_not_null()
