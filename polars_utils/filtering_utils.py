import polars as pl

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
