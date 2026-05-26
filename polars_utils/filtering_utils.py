import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def add_filtering_rule_column(
    lf: pl.LazyFrame,
    filter_rule_col_name: str,
    col_to_filter: str,
    populated_rule: str,
    missing_rule: str,
    categorical_type: pl.Categorical = None,
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
        categorical_type (pl.Categorical): If provided, creates a pl.Categorical col. Defaults to None.

    Returns:
        pl.LazyFrame: A LazyFrame with an additional column indicating
        whether data is present or missing.
    """
    if categorical_type:
        lf = lf.with_columns(
            pl.when(pl.col(col_to_filter).is_not_null())
            .then(pl.lit(populated_rule))
            .otherwise(pl.lit(missing_rule))
            .cast(categorical_type)
            .alias(filter_rule_col_name)
        )
    else:
        lf = lf.with_columns(
            pl.when(pl.col(col_to_filter).is_not_null())
            .then(pl.lit(populated_rule))
            .otherwise(pl.lit(missing_rule))
            .alias(filter_rule_col_name)
        )
    return lf


def update_filtering_rule(
    lf: pl.LazyFrame,
    filter_rule_col_name: str,
    raw_col_name: str,
    clean_col_name: str,
    populated_rule: str,
    new_rule_name: str,
    winsorized_rule: str = None,
    categorical_type: pl.Categorical = None,
) -> pl.LazyFrame:
    """
    Updates the text in the filtering rule column to reflect the change.

    This function updates the filtering rule in 2 cases:
    1) Where the rule is listed as "populated" but the cleaned data
       has been nulled or changed from the original value.
    2) If a winsorized process has already occurred, where the rule
       is listed as "winsorized" but the cleaned data has been nulled.

    Args:
        lf (pl.LazyFrame): A LazyFrame containing the raw column,
            cleaned column, and filtering rule column.
        filter_rule_col_name (str): The name of the filtering rule column.
        raw_col_name (str): The name of the original column with values.
        clean_col_name (str): The name of the cleaned column with values.
        populated_rule (str): The rule name when original data is being used.
        new_rule_name (str): The name of the new rule to add.
        winsorized_rule (str, optional): The rule name assigned if data
            has been winsorized (capped). Defaults to None.
        categorical_type (pl.Categorical): If provided, creates a pl.Categorical col. Defaults to None.

    Returns:
        pl.LazyFrame: A LazyFrame with the filtering rule column updated.
    """
    if not categorical_type:
        lf = lf.with_columns(
            pl.when(
                (
                    (
                        pl.col(clean_col_name).is_null()
                        | (pl.col(clean_col_name) != pl.col(raw_col_name))
                    )
                    & (pl.col(filter_rule_col_name) == populated_rule)
                )
            )
            .then(pl.lit(new_rule_name))
            .otherwise(pl.col(filter_rule_col_name))
            .alias(filter_rule_col_name)
        )
        if winsorized_rule:
            lf = lf.with_columns(
                pl.when(
                    pl.col(clean_col_name).is_null()
                    & (pl.col(filter_rule_col_name) == winsorized_rule)
                )
                .then(pl.lit(new_rule_name))
                .otherwise(pl.col(filter_rule_col_name))
                .alias(filter_rule_col_name)
            )
    else:
        lf = lf.with_columns(
            pl.when(
                (
                    (
                        pl.col(clean_col_name).is_null()
                        | (pl.col(clean_col_name) != pl.col(raw_col_name))
                    )
                    & (pl.col(filter_rule_col_name) == populated_rule)
                )
            )
            .then(pl.lit(new_rule_name))
            .otherwise(pl.col(filter_rule_col_name))
            .cast(categorical_type)
            .alias(filter_rule_col_name)
        )
        if winsorized_rule:
            lf = lf.with_columns(
                pl.when(
                    pl.col(clean_col_name).is_null()
                    & (pl.col(filter_rule_col_name) == winsorized_rule)
                )
                .then(pl.lit(new_rule_name))
                .otherwise(pl.col(filter_rule_col_name))
                .cast(categorical_type)
                .alias(filter_rule_col_name)
            )

    return lf
