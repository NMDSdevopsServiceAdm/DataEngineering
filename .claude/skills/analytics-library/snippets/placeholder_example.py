"""
PLACEHOLDER: shows the shape of a snippet. Delete it, and its test
`tests/skills/test_placeholder_example.py`, when the first real snippet is added.

State what the function is for and any repo functions it imports; without a repo checkout
on `sys.path` they must be inlined and checked against the repo's version.
"""

import polars as pl


def placeholder_example(
    lf: pl.LazyFrame, value_columns: list[str], group_columns: list[str]
) -> pl.LazyFrame:
    """
    Take the mean of each value column within each group.

    Args:
        lf (pl.LazyFrame): dataset containing the value and group columns
        value_columns (list[str]): the columns to average
        group_columns (list[str]): the columns defining each group

    Returns:
        pl.LazyFrame: a row per group, with the mean of each value column
    """
    return lf.group_by(group_columns).agg(pl.col(value_columns).mean())
