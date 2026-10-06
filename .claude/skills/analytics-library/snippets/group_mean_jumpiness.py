"""
Jumpiness of group trendlines: how much each group's mean moves between dates.

Requires `mean_period_to_period_change` from
`projects/_03_independent_cqc/utils/model_evaluation_utils.py`. Import it from a repo
checkout on `sys.path`; without one, inline its source and check it matches the repo's
version on synthetic data.
"""

import polars as pl

from projects._03_independent_cqc.utils.model_evaluation_utils import (
    mean_period_to_period_change,
)


def group_mean_jumpiness(
    lf: pl.LazyFrame,
    value_columns: list[str],
    group_columns: list[str],
    date_column: str,
) -> pl.LazyFrame:
    """
    Measure how jumpy each value column's group trendlines are.

    Values are averaged within each group and date first, so the score reflects the
    trendline rather than row-level noise. Averaging also leaves one row per group and
    date, so the metric's `.over()` runs on a small frame whatever the input size.

    Args:
        lf (pl.LazyFrame): dataset containing the value, group and date columns
        value_columns (list[str]): the columns to measure
        group_columns (list[str]): the columns defining each trendline; at least one
        date_column (str): the date column

    Returns:
        pl.LazyFrame: a row per value column, named in "column_name", with its
            "mean_period_to_period_change"
    """
    group_means_lf = lf.group_by(group_columns + [date_column]).agg(
        pl.col(value_columns).mean()
    )

    return mean_period_to_period_change(
        group_means_lf, value_columns, group_columns, date_column
    )
