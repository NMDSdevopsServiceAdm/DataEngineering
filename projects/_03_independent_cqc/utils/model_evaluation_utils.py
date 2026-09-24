import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)


def assign_location_folds(
    lf: pl.LazyFrame, location_column: str, n_folds: int, seed: int
) -> pl.LazyFrame:
    """
    Assign each location to one of `n_folds` cross-validation folds.

    Shuffled unique location IDs are dealt into folds in turn, so sizes differ by at most one.
    IDs are sorted as text first, so a seed always gives the same folds. Replaces any existing
    "fold".

    Args:
        lf (pl.LazyFrame): dataset containing the location ID
        location_column (str): the location ID
        n_folds (int): the number of folds
        seed (int): the shuffle seed

    Returns:
        pl.LazyFrame: dataset with "fold" added, numbered from 0
    """
    lf = lf.drop(ModelEvaluation.fold, strict=False)

    location_folds_lf = (
        lf.select(pl.col(location_column).unique())
        .sort(pl.col(location_column).cast(pl.String))
        .select(
            pl.col(location_column).shuffle(seed=seed),
            (pl.int_range(pl.len()) % n_folds)
            .cast(pl.UInt8)
            .alias(ModelEvaluation.fold),
        )
    )

    return lf.join(location_folds_lf, on=location_column, how="left")


def add_never_submitted_flag(
    lf: pl.LazyFrame, known_column: str, location_column: str
) -> pl.LazyFrame:
    """
    Flag locations that never have a known value, on any row.

    Args:
        lf (pl.LazyFrame): dataset containing the known value column
        known_column (str): a column populated only when the value is known
        location_column (str): the location ID

    Returns:
        pl.LazyFrame: dataset with boolean "never_submitted" added
    """
    return lf.with_columns(
        pl.col(known_column)
        .is_not_null()
        .any()
        .over(location_column)
        .not_()
        .alias(ModelEvaluation.never_submitted)
    )


def mean_period_to_period_change(
    lf: pl.LazyFrame,
    columns: list[str],
    partition_columns: list[str],
    date_column: str,
) -> pl.LazyFrame:
    """
    Measure jumpiness: each column's mean absolute change between consecutive dates, within
    each partition, in the column's own units. Changes next to a null are skipped, so compare
    models on the same rows.

    Args:
        lf (pl.LazyFrame): dataset containing the measured, partition and date columns
        columns (list[str]): the columns to measure, such as predictions
        partition_columns (list[str]): the columns identifying each timeline
        date_column (str): the date column

    Returns:
        pl.LazyFrame: a row per measured column, named in "column_name", with its
            "mean_period_to_period_change"
    """
    return lf.select(
        pl.col(column)
        .cast(pl.Float64)
        .diff()
        .over(partition_columns, order_by=date_column)
        .abs()
        .mean()
        .alias(column)
        for column in columns
    ).unpivot(
        variable_name=ModelEvaluation.column_name,
        value_name=ModelEvaluation.mean_period_to_period_change,
    )
