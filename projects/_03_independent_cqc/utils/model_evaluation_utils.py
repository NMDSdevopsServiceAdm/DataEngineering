import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)


def assign_location_folds(
    lf: pl.LazyFrame, location_column: str, n_folds: int, seed: int
) -> pl.LazyFrame:
    """
    Assign each location to one of `n_folds` cross-validation folds, so all of its rows share
    a fold.

    Folds are dealt out in turn down a shuffled list of the unique location IDs, so fold sizes
    differ by at most one location. The IDs are sorted before shuffling, so the same seed gives
    the same folds whatever order the rows are in. Any existing "fold" column is replaced, such
    as when folds are assigned again with another seed.

    Args:
        lf (pl.LazyFrame): dataset containing the location ID
        location_column (str): the location ID
        n_folds (int): the number of folds
        seed (int): the seed for shuffling the locations

    Returns:
        pl.LazyFrame: dataset with the "fold" column added, numbered from 0
    """
    lf = lf.drop(ModelEvaluation.fold, strict=False)

    location_folds_lf = (
        lf.select(pl.col(location_column).unique())
        .sort(location_column)
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
    Flag the locations that never have a known value, on any row.

    Args:
        lf (pl.LazyFrame): dataset containing the known value column
        known_column (str): a column that's populated when the value is known
        location_column (str): the location ID

    Returns:
        pl.LazyFrame: dataset with the boolean "never_submitted" column added
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
    Measure how jumpy each column is: its mean absolute change from one period to the next, in
    the column's own units.

    Changes are only taken between consecutive dates within a partition (such as a location, or
    a location and job role), never from one partition's last date to the next partition's
    first.

    Args:
        lf (pl.LazyFrame): dataset containing the measured, partition and date columns
        columns (list[str]): the columns to measure, such as predictions
        partition_columns (list[str]): the columns that identify each timeline
        date_column (str): the date that orders each timeline

    Returns:
        pl.LazyFrame: one row per measured column, naming it in "column_name", with its
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
