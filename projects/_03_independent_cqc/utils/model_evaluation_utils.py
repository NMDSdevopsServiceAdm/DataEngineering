from typing import Callable

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


def add_fold_safe_rolling_average(
    lf: pl.LazyFrame,
    rolling_average_function: Callable[[pl.LazyFrame], pl.LazyFrame],
    input_columns: list[str],
    output_columns: list[str],
    fold_column: str,
    n_folds: int,
    key_columns: list[str],
) -> pl.LazyFrame:
    """
    Add a rolling average in which each fold's rows only get averages made from the other folds'
    values.

    For each fold, that fold's input values are blanked, `rolling_average_function` is run, and
    only that fold's rows are kept. So a tested fold's rows still get their group's average, even
    rows with no value of their own, but their own values don't feed into it. The kept outputs
    are joined back onto `lf`, so every row keeps its own input values and the row count doesn't
    change. Any `output_columns` already in `lf`, such as averages made from all folds, are
    replaced, rather than being kept alongside the fold-safe ones.

    This runs `rolling_average_function` once per fold, over every row each time. The folds run
    one after another rather than all at once, so memory peaks at one fold's working data rather
    than all of them. Pass a collected frame (as `df.lazy()`) so the steps that built `lf` aren't
    repeated for every fold.

    Args:
        lf (pl.LazyFrame): dataset containing the input, fold and key columns
        rolling_average_function (Callable[[pl.LazyFrame], pl.LazyFrame]): adds
            `output_columns` averaged from `input_columns`, keeping every row
        input_columns (list[str]): the columns the rolling average is made from
        output_columns (list[str]): the rolling average columns `rolling_average_function`
            adds
        fold_column (str): the fold of each row, numbered from 0
        n_folds (int): the number of folds
        key_columns (list[str]): the columns that identify each row

    Returns:
        pl.LazyFrame: dataset with the fold-safe `output_columns`
    """
    lf = lf.drop(output_columns, strict=False)

    fold_output_lfs = []
    for fold in range(n_folds):
        blanked_lf = lf.with_columns(
            pl.when(pl.col(fold_column) != fold).then(pl.col(column)).alias(column)
            for column in input_columns
        )
        fold_output_lfs.append(
            rolling_average_function(blanked_lf)
            .filter(pl.col(fold_column) == fold)
            .select(*key_columns, *output_columns)
        )

    return lf.join(
        pl.concat(fold_output_lfs, parallel=False), on=key_columns, how="left"
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
