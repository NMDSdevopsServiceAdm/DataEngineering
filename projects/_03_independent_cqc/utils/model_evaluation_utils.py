import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)


def assign_location_folds(
    lf: pl.LazyFrame, location_column: str, n_folds: int, seed: int
) -> pl.LazyFrame:
    """
    Assign each location to one of `n_folds` cross-validation folds.

    Shuffled unique location IDs are dealt into folds in turn, so sizes differ by at most one.
    IDs are sorted first, so a seed always gives the same folds. Replaces any existing "fold".

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
    value_columns: list[str],
    partition_columns: list[str],
    date_column: str,
) -> pl.LazyFrame:
    """
    Measure jumpiness: each column's mean absolute change between consecutive dates, within
    each partition, in the column's own units.

    Consecutive means the next row present, so a gap in dates counts as one period. A null
    drops the changes either side of it, so columns with different nulls are scored on
    different rows.

    Args:
        lf (pl.LazyFrame): dataset containing the measured, partition and date columns
        value_columns (list[str]): the columns to measure, such as predictions
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
        for column in value_columns
    ).unpivot(
        variable_name=ModelEvaluation.column_name,
        value_name=ModelEvaluation.mean_period_to_period_change,
    )


def aggregate_totals_by_group(
    lf: pl.LazyFrame,
    predicted_column: str,
    actual_column: str,
    grouping_columns: list[str],
) -> pl.LazyFrame:
    """
    Sum predicted and known filled posts into groups.

    Only rows with a known value are used, so predicted and known totals cover the same rows.

    Args:
        lf (pl.LazyFrame): dataset containing the predicted, known and grouping columns
        predicted_column (str): the predicted filled posts column
        actual_column (str): the known filled posts column
        grouping_columns (list[str]): the columns defining each group

    Returns:
        pl.LazyFrame: a row per group, with its summed predicted and known filled posts and
            its "number_of_rows"
    """
    lf = lf.filter(pl.col(actual_column).is_not_null())

    return lf.group_by(grouping_columns).agg(
        pl.col(predicted_column).cast(pl.Float64).sum(),
        pl.col(actual_column).cast(pl.Float64).sum(),
        pl.len().alias(ModelEvaluation.number_of_rows),
    )


def score_group_totals(
    groups_lf: pl.LazyFrame,
    predicted_column: str,
    actual_column: str,
    by_columns: list[str] | None = None,
) -> pl.LazyFrame:
    """
    Score predicted against known group totals with R² and weighted absolute % error.

    R² is unweighted across groups. The error weights each group by its known total, so bigger
    groups count for more: sum(|predicted - known|) / sum(known).

    Args:
        groups_lf (pl.LazyFrame): a row per group, such as from `aggregate_totals_by_group`
        predicted_column (str): the predicted total column
        actual_column (str): the known total column
        by_columns (list[str] | None): columns to score separately by, such as the fold.
            Defaults to scoring all groups together.

    Returns:
        pl.LazyFrame: a row (per group), with its "r2" and "weighted_absolute_percentage_error"
    """
    by_columns = by_columns or []
    predicted = pl.col(predicted_column)
    actual = pl.col(actual_column)
    error = actual - predicted
    mean_actual = actual.mean()

    scores = [
        (1 - (error**2).sum() / ((actual - mean_actual) ** 2).sum()).alias(IndCQC.r2),
        (error.abs().sum() / actual.sum()).alias(
            ModelEvaluation.weighted_absolute_percentage_error
        ),
    ]

    return (
        groups_lf.group_by(by_columns).agg(scores)
        if by_columns
        else groups_lf.select(scores)
    )


def add_financial_year(lf: pl.LazyFrame, date_column: str) -> pl.LazyFrame:
    """
    Add the financial year (April to March) each date falls in.

    Args:
        lf (pl.LazyFrame): dataset containing the date column
        date_column (str): the date column

    Returns:
        pl.LazyFrame: dataset with "financial_year" added, as the calendar year it starts in
            (so 2023 is April 2023 to March 2024)
    """
    date = pl.col(date_column)
    january_to_march = (date.dt.month() < 4).cast(pl.Int32)

    return lf.with_columns(
        (date.dt.year() - january_to_march).alias(ModelEvaluation.financial_year)
    )


def score_rows(
    lf: pl.LazyFrame,
    predicted_column: str,
    actual_column: str,
    by_columns: list[str] | None = None,
) -> pl.LazyFrame:
    """
    Score predicted against known filled posts row by row, in posts.

    Only rows with a known value are scored. Bias is sum(predicted - known) / sum(known) and
    the weighted error is sum(|predicted - known|) / sum(known), so bigger locations count more.

    Args:
        lf (pl.LazyFrame): dataset containing the predicted, known and "by" columns
        predicted_column (str): the predicted filled posts column
        actual_column (str): the known filled posts column
        by_columns (list[str] | None): columns to score separately by, such as the fold or size
            band. Defaults to scoring all rows together.

    Returns:
        pl.LazyFrame: a row (per group), with its "number_of_rows", "bias",
            "weighted_absolute_percentage_error", "r2", "rmse" and the proportions of rows
            within ten and twenty five posts
    """
    by_columns = by_columns or []
    predicted = pl.col(predicted_column).cast(pl.Float64)
    actual = pl.col(actual_column).cast(pl.Float64)
    error = predicted - actual

    scores = [
        pl.len().alias(ModelEvaluation.number_of_rows),
        (error.sum() / actual.sum()).alias(ModelEvaluation.bias),
        (error.abs().sum() / actual.sum()).alias(
            ModelEvaluation.weighted_absolute_percentage_error
        ),
        (1 - (error**2).sum() / ((actual - actual.mean()) ** 2).sum()).alias(IndCQC.r2),
        (error**2).mean().sqrt().alias(IndCQC.rmse),
        (error.abs() <= 10)
        .mean()
        .alias(IndCQC.proportion_of_model_predictions_within_ten),
        (error.abs() <= 25)
        .mean()
        .alias(IndCQC.proportion_of_model_predictions_within_twenty_five),
    ]

    known_lf = lf.filter(pl.col(actual_column).is_not_null())

    return (
        known_lf.group_by(by_columns).agg(scores)
        if by_columns
        else known_lf.select(scores)
    )


def calculate_period_bias(
    lf: pl.LazyFrame,
    predicted_column: str,
    actual_column: str,
    period_column: str,
    by_columns: list[str],
) -> pl.LazyFrame:
    """
    Measure the bias of predicted against known filled posts in each period.

    Bias is (sum predicted - sum known) / sum known, over the rows with a known value in the
    period. Both sides cover the same rows, so a change in who is known doesn't show up as bias.

    Args:
        lf (pl.LazyFrame): dataset containing the predicted, known, period and "by" columns
        predicted_column (str): the predicted filled posts column
        actual_column (str): the known filled posts column
        period_column (str): the date column giving each period
        by_columns (list[str]): columns to measure separately by, such as service or fold

    Returns:
        pl.LazyFrame: a row per period (per group), with its summed predicted and known
            posts, "number_of_rows" and "bias"
    """
    period_totals_lf = aggregate_totals_by_group(
        lf, predicted_column, actual_column, [*by_columns, period_column]
    )

    return period_totals_lf.with_columns(
        (
            (pl.col(predicted_column) - pl.col(actual_column)) / pl.col(actual_column)
        ).alias(ModelEvaluation.bias)
    )


def fit_bias_slope_per_year(
    period_bias_lf: pl.LazyFrame,
    actual_column: str,
    period_column: str,
    by_columns: list[str],
) -> pl.LazyFrame:
    """
    Fit the weighted least squares slope of bias against time, in bias per year.

    Each period is weighted by its known total, so thin periods count for less. A group with
    one period has no slope, so filter on "number_of_periods".

    Args:
        period_bias_lf (pl.LazyFrame): a row per period (per group), such as from
            `calculate_period_bias`
        actual_column (str): the summed known posts column, used as the weight
        period_column (str): the date column giving each period
        by_columns (list[str]): columns to fit separately by, such as service or fold

    Returns:
        pl.LazyFrame: a row (per group), with its "bias_slope_per_year", "number_of_periods"
            and "minimum_rows_in_period"
    """
    days_per_year = 365.25
    years = pl.col(period_column).dt.epoch("d").cast(pl.Float64) / days_per_year
    bias = pl.col(ModelEvaluation.bias)
    weight = pl.col(actual_column)

    years_from_mean = years - (years * weight).sum() / weight.sum()
    bias_from_mean = bias - (bias * weight).sum() / weight.sum()

    return period_bias_lf.group_by(by_columns).agg(
        (
            (weight * years_from_mean * bias_from_mean).sum()
            / (weight * years_from_mean**2).sum()
        ).alias(ModelEvaluation.bias_slope_per_year),
        pl.len().alias(ModelEvaluation.number_of_periods),
        pl.col(ModelEvaluation.number_of_rows)
        .min()
        .alias(ModelEvaluation.minimum_rows_in_period),
    )
