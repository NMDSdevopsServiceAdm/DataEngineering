import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ShareModelColumns as ShareModel,
)


def aggregate_shares_by_cell(
    lf: pl.LazyFrame,
    predicted_columns: list[str],
    actual_columns: list[str],
    weight_column: str,
    cell_columns: list[str],
) -> pl.LazyFrame:
    """
    Aggregate predicted and actual shares into cells, weighting rows by their workers.

    Only rows with both shares are used, so compare models on the same rows. Sums are Float64,
    as a cell can sum many rows.

    Args:
        lf (pl.LazyFrame): dataset containing the share, weight and cell columns
        predicted_columns (list[str]): the predicted share columns
        actual_columns (list[str]): the actual share columns
        weight_column (str): each row's number of workers
        cell_columns (list[str]): the columns defining each cell

    Returns:
        pl.LazyFrame: a row per cell, with its weighted shares and total "cell_weight"
    """
    weight = pl.col(weight_column).cast(pl.Float64)

    lf = lf.filter(
        pl.all_horizontal(pl.col([*predicted_columns, *actual_columns]).is_not_null())
    )

    return lf.group_by(cell_columns).agg(
        *[
            ((pl.col(column).cast(pl.Float64) * weight).sum() / weight.sum()).alias(
                column
            )
            for column in [*predicted_columns, *actual_columns]
        ],
        weight.sum().alias(ShareModel.cell_weight),
    )


def score_cell_shares(
    cells_lf: pl.LazyFrame,
    predicted_columns: list[str],
    actual_columns: list[str],
    weight_column: str,
    by_columns: list[str] | None = None,
) -> pl.LazyFrame:
    """
    Score predicted against actual cell shares with weighted R² and mean absolute error.

    Bigger cells count for more, the error is in percentage points, and cells missing either
    share are skipped.

    Args:
        cells_lf (pl.LazyFrame): a row per cell, such as from `aggregate_shares_by_cell`
        predicted_columns (list[str]): the predicted share columns
        actual_columns (list[str]): the actual share columns, in the same order
        weight_column (str): each cell's weight
        by_columns (list[str] | None): columns to score separately by, such as the fold.
            Defaults to scoring all cells together.

    Returns:
        pl.LazyFrame: a row per share (and group), with its "r2" and "mean_absolute_error"
    """
    by_columns = by_columns or []
    weight = pl.col(weight_column).cast(pl.Float64)

    share_scores_lfs = []
    for predicted_column, actual_column in zip(
        predicted_columns, actual_columns, strict=True
    ):
        predicted = pl.col(predicted_column).cast(pl.Float64)
        actual = pl.col(actual_column).cast(pl.Float64)
        error = actual - predicted
        weighted_mean_actual = (weight * actual).sum() / weight.sum()
        scored_cells_lf = cells_lf.filter(
            predicted.is_not_null() & actual.is_not_null()
        )

        scores = [
            (
                1
                - (weight * error**2).sum()
                / (weight * (actual - weighted_mean_actual) ** 2).sum()
            ).alias(IndCQC.r2),
            (100 * (weight * error.abs()).sum() / weight.sum()).alias(
                ShareModel.mean_absolute_error
            ),
        ]
        scores_lf = (
            scored_cells_lf.group_by(by_columns).agg(scores)
            if by_columns
            else scored_cells_lf.select(scores)
        )

        share_scores_lfs.append(
            scores_lf.select(
                *by_columns,
                pl.lit(actual_column).alias(ShareModel.share),
                IndCQC.r2,
                ShareModel.mean_absolute_error,
            )
        )

    return pl.concat(share_scores_lfs)
