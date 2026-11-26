import pandas as pd
import polars as pl
from sklearn.metrics import brier_score_loss, roc_auc_score

from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols

WORKER_KEY_COLUMNS = [AWKClean.establishment_id, AWKClean.worker_id]
# Placeholder bucket: replace with the bucket holding the answers dataset in the
# hackathon AWS account.
DEFAULT_ANSWERS_SOURCE = (
    "s3://<hackathon-bucket>/domain=98_hackathon/dataset=worker_turnover_answers/"
)


def check_predictions(predictions_df: pl.DataFrame, answers_df: pl.DataFrame) -> None:
    """Checks predictions are complete and valid for scoring.

    Predictions must have exactly the worker key columns and
    `predicted_probability`, with one non-null probability between 0 and 1 for
    every worker in the predict dataset, and no other workers.

    Args:
        predictions_df (pl.DataFrame): Predictions, one row per worker.
        answers_df (pl.DataFrame): Every worker in the predict dataset, one row
            each, with the worker key columns.

    Raises:
        ValueError: If the predictions fail any check, or the answers have
            duplicated workers, with the number of rows affected and, for
            workers, the first few examples.
    """
    prediction_columns = WORKER_KEY_COLUMNS + [HackCols.predicted_probability]
    example_count = 5

    missing_columns = [c for c in prediction_columns if c not in predictions_df.columns]
    if missing_columns:
        raise ValueError(f"Predictions are missing required columns: {missing_columns}")

    unexpected_columns = [
        c for c in predictions_df.columns if c not in prediction_columns
    ]
    if unexpected_columns:
        raise ValueError(
            f"Predictions have unexpected columns: {unexpected_columns}. "
            f"Expected exactly {prediction_columns}"
        )

    predicted_keys_df = predictions_df.select(
        pl.col(WORKER_KEY_COLUMNS).cast(pl.String)
    )
    answer_keys_df = answers_df.select(pl.col(WORKER_KEY_COLUMNS).cast(pl.String))

    duplicated_answers_count = (
        answer_keys_df.filter(answer_keys_df.is_duplicated()).unique().height
    )
    if duplicated_answers_count:
        raise ValueError(
            f"Answers have {duplicated_answers_count} duplicated workers, "
            "so scores would be distorted. Report this to the organisers"
        )

    probability = pl.col(HackCols.predicted_probability).cast(pl.Float64, strict=False)

    null_key_count = predicted_keys_df.filter(
        pl.any_horizontal(pl.all().is_null())
    ).height
    if null_key_count:
        raise ValueError(f"Predictions have {null_key_count} null worker keys")

    invalid_probability_count = predictions_df.filter(
        probability.is_null() | probability.is_nan()
    ).height
    if invalid_probability_count:
        raise ValueError(
            f"Predictions have {invalid_probability_count} null, NaN or non-numeric "
            "probabilities"
        )

    out_of_range_count = predictions_df.filter(
        (probability < 0) | (probability > 1)
    ).height
    if out_of_range_count:
        raise ValueError(
            f"Predictions have {out_of_range_count} probabilities outside 0 to 1"
        )

    duplicated_df = predicted_keys_df.filter(predicted_keys_df.is_duplicated()).unique()
    if duplicated_df.height:
        raise ValueError(
            f"Predictions have {duplicated_df.height} duplicated workers, "
            f"e.g. {duplicated_df.head(example_count).rows()}"
        )

    missing_df = answer_keys_df.join(
        predicted_keys_df, on=WORKER_KEY_COLUMNS, how="anti"
    )
    if missing_df.height:
        raise ValueError(
            f"Predictions are missing {missing_df.height} workers from the predict dataset, "
            f"e.g. {missing_df.head(example_count).rows()}"
        )

    unknown_df = predicted_keys_df.join(
        answer_keys_df, on=WORKER_KEY_COLUMNS, how="anti"
    )
    if unknown_df.height:
        raise ValueError(
            f"Predictions have {unknown_df.height} workers not in the predict dataset, "
            f"e.g. {unknown_df.head(example_count).rows()}"
        )


def calculate_scores(
    predictions_df: pl.DataFrame, answers_df: pl.DataFrame
) -> dict[str, float | int]:
    """Calculates worker-level and workplace-level scores.

    Scores workers with a known `is_leaver`.
    Worker scores are AUC and Brier score, over all scored workers. The workplace
    score is the mean, over establishments with at least 10 scored workers, of
    the absolute difference between predicted leave rate (summed probabilities /
    scored headcount) and observed leave rate. Smaller workplaces are left out
    as their observed rates are mostly noise. Frames are one row per worker, so
    small enough for memory.

    Args:
        predictions_df (pl.DataFrame): Checked predictions, one row per worker.
        answers_df (pl.DataFrame): Answers with `is_leaver`.

    Returns:
        dict[str, float | int]: `worker_auc`, `worker_brier`, `workplace_mae`,
            `n_workers` (scored workers) and `n_workplaces` (establishments
            included in `workplace_mae`).

    Raises:
        ValueError: If the scored workers are not both leavers and stayers, as
            AUC is undefined, or no establishment has at least 10 scored workers.
    """
    min_workplace_workers = 10

    probability = pl.col(HackCols.predicted_probability)
    is_leaver = pl.col(HackCols.is_leaver)

    scored_df = (
        answers_df.filter(is_leaver.is_not_null())
        .select(pl.col(WORKER_KEY_COLUMNS).cast(pl.String), is_leaver)
        .join(
            predictions_df.select(
                pl.col(WORKER_KEY_COLUMNS).cast(pl.String),
                probability.cast(pl.Float64),
            ),
            on=WORKER_KEY_COLUMNS,
            how="inner",
        )
    )
    if scored_df.get_column(HackCols.is_leaver).n_unique() < 2:
        raise ValueError(
            "Scoring needs both leavers and stayers among the scored workers"
        )

    predicted_rate = probability.sum() / pl.len()
    observed_rate = is_leaver.mean()
    workplace_error_df = (
        scored_df.filter(
            pl.len().over(AWKClean.establishment_id) >= min_workplace_workers
        )
        .group_by(AWKClean.establishment_id)
        .agg((predicted_rate - observed_rate).abs())
    )
    if workplace_error_df.is_empty():
        raise ValueError(
            f"Scoring needs a workplace with at least {min_workplace_workers} "
            "scored workers"
        )

    return {
        "worker_auc": float(
            roc_auc_score(
                scored_df.get_column(HackCols.is_leaver),
                scored_df.get_column(HackCols.predicted_probability),
            )
        ),
        "worker_brier": float(
            brier_score_loss(
                scored_df.get_column(HackCols.is_leaver),
                scored_df.get_column(HackCols.predicted_probability),
            )
        ),
        "workplace_mae": workplace_error_df.select(
            pl.exclude(AWKClean.establishment_id).mean()
        ).item(),
        "n_workers": scored_df.height,
        "n_workplaces": workplace_error_df.height,
    }


def score_predictions(
    predictions: pd.DataFrame | pl.DataFrame | pl.LazyFrame,
    answers_source: str = DEFAULT_ANSWERS_SOURCE,
) -> dict[str, float | int]:
    """Scores predicted leaving probabilities against the held-back answers.

    Reads the answers itself, so callers never open them. Reads only the worker
    key and `is_leaver` columns: one row per worker,
    collected for scoring.

    Args:
        predictions (pd.DataFrame | pl.DataFrame | pl.LazyFrame): One row per
            worker with exactly `establishmentid`, `workerid` and
            `predicted_probability` (between 0 and 1).
        answers_source (str): S3 path of the answers dataset.

    Returns:
        dict[str, float | int]: `worker_auc`, `worker_brier`, `workplace_mae`,
            `n_workers` (scored workers) and `n_workplaces` (establishments
            with at least 10 scored workers, used in `workplace_mae`).

    Example:
        scores = score_predictions(predictions_df)
    """
    if isinstance(predictions, pd.DataFrame):
        predictions_df = pl.from_pandas(predictions)
    elif isinstance(predictions, pl.LazyFrame):
        predictions_df = predictions.collect()
    else:
        predictions_df = predictions

    answers_df = utils.scan_parquet(
        answers_source,
        selected_columns=WORKER_KEY_COLUMNS + [HackCols.is_leaver],
    ).collect()

    check_predictions(predictions_df, answers_df)
    return calculate_scores(predictions_df, answers_df)
