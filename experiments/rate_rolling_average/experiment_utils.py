"""
Helpers for the turnover rate rolling average experiment (ticket 2011).

Imported by experiment.ipynb and compare.ipynb after the repo is cloned onto SageMaker. Nothing
here runs in the pipeline. Scores are on the turnover rate: leavers over estimated employees.
"""

import io
from dataclasses import dataclass
from pathlib import Path

import polars as pl

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.utils.impute_utils as iUtils
from projects._03_independent_cqc.utils.imputation.trendline import (
    add_imputed_trendline,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as Eval,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)

LOCATION = IndCQC.location_id
DATE = IndCQC.cqc_location_import_date
SERVICE = IndCQC.primary_service_type
REGION = IndCQC.current_region
CSSR = IndCQC.current_cssr
ROLE = IndCQC.published_job_role_label
SIZE_BAND = Eval.size_band
SIZE_POSTS = IndCQC.estimate_filled_posts_by_job_role
WORKERS = EmpStatus.estimated_employees
FOLD = Eval.fold

RATE = SLVCols.ascwds_turnover_rate
TREND = SLVCols.ascwds_turnover_rate_imputed_trendline
ROLLING = SLVCols.ascwds_turnover_rate_rolling_avg
IMPUTED = SLVCols.ascwds_turnover_rate_imputed
RATE_TRUTH = f"{RATE}_truth"
HIDDEN = "hidden"

# Names of the columns being scored. Tests set both to a predicted and a known rate.
PREDICTED = "predicted_rate"
TRUTH = "known_rate"

AGGREGATE_ERROR = "aggregate_error"
AGGREGATE_BIAS = "aggregate_bias"
WORKPLACE_MAE = "workplace_mae"
WORKPLACE_BIAS = "workplace_bias"
JUMPINESS_AGGREGATE = "jumpiness_aggregate"
JUMPINESS_WORKPLACE = "jumpiness_workplace"

# The lowest published grain: service by CSSR by job role.
AGGREGATE_CELL = [SERVICE, CSSR, ROLE, DATE]
AGGREGATE_SERIES = [SERVICE, CSSR, ROLE]

WINDOW_MONTHS = [3, 6, 12, 24, 36]
FILL_CAPS = ["12mo", "2y"]
METHODS = ["nominal", "ratio"]
INTERPOLATION_CAP = "5y"
MIN_CONTRIBUTORS = 5
HIDDEN_MONTHS = 12
SIZE_BAND_EDGES = [(10, "<10"), (50, "10-49"), (150, "50-149")]
SIZE_BAND_TOP = "150+"
SIZE_BAND_UNKNOWN = "unknown"

# Group columns finest to coarsest. The finest with enough contributors on a date is used,
# else the next coarser, and the last is pooled across everything for the job role.
SPLITS: dict[str, list[list[str]]] = {
    "current": [[SERVICE, REGION, ROLE], [SERVICE, ROLE], [ROLE]],
    "service_role": [[SERVICE, ROLE], [ROLE]],
    "region_role": [[REGION, ROLE], [ROLE]],
    "role": [[ROLE]],
    "service_size_role": [[SERVICE, SIZE_BAND, ROLE], [SERVICE, ROLE], [ROLE]],
    "service_region_size_role": [
        [SERVICE, REGION, SIZE_BAND, ROLE],
        [SERVICE, REGION, ROLE],
        [SERVICE, ROLE],
        [ROLE],
    ],
}


@dataclass(frozen=True)
class Variant:
    split: str
    window_months: int
    fill_cap: str
    method: str

    @property
    def name(self) -> str:
        return f"{self.split}_{self.window_months}mo_{self.fill_cap}_{self.method}"

    @property
    def fill_name(self) -> str:
        """The name shared by variants that differ only in the final imputation method."""
        return f"{self.split}_{self.window_months}mo_{self.fill_cap}"


VARIANTS = [
    Variant(split, window, cap, method)
    for split in SPLITS
    for window in WINDOW_MONTHS
    for cap in FILL_CAPS
    for method in METHODS
]


def add_size_band(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add the workplace size band: the sum of job role estimates per workplace and date, banded
    <10, 10-49, 50-149 and 150+, or "unknown" where no job role has an estimate.
    """
    size = (
        pl.when(pl.col(SIZE_POSTS).is_not_null().any())
        .then(pl.col(SIZE_POSTS).sum())
        .over(LOCATION, DATE)
    )
    band = pl.when(size.is_null()).then(pl.lit(SIZE_BAND_UNKNOWN))
    for upper_edge, label in SIZE_BAND_EDGES:
        band = band.when(size < upper_edge).then(pl.lit(label))
    band = band.otherwise(pl.lit(SIZE_BAND_TOP))
    return lf.with_columns(band.cast(pl.Categorical).alias(SIZE_BAND))


def add_trendline(lf: pl.LazyFrame, fill_cap: str) -> pl.LazyFrame:
    """Add the turnover rate trendline fill with the given extrapolation cap."""
    return add_imputed_trendline(
        lf,
        known_columns=[RATE],
        trendline_columns=[TREND],
        group_columns=[LOCATION, ROLE],
        extrapolation_period=fill_cap,
        interpolation_cap_period=INTERPOLATION_CAP,
    )


def add_variant_rolling_avg(lf: pl.LazyFrame, variant: Variant) -> pl.LazyFrame:
    """Add the rolling average with the variant's split and window."""
    return iUtils.add_rolling_avg_rate(
        lf,
        rolling_period=f"{variant.window_months}mo",
        group_levels=SPLITS[variant.split],
        min_contributors=MIN_CONTRIBUTORS,
    )


def as_mean_of_rates(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Give every contributing workplace one worker, so the rolling average becomes a mean of
    rates instead of leavers over employees. Null or sub-1 employees are left as they are,
    so the contributor filter still excludes them.
    """
    return lf.with_columns(
        pl.when(pl.col(WORKERS) >= 1)
        .then(pl.lit(1.0, dtype=pl.Float32))
        .otherwise(pl.col(WORKERS))
        .alias(WORKERS)
    )


def hold_out_fold(lf: pl.LazyFrame, fold: int) -> pl.LazyFrame:
    """
    Null the turnover rate and its trendline on every row of the workplaces in `fold`, so they
    add nothing to any average.

    Args:
        lf (pl.LazyFrame): dataset with a "fold" column per location
        fold (int): the fold to hold out

    Returns:
        pl.LazyFrame: dataset with the held-out workplaces' rates nulled
    """
    held_out = pl.col(FOLD) == fold
    columns = [col for col in [RATE, TREND] if col in lf.collect_schema().names()]
    return lf.with_columns(
        pl.when(held_out).then(None).otherwise(pl.col(col)).alias(col)
        for col in columns
    )


def hide_last_months(lf: pl.LazyFrame, months: int = HIDDEN_MONTHS) -> pl.LazyFrame:
    """
    Null each workplace's known turnover rate in its last `months` months with a known rate,
    keeping the original in "<rate>_truth" and flagging the hidden rows in "hidden".
    """
    last_known_date = (
        pl.col(DATE).filter(pl.col(RATE).is_not_null()).max().over(LOCATION)
    )
    hidden = pl.col(RATE).is_not_null() & (
        pl.col(DATE) > last_known_date.dt.offset_by(f"-{months}mo")
    )
    return lf.with_columns(
        pl.col(RATE).alias(RATE_TRUTH),
        hidden.alias(HIDDEN),
    ).with_columns(
        pl.when(pl.col(HIDDEN)).then(None).otherwise(pl.col(RATE)).alias(RATE)
    )


def _scored_rows(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.filter(
        pl.col(PREDICTED).is_not_null()
        & pl.col(TRUTH).is_not_null()
        & (pl.col(WORKERS) >= 1)
    )


def score_aggregate(lf: pl.LazyFrame) -> dict[str, float]:
    """
    Score the rate of each service by CSSR by job role by date cell, weighted by workers.

    A cell's rate is its total leavers over its total workers, for both prediction and truth,
    over workplaces with a known rate and at least 1 worker. Error is the mean absolute
    difference and bias the mean signed difference (predicted minus known), each weighted by
    the cell's workers.

    Args:
        lf (pl.LazyFrame): dataset with the predicted and known rates, workers and cell columns

    Returns:
        dict[str, float]: "aggregate_error" and "aggregate_bias"
    """
    cell_df = (
        _scored_rows(lf)
        .group_by(AGGREGATE_CELL)
        .agg(
            pl.col(WORKERS).cast(pl.Float64).sum().alias("workers"),
            (pl.col(PREDICTED).cast(pl.Float64) * pl.col(WORKERS).cast(pl.Float64))
            .sum()
            .alias("predicted_leavers"),
            (pl.col(TRUTH).cast(pl.Float64) * pl.col(WORKERS).cast(pl.Float64))
            .sum()
            .alias("known_leavers"),
        )
        .with_columns(
            (
                (pl.col("predicted_leavers") - pl.col("known_leavers"))
                / pl.col("workers")
            ).alias("difference")
        )
        .select(
            (
                (pl.col("difference").abs() * pl.col("workers")).sum()
                / pl.col("workers").sum()
            ).alias(AGGREGATE_ERROR),
            (
                (pl.col("difference") * pl.col("workers")).sum()
                / pl.col("workers").sum()
            ).alias(AGGREGATE_BIAS),
        )
        .collect()
    )
    return {
        AGGREGATE_ERROR: cell_df.item(0, 0),
        AGGREGATE_BIAS: cell_df.item(0, 1),
    }


def score_workplace(lf: pl.LazyFrame) -> dict[str, float]:
    """Mean absolute error and mean bias per workplace row, each row counting once."""
    difference = pl.col(PREDICTED).cast(pl.Float64) - pl.col(TRUTH).cast(pl.Float64)
    scores_df = (
        _scored_rows(lf)
        .select(
            difference.abs().mean().alias(WORKPLACE_MAE),
            difference.mean().alias(WORKPLACE_BIAS),
        )
        .collect()
    )
    return {
        WORKPLACE_MAE: scores_df.item(0, 0),
        WORKPLACE_BIAS: scores_df.item(0, 1),
    }


def mean_change_between_dates(lf: pl.LazyFrame, series: list[str]) -> float:
    """Mean absolute change in the predicted rate between consecutive dates in each series."""
    return (
        lf.sort(*series, DATE)
        .select(pl.col(PREDICTED).cast(pl.Float64).diff().abs().over(series).mean())
        .collect()
        .item()
    )


def score_jumpiness(lf: pl.LazyFrame) -> dict[str, float]:
    """
    Mean absolute change in the predicted rate between consecutive dates, for each workplace
    (by location and job role) and for each service by CSSR by job role aggregate. Uses every
    row with a prediction.
    """
    predicted = pl.col(PREDICTED).cast(pl.Float64)
    prediction_lf = lf.filter(predicted.is_not_null())

    aggregate_lf = (
        prediction_lf.filter(pl.col(WORKERS) >= 1)
        .group_by(AGGREGATE_CELL)
        .agg(
            (
                (predicted * pl.col(WORKERS).cast(pl.Float64)).sum()
                / pl.col(WORKERS).cast(pl.Float64).sum()
            ).alias(PREDICTED)
        )
    )
    return {
        JUMPINESS_WORKPLACE: mean_change_between_dates(prediction_lf, [LOCATION, ROLE]),
        JUMPINESS_AGGREGATE: mean_change_between_dates(aggregate_lf, AGGREGATE_SERIES),
    }


def score_variant(
    lf: pl.LazyFrame, predicted_column: str, truth_column: str, scored_filter: pl.Expr
) -> dict[str, float]:
    """
    Score a variant's predictions on the rows selected by `scored_filter`: aggregate error and
    bias, workplace error and bias, and jumpiness over every row.

    Args:
        lf (pl.LazyFrame): predictions with the known rate, workers and group columns
        predicted_column (str): the predicted rate column
        truth_column (str): the known rate column
        scored_filter (pl.Expr): which rows are scored, such as held-out or hidden rows

    Returns:
        dict[str, float]: every score
    """
    renames = {predicted_column: PREDICTED, truth_column: TRUTH}
    scoring_lf = lf.filter(scored_filter).rename(renames, strict=False)
    return {
        **score_aggregate(scoring_lf),
        **score_workplace(scoring_lf),
        **score_jumpiness(lf.rename(renames, strict=False)),
    }


def predict_out_of_fold(
    lf: pl.LazyFrame, variant: Variant, n_folds: int
) -> pl.LazyFrame:
    """
    Predict every row from workplaces in the other folds only (test A).

    For each fold the held-out workplaces' rates are nulled before the trendline fill, so they
    add nothing to any average, then the variant's rolling average is read onto the held-out
    rows. Held-out workplaces keep their known rate in "<rate>_truth".

    Args:
        lf (pl.LazyFrame): dataset with the turnover rate, group columns and "fold"
        variant (Variant): the split, window and fill cap to apply
        n_folds (int): the number of folds

    Returns:
        pl.LazyFrame: the held-out rows of every fold, with the rolling average
    """
    lf = lf.with_columns(pl.col(RATE).alias(RATE_TRUTH))
    folds = []
    for fold in range(n_folds):
        masked_lf = add_trendline(hold_out_fold(lf, fold), variant.fill_cap)
        folds.append(
            add_variant_rolling_avg(masked_lf, variant).filter(pl.col(FOLD) == fold)
        )
    return pl.concat(folds)


def run_imputation_with_variant(lf: pl.LazyFrame, variant: Variant) -> pl.LazyFrame:
    """Run the pipeline's imputation steps with the variant's settings (test B)."""
    lf = add_trendline(lf, variant.fill_cap)
    lf = add_variant_rolling_avg(lf, variant)
    return iUtils.add_imputed_rate(lf, extrapolation_method=variant.method)


def write_result(df: pl.DataFrame, name: str, results_dir: str, local: bool) -> None:
    """Write a result as parquet to `results_dir`, an S3 prefix or a local folder."""
    if local:
        path = Path(results_dir) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)
        return
    import boto3

    bucket, key = results_dir.removeprefix("s3://").split("/", 1)
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    boto3.client("s3").put_object(
        Bucket=bucket, Key=f"{key.rstrip('/')}/{name}", Body=buffer.getvalue()
    )


def read_result(name: str, results_dir: str, local: bool) -> pl.DataFrame:
    """Read a result written by `write_result`."""
    if local:
        return pl.read_parquet(Path(results_dir) / name)
    import boto3

    bucket, key = results_dir.removeprefix("s3://").split("/", 1)
    body = (
        boto3.client("s3")
        .get_object(Bucket=bucket, Key=f"{key.rstrip('/')}/{name}")["Body"]
        .read()
    )
    return pl.read_parquet(io.BytesIO(body))


def list_results(prefix: str, results_dir: str, local: bool) -> list[str]:
    """List result names under `prefix` in `results_dir`, such as "test_a/"."""
    if local:
        return sorted(
            f"{prefix}{p.name}" for p in (Path(results_dir) / prefix).glob("*.parquet")
        )
    import boto3

    bucket, key = results_dir.removeprefix("s3://").split("/", 1)
    pages = (
        boto3.client("s3")
        .get_paginator("list_objects_v2")
        .paginate(Bucket=bucket, Prefix=f"{key.rstrip('/')}/{prefix}")
    )
    strip = len(key.rstrip("/")) + 1
    return sorted(
        o["Key"][strip:]
        for page in pages
        for o in page.get("Contents", [])
        if o["Key"].endswith(".parquet")
    )
