"""
Helpers for the 2260 employment status rolling average experiments.

Imported by experiment.ipynb and compare.ipynb after the repo is cloned onto SageMaker, so it
may use the repo's modules. It lives outside the pipeline and has no repo tests. Written to be
reusable for other categorical shares (such as gender): change the column constants and `SPLITS`.
"""

import io
from dataclasses import dataclass
from pathlib import Path

import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate.utils.impute_utils as iUtils
from projects._03_independent_cqc.utils import model_evaluation_utils as evaluation
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import ModelEvaluationColumns as Eval

LOCATION = IndCQC.location_id
DATE = IndCQC.cqc_location_import_date
SERVICE = IndCQC.primary_service_type
REGION = IndCQC.current_region
ROLE = IndCQC.published_job_role_label
SIZE_BAND = Eval.size_band
SIZE_POSTS = IndCQC.estimate_filled_posts_by_job_role
# The cleaned employment status data carries region but not CSSR.
AGGREGATE_GEOGRAPHY = REGION

KNOWN = iUtils.PCT_COLUMNS
TREND = iUtils.IMPUTED_TRENDLINE_PCT_COLUMNS
ROLLING = iUtils.ROLLING_AVG_PCT_COLUMNS
IMPUTED = iUtils.IMPUTED_PCT_COLUMNS

WINDOW_MONTHS = [3, 6, 12, 24]
CURRENT_SPLIT = "current"
CURRENT_WINDOW_MONTHS = 6
MIN_WORKPLACES = 5
SIZE_BAND_EDGES = [(10, "<10"), (50, "10-49"), (150, "50-149")]
SIZE_BAND_TOP = "150+"
SIZE_BAND_UNKNOWN = "unknown"
HIDDEN = "hidden"
HIDDEN_MONTHS = 12
SHORT_TERM_EXTRAPOLATION = "2y"
SHORT_TERM_INTERPOLATION_CAP = "5y"

# Finest to coarsest group columns. The finest with enough workplaces on a date is used,
# else the next coarser, and the last is the pooled average for the job role.
SPLITS: dict[str, list[list[str]]] = {
    CURRENT_SPLIT: [[SERVICE, REGION, ROLE]],
    "current_with_fallback": [[SERVICE, REGION, ROLE], [SERVICE, ROLE], [ROLE]],
    "service_role": [[SERVICE, ROLE], [ROLE]],
    "region_role": [[REGION, ROLE], [ROLE]],
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

    @property
    def name(self) -> str:
        return f"{self.split}_{self.window_months}mo"

    @property
    def period(self) -> str:
        return f"{self.window_months}mo"

    @property
    def min_workplaces(self) -> int:
        # The current split has no fallback, so it keeps its any-contributor rule.
        return 1 if self.split == CURRENT_SPLIT else MIN_WORKPLACES


CURRENT = Variant(CURRENT_SPLIT, CURRENT_WINDOW_MONTHS)
VARIANTS = [Variant(split, window) for split in SPLITS for window in WINDOW_MONTHS]


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


def add_rolling_average_with_fallback(
    lf: pl.LazyFrame, levels: list[list[str]], period: str, min_workplaces: int
) -> pl.LazyFrame:
    """
    Add the 5 rolling average columns using the finest group with at least `min_workplaces`
    workplaces contributing on the row's date, else the next coarser group, else the last.

    Args:
        lf (pl.LazyFrame): dataset containing the trendline percentages and every group column
        levels (list[list[str]]): group columns, finest first
        period (str): the rolling window, as a Polars offset string
        min_workplaces (int): workplaces needed on the date to use a level

    Returns:
        pl.LazyFrame: dataset with the 5 rolling average columns added
    """
    for i, group_columns in enumerate(levels):
        lf = iUtils.add_rolling_avg_pcts(
            lf, rolling_period=period, group_columns=group_columns
        )
        lf = lf.rename({col: f"{col}_level{i}" for col in ROLLING}).with_columns(
            pl.col(TREND[0])
            .is_not_null()
            .sum()
            .over(group_columns + [DATE])
            .alias(f"workplaces_level{i}")
        )

    coarsest = len(levels) - 1
    lf = lf.with_columns(pl.col(f"{col}_level{coarsest}").alias(col) for col in ROLLING)
    for i in reversed(range(coarsest)):
        lf = lf.with_columns(
            pl.when(pl.col(f"workplaces_level{i}") >= min_workplaces)
            .then(pl.col(f"{col}_level{i}"))
            .otherwise(pl.col(col))
            .alias(col)
            for col in ROLLING
        )

    temporary = [f"workplaces_level{i}" for i in range(len(levels))] + [
        f"{col}_level{i}" for col in ROLLING for i in range(len(levels))
    ]
    return lf.drop(temporary)


def add_variant_rolling_average(lf: pl.LazyFrame, variant: Variant) -> pl.LazyFrame:
    return add_rolling_average_with_fallback(
        lf, SPLITS[variant.split], variant.period, variant.min_workplaces
    )


def predict_out_of_fold(
    lf: pl.LazyFrame, variant: Variant, n_folds: int
) -> pl.LazyFrame:
    """
    Predict every row from workplaces in the other folds only.

    For each fold the held-out workplaces' trendline percentages are nulled, so they add nothing
    to any average, then the variant's rolling average is read back onto the held-out rows.

    Args:
        lf (pl.LazyFrame): dataset with the trendline percentages, group columns and "fold"
        variant (Variant): the split and window to apply
        n_folds (int): the number of folds

    Returns:
        pl.LazyFrame: the held-out rows of every fold, with the 5 rolling average columns
    """
    folds = []
    for fold in range(n_folds):
        held_out = pl.col(Eval.fold) == fold
        masked_lf = lf.with_columns(
            pl.when(held_out).then(None).otherwise(pl.col(col)).alias(col)
            for col in TREND
        )
        folds.append(add_variant_rolling_average(masked_lf, variant).filter(held_out))
    return pl.concat(folds)


def hide_last_months(lf: pl.LazyFrame, months: int = HIDDEN_MONTHS) -> pl.LazyFrame:
    """
    Null each workplace's known percentages in its last `months` months with known values,
    keeping the originals in "<column>_truth" and flagging the hidden rows in "hidden".
    """
    last_known_date = (
        pl.col(DATE).filter(pl.col(KNOWN[0]).is_not_null()).max().over(LOCATION)
    )
    hidden = pl.col(DATE) > last_known_date.dt.offset_by(f"-{months}mo")
    return lf.with_columns(
        *[pl.col(col).alias(f"{col}_truth") for col in KNOWN],
        hidden.fill_null(False).alias(HIDDEN),
    ).with_columns(
        pl.when(pl.col(HIDDEN)).then(None).otherwise(pl.col(col)).alias(col)
        for col in KNOWN
    )


def run_imputation_with_variant(lf: pl.LazyFrame, variant: Variant) -> pl.LazyFrame:
    """Run the pipeline's imputation steps with the variant's rolling average."""
    lf = iUtils.add_imputed_trendline_pcts(
        lf,
        extrapolation_period=SHORT_TERM_EXTRAPOLATION,
        interpolation_cap_period=SHORT_TERM_INTERPOLATION_CAP,
    )
    lf = add_variant_rolling_average(lf, variant)
    return iUtils.add_imputed_pcts(lf)


def _long(df: pl.DataFrame, level: str, group_column: str | None) -> pl.DataFrame:
    index = [group_column] if group_column else []
    group = (
        pl.col(group_column).cast(pl.String) if group_column else pl.lit("all")
    ).alias(Eval.group)
    return (
        df.with_columns(pl.exclude(index).cast(pl.Float64))
        .unpivot(index=index, variable_name=Eval.metric, value_name=Eval.value)
        .select(pl.lit(level).alias(Eval.level), group, Eval.metric, Eval.value)
    )


def score_variant(
    pred_lf: pl.LazyFrame,
    predicted_columns: list[str],
    known_columns: list[str],
    scored_filter: pl.Expr | None = None,
) -> pl.DataFrame:
    """
    Score predictions in long format: share scores pooled, per job role and per size band,
    coverage, and jumpiness per workplace and for the service by geography by role aggregate.

    Args:
        pred_lf (pl.LazyFrame): predictions with the known columns, group columns and date
        predicted_columns (list[str]): the 5 predicted share columns
        known_columns (list[str]): the 5 known share columns, in the same order
        scored_filter (pl.Expr | None): restricts which rows are scored, such as hidden rows.
            Coverage and jumpiness always use every row.

    Returns:
        pl.DataFrame: "level", "group", "metric" and "value" rows
    """
    scoring_lf = pred_lf if scored_filter is None else pred_lf.filter(scored_filter)
    results = []
    for level, by in [("pooled", None), ("job_role", ROLE), ("size_band", SIZE_BAND)]:
        scores_lf = evaluation.score_shares(
            scoring_lf, predicted_columns, known_columns, [by] if by else None
        )
        results.append(_long(scores_lf.collect(), level, by))

    coverage_lf = evaluation.calculate_coverage(pred_lf, predicted_columns[0])
    results.append(_long(coverage_lf.collect(), "coverage", None))

    workplace_jump_df = evaluation.mean_period_to_period_change(
        pred_lf, predicted_columns, [LOCATION, ROLE], DATE
    ).collect()
    aggregate_lf = pred_lf.group_by(SERVICE, AGGREGATE_GEOGRAPHY, ROLE, DATE).agg(
        pl.col(predicted_columns).mean()
    )
    aggregate_jump_df = evaluation.mean_period_to_period_change(
        aggregate_lf, predicted_columns, [SERVICE, AGGREGATE_GEOGRAPHY, ROLE], DATE
    ).collect()
    for level, jump_df in [
        ("jumpiness_workplace", workplace_jump_df),
        ("jumpiness_aggregate", aggregate_jump_df),
    ]:
        results.append(
            pl.DataFrame(
                {
                    Eval.level: [level],
                    Eval.group: ["all"],
                    Eval.metric: [Eval.mean_period_to_period_change],
                    Eval.value: [jump_df[Eval.mean_period_to_period_change].mean()],
                }
            )
        )
    return pl.concat(results)


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
