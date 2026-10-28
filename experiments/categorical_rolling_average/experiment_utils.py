"""
Helpers for scoring rolling average variants of a categorical's shares (first used for the
employment status rolling average, ticket 2260).

Imported by experiment.ipynb and compare.ipynb after the repo is cloned onto SageMaker, so it
may use the repo's modules. It lives outside the pipeline and has no repo tests. For another
categorical (such as gender), change only the "Settings for this categorical" block below and
the notebooks' parameters; see README.md.

Scale: the data is tens of millions of rows, so nothing here holds row-level plans in memory.
Rolling averages depend only on the "cell" (service, region, size band, job role, date, fold),
so they are built from small per-cell totals. Rows are only used for known-value scoring and for
a fixed random sample of workplaces (jumpiness, and test B's imputation).
"""

import io
from dataclasses import dataclass
from pathlib import Path

import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate.utils.impute_utils as imputeUtils
from projects._03_independent_cqc.utils import model_evaluation_utils as evaluation
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import ModelEvaluationColumns as Eval

# --- Settings for this categorical (employment status). Change this block for another. ---
LOCATION = IndCQC.location_id
DATE = IndCQC.cqc_location_import_date
SERVICE = IndCQC.primary_service_type
REGION = IndCQC.current_region
ROLE = IndCQC.published_job_role_label
SIZE_POSTS = IndCQC.estimate_filled_posts_by_job_role
# The cleaned employment status data carries region but not CSSR.
AGGREGATE_GEOGRAPHY = REGION

# The share columns at each pipeline stage, in the same status order.
KNOWN = imputeUtils.PCT_COLUMNS
TREND = imputeUtils.IMPUTED_TRENDLINE_PCT_COLUMNS
ROLLING = imputeUtils.ROLLING_AVG_PCT_COLUMNS
IMPUTED = imputeUtils.IMPUTED_PCT_COLUMNS

SHORT_TERM_EXTRAPOLATION = "2y"
SHORT_TERM_INTERPOLATION_CAP = "5y"


def add_trendline(lf: pl.LazyFrame) -> pl.LazyFrame:
    """The pipeline's step that fills short gaps in the known shares."""
    return imputeUtils.add_imputed_trendline_pcts(
        lf,
        extrapolation_period=SHORT_TERM_EXTRAPOLATION,
        interpolation_cap_period=SHORT_TERM_INTERPOLATION_CAP,
    )


def add_imputed(lf: pl.LazyFrame) -> pl.LazyFrame:
    """The pipeline's step that fills every remaining gap along the rolling averages."""
    return imputeUtils.add_imputed_pcts(lf)


# The cell-based rolling average below repeats `add_rolling_avg_pcts` (each workplace counts
# once, window sums of totals, nearest average where there are no contributors).
# --- End of settings for this categorical. ---

SIZE_BAND = Eval.size_band
FOLD = Eval.fold
ROWS = Eval.number_of_rows
CONTRIBUTORS = "contributing_workplaces"
WINDOW_CONTRIBUTORS = "window_contributing_workplaces"

CELL_KEYS = [SERVICE, REGION, SIZE_BAND, ROLE]
CELL_DATE_KEYS = [*CELL_KEYS, DATE]
TRUTH = [f"{col}_truth" for col in KNOWN]

WINDOW_MONTHS = [3, 6, 12, 24]
CURRENT_SPLIT = "current"
CURRENT_WINDOW_MONTHS = 6
MIN_WORKPLACES = 5
SIZE_BAND_EDGES = [(10, "<10"), (50, "10-49"), (150, "50-149")]
SIZE_BAND_TOP = "150+"
SIZE_BAND_UNKNOWN = "unknown"
HIDDEN = "hidden"
HIDDEN_MONTHS = 12
SAMPLE_SEED = 42

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
        *[pl.col(col).alias(truth) for col, truth in zip(KNOWN, TRUTH)],
        hidden.fill_null(False).alias(HIDDEN),
    ).with_columns(
        pl.when(pl.col(HIDDEN)).then(None).otherwise(pl.col(col)).alias(col)
        for col in KNOWN
    )


def prepare_hidden_trendline(
    prepared_lf: pl.LazyFrame, destination: Path
) -> pl.LazyFrame:
    """
    Hide each workplace's last months, re-run the pipeline's trendline imputation on what is
    left, and write the narrow result to `destination`.

    Args:
        prepared_lf (pl.LazyFrame): the prepared data, with known percentages and cell columns
        destination (Path): the parquet file to write

    Returns:
        pl.LazyFrame: a scan of the written file
    """
    hidden_lf = hide_last_months(
        prepared_lf.select(LOCATION, *CELL_DATE_KEYS, FOLD, *KNOWN)
    )
    hidden_lf = add_trendline(hidden_lf)
    hidden_lf.select(
        LOCATION, *CELL_DATE_KEYS, FOLD, HIDDEN, *KNOWN, *TRUTH, *TREND
    ).sink_parquet(destination)
    return pl.scan_parquet(destination)


def build_cells(lf: pl.LazyFrame) -> pl.DataFrame:
    """
    Total the trendline percentages per cell (service, region, size band, job role, date, fold).

    Each total is the sum over the cell's rows, with the contributing rows counted from the
    first percentage and every row counted in "number_of_rows". Totals are Float64 so window
    sums keep no rounding residual.
    """
    return (
        lf.group_by(*CELL_DATE_KEYS, FOLD)
        .agg(
            *[pl.col(col).cast(pl.Float64).sum() for col in TREND],
            pl.col(TREND[0]).is_not_null().sum().alias(CONTRIBUTORS),
            pl.len().alias(ROWS),
        )
        .collect(engine="streaming")
    )


def known_rows(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect the rows with every percentage known, narrowed to the cell keys and shares."""
    return (
        lf.filter(pl.all_horizontal(pl.col(KNOWN).is_not_null()))
        .select(*CELL_DATE_KEYS, FOLD, *KNOWN)
        .collect(engine="streaming")
    )


def workplace_sample(
    lf: pl.LazyFrame, percent: int, columns: list[str]
) -> pl.DataFrame:
    """Collect every row of a fixed random `percent` of workplaces, keeping `columns`."""
    in_sample = pl.col(LOCATION).hash(SAMPLE_SEED) % 100 < percent
    return lf.filter(in_sample).select(columns).collect(engine="streaming")


def rolling_averages(
    cells_lf: pl.LazyFrame,
    group_columns: list[str],
    period: str,
    hold_out_fold: int | None,
) -> pl.LazyFrame:
    """
    Rolling average of the trendline percentages per group and date, from cell totals.

    Matches the pipeline's `add_rolling_avg_pcts`: each workplace counts once, the window
    sums totals and contributors, and a date with no contributors takes the nearest average.
    Cells in `hold_out_fold` add nothing to any total but their dates still exist.

    Args:
        cells_lf (pl.LazyFrame): cell totals from `build_cells`
        group_columns (list[str]): the columns to average within
        period (str): the rolling window, as a Polars offset string
        hold_out_fold (int | None): a fold to leave out, or None to use every cell

    Returns:
        pl.LazyFrame: a row per group and date, with the 5 rolling averages and the date's
            contributing workplaces
    """

    def kept(column: str) -> pl.Expr:
        expr = pl.col(column)
        return (
            expr
            if hold_out_fold is None
            else expr.filter(pl.col(FOLD) != hold_out_fold)
        )

    date_totals_lf = cells_lf.group_by(*group_columns, DATE).agg(
        *[kept(col).sum() for col in TREND], kept(CONTRIBUTORS).sum()
    )
    rolled_lf = (
        date_totals_lf.sort(*group_columns, DATE)
        .rolling(index_column=DATE, group_by=group_columns, period=period)
        .agg(
            *[pl.col(col).sum() for col in TREND],
            pl.col(CONTRIBUTORS).sum().alias(WINDOW_CONTRIBUTORS),
        )
    )
    averages_lf = rolled_lf.with_columns(
        pl.when(pl.col(WINDOW_CONTRIBUTORS) > 0)
        .then(pl.col(total) / pl.col(WINDOW_CONTRIBUTORS))
        .clip(0, 1)
        .cast(pl.Float32)
        .alias(rolling)
        for total, rolling in zip(TREND, ROLLING)
    ).with_columns(
        pl.col(ROLLING)
        .forward_fill()
        .backward_fill()
        .over(group_columns, order_by=DATE)
    )
    return averages_lf.select(*group_columns, DATE, *ROLLING).join(
        date_totals_lf.select(*group_columns, DATE, CONTRIBUTORS),
        on=[*group_columns, DATE],
    )


def cell_predictions(
    cells_lf: pl.LazyFrame, variant: Variant, hold_out_fold: int | None
) -> pl.LazyFrame:
    """
    Predict each cell with the variant's rolling average: the finest group with at least
    `min_workplaces` workplaces contributing on the cell's date, else the next coarser group,
    else the last.

    Args:
        cells_lf (pl.LazyFrame): cell totals from `build_cells`
        variant (Variant): the split and window to apply
        hold_out_fold (int | None): a fold to leave out of the averages and predict, or None to
            predict every cell from every cell

    Returns:
        pl.LazyFrame: a row per cell (per fold when held out) with its "number_of_rows" and the
            5 rolling average columns
    """
    levels = SPLITS[variant.split]
    if hold_out_fold is None:
        predictions_lf = cells_lf.group_by(*CELL_DATE_KEYS).agg(pl.col(ROWS).sum())
    else:
        predictions_lf = cells_lf.filter(pl.col(FOLD) == hold_out_fold).select(
            *CELL_DATE_KEYS, FOLD, ROWS
        )

    for i, group_columns in enumerate(levels):
        averages_lf = rolling_averages(
            cells_lf, group_columns, variant.period, hold_out_fold
        ).rename({col: f"{col}_level{i}" for col in [*ROLLING, CONTRIBUTORS]})
        predictions_lf = predictions_lf.join(
            averages_lf, on=[*group_columns, DATE], how="left"
        )

    coarsest = len(levels) - 1
    predictions_lf = predictions_lf.with_columns(
        pl.col(f"{col}_level{coarsest}").alias(col) for col in ROLLING
    )
    for i in reversed(range(coarsest)):
        predictions_lf = predictions_lf.with_columns(
            pl.when(pl.col(f"{CONTRIBUTORS}_level{i}") >= variant.min_workplaces)
            .then(pl.col(f"{col}_level{i}"))
            .otherwise(pl.col(col))
            .alias(col)
            for col in ROLLING
        )

    temporary = [
        f"{col}_level{i}"
        for col in [*ROLLING, CONTRIBUTORS]
        for i in range(len(levels))
    ]
    return predictions_lf.drop(temporary)


def predict_out_of_fold(
    cells_df: pl.DataFrame, variant: Variant, n_folds: int
) -> pl.DataFrame:
    """
    Predict every cell from the cells of the other folds only, one fold at a time.

    Args:
        cells_df (pl.DataFrame): cell totals from `build_cells`
        variant (Variant): the split and window to apply
        n_folds (int): the number of folds

    Returns:
        pl.DataFrame: a row per cell and fold with its 5 rolling average columns
    """
    return pl.concat(
        [
            cell_predictions(cells_df.lazy(), variant, fold).collect(engine="streaming")
            for fold in range(n_folds)
        ]
    )


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


def _share_scores(
    scored_lf: pl.LazyFrame, predicted_columns: list[str], known_columns: list[str]
) -> pl.DataFrame:
    """Share scores pooled, per job role and per size band, in long format."""
    scored_lf = scored_lf.collect(engine="streaming").lazy()
    results = []
    for level, by in [("pooled", None), ("job_role", ROLE), ("size_band", SIZE_BAND)]:
        scores_lf = evaluation.score_shares(
            scored_lf, predicted_columns, known_columns, [by] if by else None
        )
        results.append(_long(scores_lf.collect(), level, by))
    return pl.concat(results)


def _coverage(cells_df: pl.DataFrame) -> pl.DataFrame:
    """Share of rows whose cell has a prediction."""
    weighted = pl.col(ROWS) * pl.col(ROLLING[0]).is_not_null()
    coverage_df = cells_df.select(
        (weighted.sum() / pl.col(ROWS).sum()).alias(Eval.coverage)
    )
    return _long(coverage_df, "coverage", None)


def _jumpiness(
    level: str,
    lf: pl.LazyFrame,
    value_columns: list[str],
    partition_columns: list[str],
) -> pl.DataFrame:
    jump_df = evaluation.mean_period_to_period_change(
        lf, value_columns, partition_columns, DATE
    ).collect()
    return pl.DataFrame(
        {
            Eval.level: [level],
            Eval.group: ["all"],
            Eval.metric: [Eval.mean_period_to_period_change],
            Eval.value: [jump_df[Eval.mean_period_to_period_change].mean()],
        }
    )


def score_out_of_fold(
    predictions_df: pl.DataFrame, known_df: pl.DataFrame, sample_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Score out-of-fold predictions in long format: share scores on known rows, coverage, and
    jumpiness per sampled workplace and for the service by geography by role aggregate.

    Args:
        predictions_df (pl.DataFrame): from `predict_out_of_fold`
        known_df (pl.DataFrame): from `known_rows`
        sample_df (pl.DataFrame): the workplace sample, from `workplace_sample`, with its
            location, cell keys and fold

    Returns:
        pl.DataFrame: "level", "group", "metric" and "value" rows
    """
    keys = [*CELL_DATE_KEYS, FOLD]
    predictions_lf = predictions_df.lazy()
    shares_lf = predictions_lf.select(*keys, *ROLLING)

    scored_lf = known_df.lazy().join(shares_lf, on=keys)
    workplace_lf = sample_df.lazy().join(shares_lf, on=keys)
    aggregate_lf = predictions_lf.group_by(
        SERVICE, AGGREGATE_GEOGRAPHY, ROLE, DATE
    ).agg((pl.col(ROLLING) * pl.col(ROWS)).sum() / pl.col(ROWS).sum())
    return pl.concat(
        [
            _share_scores(scored_lf, ROLLING, KNOWN),
            _coverage(predictions_df),
            _jumpiness("jumpiness_workplace", workplace_lf, ROLLING, [LOCATION, ROLE]),
            _jumpiness(
                "jumpiness_aggregate",
                aggregate_lf,
                ROLLING,
                [SERVICE, AGGREGATE_GEOGRAPHY, ROLE],
            ),
        ]
    )


def score_hidden(predictions_df: pl.DataFrame, sample_df: pl.DataFrame) -> pl.DataFrame:
    """
    Run the pipeline's imputation on the sampled workplaces with the variant's rolling
    averages, then score the imputed shares on the hidden rows.

    Args:
        predictions_df (pl.DataFrame): `cell_predictions` for every cell, collected
        sample_df (pl.DataFrame): sampled workplaces' rows from the hidden data, with cell keys,
            known and truth percentages and the "hidden" flag

    Returns:
        pl.DataFrame: "level", "group", "metric" and "value" rows
    """
    imputed_lf = add_imputed(
        sample_df.lazy().join(
            predictions_df.lazy().select(*CELL_DATE_KEYS, *ROLLING),
            on=CELL_DATE_KEYS,
            how="left",
        )
    )
    imputed_df = imputed_lf.select(
        LOCATION, *CELL_DATE_KEYS, HIDDEN, *IMPUTED, *TRUTH
    ).collect(engine="streaming")
    return pl.concat(
        [
            _share_scores(imputed_df.lazy().filter(pl.col(HIDDEN)), IMPUTED, TRUTH),
            _coverage(predictions_df),
            _jumpiness(
                "jumpiness_workplace", imputed_df.lazy(), IMPUTED, [LOCATION, ROLE]
            ),
        ]
    )


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
