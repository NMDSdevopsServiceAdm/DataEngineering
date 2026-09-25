"""
DO NOT MERGE (ticket 2107 spike): back-tests the diagnostic threshold methods.

Runs every method and setting over the monthly tracker clean output twice -
once as published (BD214 brand id filter on) and once from the branch's
filter-off pipeline run - and writes two small CSVs holding flags and counts
only, never metric values:

- backtest_flags.csv: breached (true/false/blank) per run, method, setting,
  basis, metric, service type and period.
- backtest_summary.csv: summarise_backtest's false-alarm and detected-break
  counts.

IO only: all logic lives in fargate/utils/diagnostic_thresholds.py, which is
unit tested.
"""

import io
from datetime import date

import boto3
import polars as pl

from polars_utils import utils
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    diagnostic_thresholds as DT,
)

Cols = DT.Cols

K_VALUES: list[float] = [2, 3, 4]
FIXED_BAND_SETTINGS: list[dict[str, float]] = [
    {DT.IntervalType.monthly: 0.01, DT.IntervalType.quarterly: 0.03},
    {DT.IntervalType.monthly: 0.02, DT.IntervalType.quarterly: 0.05},
    {DT.IntervalType.monthly: 0.03, DT.IntervalType.quarterly: 0.08},
]
CROSS_SECTIONAL_SETTINGS: list[dict[str, float]] = FIXED_BAND_SETTINGS

# The BD214 filter covers import dates after 2024-03-01 and before
# 2026-05-01. The monthly tracker keeps Jan 2024 (quarterly) then monthly
# from Apr 2024, so the drop is the Jan->Apr 2024 step and the recovery the
# Apr->May 2026 step. Non-res and its All CQC locations total both contain
# BD214; the SfC-minus-CT gap shows it for non-res in each window covering
# the period.
_BREAK_PERIODS: list[date] = [date(2024, 4, 1), date(2026, 5, 1)]
_BREAK_SERIES: list[tuple[str, str]] = [
    (DT.Metric.filled_posts, DT.WorkbookServiceType.non_residential),
    (DT.Metric.filled_posts, DT.WorkbookServiceType.all_cqc_locations),
] + [
    (DT.sfc_minus_ct_metric(term), DT.WorkbookServiceType.non_residential)
    for term in DT.ASSESSMENT_TERMS
]

_RUN: str = "run"
_FLAG_COLUMNS: list[str] = DT.BACKTEST_KEYS + [Cols.period, Cols.breached]


def _window_from_dates(today: date) -> dict[str, date]:
    # Mirrors _02_clean's assessment window start dates.
    fy_year = today.year if today.month >= 4 else today.year - 1
    cutoff_date = date(fy_year - 6, 4, 1)
    return {
        "long_term": max(date(2021, 7, 1), cutoff_date),
        "medium_term": date(fy_year - 1, 4, 1),
        "short_term": date(fy_year, 4, 1),
    }


def _prepare(clean_lf: pl.LazyFrame, today: date) -> pl.LazyFrame:
    long_lf = DT.to_metric_long_format(clean_lf, _window_from_dates(today))
    long_lf = DT.add_interval_type(long_lf)
    long_lf = DT.add_period_on_period_change(long_lf)
    long_lf = DT.add_change_since_march(long_lf)
    gap_lf = DT.add_sfc_ct_gap(long_lf)
    return pl.concat([long_lf, gap_lf], how="diagonal")


def _setting_label(limits: dict[str, float]) -> str:
    return "/".join(f"{interval}={limit:g}" for interval, limit in limits.items())


def _flag_all_methods(prepared_lf: pl.LazyFrame) -> pl.LazyFrame:
    bases = {
        Cols.period_on_period_change: prepared_lf,
        # The since-March basis only exists for the workbook's own series.
        Cols.change_since_march: prepared_lf.filter(
            pl.col(Cols.change_since_march).is_not_null()
        ),
    }
    flag_frames = []
    for value_col, basis_lf in bases.items():
        runs = [
            ("mean_std", f"k={k:g}", DT.flag_mean_std(basis_lf, value_col, k))
            for k in K_VALUES
        ]
        runs += [
            ("median_mad", f"k={k:g}", DT.flag_median_mad(basis_lf, value_col, k))
            for k in K_VALUES
        ]
        runs += [
            (
                "fixed_band",
                _setting_label(limits),
                DT.flag_fixed_band(basis_lf, value_col, limits),
            )
            for limits in FIXED_BAND_SETTINGS
        ]
        runs += [
            (
                "cross_sectional",
                _setting_label(limits),
                DT.flag_cross_sectional(basis_lf, value_col, limits),
            )
            for limits in CROSS_SECTIONAL_SETTINGS
        ]
        flag_frames += [
            flags_lf.with_columns(
                pl.lit(method).alias(Cols.method),
                pl.lit(setting).alias(Cols.setting),
                pl.lit(value_col).alias(Cols.basis),
            ).select(_FLAG_COLUMNS)
            for method, setting, flags_lf in runs
        ]
    return pl.concat(flag_frames)


def _put_csv(df: pl.DataFrame, destination: str, file_name: str) -> None:
    bucket, prefix = destination.removeprefix("s3://").split("/", 1)
    buffer = io.BytesIO()
    df.write_csv(buffer)
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=f"{prefix.strip('/')}/{file_name}",
        Body=buffer.getvalue(),
        ContentType="text/csv",
    )
    print(f"Wrote {len(df)} rows to {destination.rstrip('/')}/{file_name}")


def main(
    filter_on_source: str, filter_off_source: str, summary_destination: str
) -> None:
    """
    Back-tests every threshold method and setting against the BD214 filter.

    Args:
        filter_on_source (str): s3 directory of the published (filter on)
            monthly_tracker_filled_posts_02_clean data.
        filter_off_source (str): s3 directory of the branch's filter-off
            monthly_tracker_filled_posts_02_clean data.
        summary_destination (str): s3 directory the flag and summary CSVs are
            written to.
    """
    today = date.today()
    filter_on_flags_lf = _flag_all_methods(
        _prepare(utils.scan_parquet(filter_on_source), today)
    )
    filter_off_flags_lf = _flag_all_methods(
        _prepare(utils.scan_parquet(filter_off_source), today)
    )
    expected_breaks_lf = pl.LazyFrame(
        [
            (metric, service_type, period)
            for metric, service_type in _BREAK_SERIES
            for period in _BREAK_PERIODS
        ],
        pl.Schema(
            [
                (Cols.metric, pl.String()),
                (Cols.service_type, pl.String()),
                (Cols.period, pl.Date()),
            ]
        ),
        orient="row",
    )

    flags_df = pl.concat(
        [
            filter_on_flags_lf.with_columns(pl.lit("filter_on").alias(_RUN)),
            filter_off_flags_lf.with_columns(pl.lit("filter_off").alias(_RUN)),
        ]
    ).collect()
    summary_df = DT.summarise_backtest(
        flags_df.lazy().filter(pl.col(_RUN) == "filter_on"),
        flags_df.lazy().filter(pl.col(_RUN) == "filter_off"),
        expected_breaks_lf,
    ).collect()

    _put_csv(
        flags_df.select([_RUN] + _FLAG_COLUMNS).sort(pl.all()),
        summary_destination,
        "backtest_flags.csv",
    )
    _put_csv(
        summary_df.sort(DT.BACKTEST_KEYS), summary_destination, "backtest_summary.csv"
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--filter_on_source",
            "Source s3 directory for the published monthly tracker clean data",
        ),
        (
            "--filter_off_source",
            "Source s3 directory for the filter-off monthly tracker clean data",
        ),
        (
            "--summary_destination",
            "Destination s3 directory for the back-test flag and summary CSVs",
        ),
    )
    main(args.filter_on_source, args.filter_off_source, args.summary_destination)
