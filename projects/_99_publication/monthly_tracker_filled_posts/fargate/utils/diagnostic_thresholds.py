"""
SPIKE (ticket 2107) prototype: automated thresholds for the checks in the
hand-made Monthly Tracking summary workbook.

Works on a generic long table - one row per metric, service type and period -
so the threshold methods don't depend on how each metric was built.
`to_metric_long_format` is the adapter from the monthly tracker's clean
output; the change functions add the bases the thresholds are tested on, and
each `flag_*` function draws a normal range (`lower`/`upper`) around a change
column and marks values outside it as `breached`.

Changes are net fractions throughout, matching clean_utils: 0.25 = +25%, and a
"1 percentage point" limit is 0.01.

Not wired into the pipeline - see the ticket 2107 findings for the
recommended pointblank wiring.
"""

from dataclasses import dataclass
from datetime import date

import polars as pl

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    clean_utils,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import PrimaryServiceType

# Series need at least this many prior values before the history-based
# methods (mean/std, median/MAD) will draw a band.
MIN_PRIOR_STEPS: int = 6


@dataclass
class DiagnosticColumns:
    metric: str = "metric"
    service_type: str = "service_type"
    period: str = "period"
    value: str = "value"
    interval_type: str = "interval_type"
    period_on_period_change: str = "period_on_period_change"
    change_since_march: str = "change_since_march"
    lower: str = "lower"
    upper: str = "upper"
    breached: str = "breached"
    insufficient_history: str = "insufficient_history"
    tier: str = "tier"
    method: str = "method"
    setting: str = "setting"
    basis: str = "basis"
    evaluated_periods: str = "evaluated_periods"
    false_alarms: str = "false_alarms"
    expected_breaks: str = "expected_breaks"
    detected_breaks: str = "detected_breaks"


@dataclass
class IntervalType:
    monthly: str = "monthly"
    quarterly: str = "quarterly"


@dataclass
class WorkbookServiceType:
    all_cqc_locations: str = "All CQC locations"
    care_homes: str = "CQC care homes"
    care_home_with_nursing: str = "CQC care home with nursing"
    care_home_without_nursing: str = "CQC care home without nursing"
    non_residential: str = "CQC non-residential"


@dataclass
class Tier:
    error: str = "error"
    warn: str = "warn"
    none: str = "none"
    insufficient_history: str = "insufficient_history"


@dataclass
class Metric:
    filled_posts: str = "filled_posts"
    location_count: str = "location_count"


# The monthly tracker's fixed-cohort assessment windows (see _02_clean).
ASSESSMENT_TERMS: list[str] = ["long_term", "medium_term", "short_term"]


def sfc_filled_posts_metric(term: str) -> str:
    """Metric name for an assessment window's SfC filled posts."""
    return f"sfc_filled_posts_{term}"


def ct_total_employed_metric(term: str) -> str:
    """Metric name for an assessment window's CT total employed."""
    return f"ct_total_employed_{term}"


def sfc_minus_ct_metric(term: str) -> str:
    """Metric name for an assessment window's SfC minus CT step-change gap."""
    return f"sfc_minus_ct_{term}"


Cols = DiagnosticColumns()

# The service types that aren't totals of the others - the only ones the
# cross-sectional method compares against each other.
BASE_SERVICE_TYPES: list[str] = [
    WorkbookServiceType.care_home_with_nursing,
    WorkbookServiceType.care_home_without_nursing,
    WorkbookServiceType.non_residential,
]

SERIES_KEYS: list[str] = [Cols.metric, Cols.service_type]

# Monthly tracker service type (including its rollup labels) -> workbook label.
_WORKBOOK_SERVICE_TYPES: dict[str, str] = {
    clean_utils._ALL_CQC_LOCATIONS: WorkbookServiceType.all_cqc_locations,
    clean_utils._ALL_CQC_CARE_HOMES: WorkbookServiceType.care_homes,
    PrimaryServiceType.care_home_with_nursing: WorkbookServiceType.care_home_with_nursing,
    PrimaryServiceType.care_home_only: WorkbookServiceType.care_home_without_nursing,
    PrimaryServiceType.non_residential: WorkbookServiceType.non_residential,
}

# The workbook only compares SfC with CT for these two groups.
_ASSESSMENT_SERVICE_TYPES: list[str] = [
    WorkbookServiceType.care_homes,
    WorkbookServiceType.non_residential,
]


def to_metric_long_format(
    clean_lf: pl.LazyFrame, window_from_dates: dict[str, date]
) -> pl.LazyFrame:
    """
    Reshapes the monthly tracker clean output into the diagnostics long table.

    Keeps only the national ("England", "All job roles") rows the workbook
    uses, relabelled to its five service types. Filled posts and location
    counts come from the publication columns. The SfC and CT fixed-cohort
    series come from each assessment window's columns, for care homes and
    non-res only, from that window's start date onwards.

    Args:
        clean_lf (pl.LazyFrame): monthly_tracker_filled_posts_02_clean data.
        window_from_dates (dict[str, date]): each assessment term's start
            date, keyed by term (see ASSESSMENT_TERMS), as set in _02_clean.

    Returns:
        pl.LazyFrame: metric, service_type, period and value columns.
    """
    national_lf = clean_lf.filter(
        (pl.col(IndCQC.current_region) == clean_utils._ENGLAND)
        & (pl.col(IndCQC.main_job_role_clean_labelled) == clean_utils._ALL_JOB_ROLES)
    ).select(
        pl.col(IndCQC.cqc_location_import_date).alias(Cols.period),
        pl.col(IndCQC.primary_service_type)
        .cast(pl.String)
        .replace_strict(_WORKBOOK_SERVICE_TYPES, default=None)
        .alias(Cols.service_type),
        pl.col(Pub.publication_filled_posts).alias(Metric.filled_posts),
        pl.col(Pub.publication_locationid_count).alias(Metric.location_count),
        *[
            pl.col(getattr(Pub, f"assessment_filled_posts_{term}")).alias(
                sfc_filled_posts_metric(term)
            )
            for term in ASSESSMENT_TERMS
        ],
        *[
            pl.col(getattr(Pub, f"assessment_ct_total_employed_{term}")).alias(
                ct_total_employed_metric(term)
            )
            for term in ASSESSMENT_TERMS
        ],
    )
    national_lf = national_lf.filter(pl.col(Cols.service_type).is_not_null())

    def _unpivot(lf: pl.LazyFrame, metrics: list[str]) -> pl.LazyFrame:
        return lf.unpivot(
            on=metrics,
            index=[Cols.service_type, Cols.period],
            variable_name=Cols.metric,
            value_name=Cols.value,
        ).select(
            Cols.metric,
            Cols.service_type,
            Cols.period,
            pl.col(Cols.value).cast(pl.Float64),
        )

    assessment_lf = national_lf.filter(
        pl.col(Cols.service_type).is_in(_ASSESSMENT_SERVICE_TYPES)
    )
    return pl.concat(
        [_unpivot(national_lf, [Metric.filled_posts, Metric.location_count])]
        + [
            _unpivot(
                assessment_lf.filter(pl.col(Cols.period) >= window_from_dates[term]),
                [sfc_filled_posts_metric(term), ct_total_employed_metric(term)],
            )
            for term in ASSESSMENT_TERMS
        ]
    )


def _month_index(date_col: str) -> pl.Expr:
    return pl.col(date_col).dt.year() * 12 + pl.col(date_col).dt.month()


def add_interval_type(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Labels each step in a series by the gap back to its previous period.

    The monthly tracker is monthly for the last two financial years and only
    quarterly (Jan/Apr/Jul/Oct) before that, and quarterly moves are naturally
    bigger, so the history-based methods band each interval type separately.
    A series' first period has no step, so it's left null, as is any gap
    that's neither one nor three months.

    Args:
        lf (pl.LazyFrame): long table with metric, service_type and period.

    Returns:
        pl.LazyFrame: lf with an interval_type column.
    """
    months_since_previous = _month_index(Cols.period) - _month_index(Cols.period).shift(
        1
    )
    return lf.with_columns(
        pl.when(months_since_previous == 1)
        .then(pl.lit(IntervalType.monthly))
        .when(months_since_previous == 3)
        .then(pl.lit(IntervalType.quarterly))
        .otherwise(None)
        .over(SERIES_KEYS, order_by=Cols.period)
        .alias(Cols.interval_type)
    )


def add_period_on_period_change(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds the change from each series' previous available period.

    Null for a series' first period, and when the previous value is exactly 0
    (an assessment window's filtered sum can legitimately be 0).

    Args:
        lf (pl.LazyFrame): long table with metric, service_type, period and
            value.

    Returns:
        pl.LazyFrame: lf with a period_on_period_change column.
    """
    previous_value = pl.col(Cols.value).shift(1)
    return lf.with_columns(
        pl.when(previous_value == 0)
        .then(None)
        .otherwise((pl.col(Cols.value) - previous_value) / previous_value)
        .over(SERIES_KEYS, order_by=Cols.period)
        .alias(Cols.period_on_period_change)
    )


def add_change_since_march(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds the change since end of March, as the workbook shows it.

    Each reporting year runs March to the following February, with March as
    its baseline (so March itself is 0.0). Where March isn't retained - the
    quarterly-only years - the reporting year's first retained period (April)
    is the baseline instead. Null when the baseline is exactly 0.

    Args:
        lf (pl.LazyFrame): long table with metric, service_type, period and
            value.

    Returns:
        pl.LazyFrame: lf with a change_since_march column.
    """
    reporting_year = (
        pl.when(pl.col(Cols.period).dt.month() >= 3)
        .then(pl.col(Cols.period).dt.year())
        .otherwise(pl.col(Cols.period).dt.year() - 1)
    )
    baseline_value = pl.col(Cols.value).first()
    return lf.with_columns(
        pl.when(baseline_value == 0)
        .then(None)
        .otherwise((pl.col(Cols.value) - baseline_value) / baseline_value)
        .over(SERIES_KEYS + [reporting_year], order_by=Cols.period)
        .alias(Cols.change_since_march)
    )


def add_sfc_ct_gap(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Builds the Skills for Care vs Capacity Tracker gap series.

    For each assessment window and service type, the gap is SfC's step change
    minus CT's step change in the same period. CT doesn't come from ASC-WDS,
    so SfC moving while CT doesn't points at our data rather than the sector.
    Null when either side is null or missing.

    Args:
        lf (pl.LazyFrame): long table with period_on_period_change and
            interval_type, including the sfc_filled_posts_<term> and
            ct_total_employed_<term> metrics.

    Returns:
        pl.LazyFrame: one sfc_minus_ct_<term> row per SfC row, with the gap in
            period_on_period_change.
    """
    term_col = "_term"
    ct_change_col = "_ct_change"
    sfc_metrics = {sfc_filled_posts_metric(term): term for term in ASSESSMENT_TERMS}
    ct_metrics = {ct_total_employed_metric(term): term for term in ASSESSMENT_TERMS}
    gap_metrics = {term: sfc_minus_ct_metric(term) for term in ASSESSMENT_TERMS}
    join_keys = [term_col, Cols.service_type, Cols.period]

    sfc_lf = lf.filter(pl.col(Cols.metric).is_in(list(sfc_metrics))).with_columns(
        pl.col(Cols.metric).replace_strict(sfc_metrics).alias(term_col)
    )
    ct_lf = (
        lf.filter(pl.col(Cols.metric).is_in(list(ct_metrics)))
        .with_columns(pl.col(Cols.metric).replace_strict(ct_metrics).alias(term_col))
        .select(join_keys + [pl.col(Cols.period_on_period_change).alias(ct_change_col)])
    )
    return sfc_lf.join(ct_lf, on=join_keys, how="left").select(
        pl.col(term_col).replace_strict(gap_metrics).alias(Cols.metric),
        Cols.service_type,
        Cols.period,
        Cols.interval_type,
        (pl.col(Cols.period_on_period_change) - pl.col(ct_change_col)).alias(
            Cols.period_on_period_change
        ),
    )


_ROW_COL: str = "_row"


def _history_partition(value_col: str) -> list[pl.Expr]:
    """
    Which earlier values a history-based method compares a value against.

    Step changes are compared within the same interval type. Since-March
    changes build up through the year, so they're only comparable with the
    same month in earlier years.

    Args:
        value_col (str): the change column being flagged.

    Returns:
        list[pl.Expr]: expressions identifying a value's comparison history.
    """
    if value_col == Cols.change_since_march:
        within_series = pl.col(Cols.period).dt.month()
    else:
        within_series = pl.col(Cols.interval_type)
    return [pl.col(key) for key in SERIES_KEYS] + [within_series]


def _flag_against_history(
    lf: pl.LazyFrame,
    value_col: str,
    k: float,
    centre: pl.Expr,
    spread: pl.Expr,
) -> pl.LazyFrame:
    """
    Bands each value at centre +/- k * spread of its own earlier values.

    Only strictly earlier periods count, so a band is exactly what would have
    been known on the day. Values with fewer than MIN_PRIOR_STEPS earlier
    values get no band and are marked insufficient_history instead.

    Args:
        lf (pl.LazyFrame): long table containing value_col.
        value_col (str): the change column to flag.
        k (float): how many spreads either side of the centre the band spans.
        centre (pl.Expr): aggregation of the prior values giving the band's
            centre, over a column named "_prior_value".
        spread (pl.Expr): aggregation of the prior values giving the band's
            half-width per unit of k, over "_prior_value".

    Returns:
        pl.LazyFrame: lf with lower, upper, breached and insufficient_history.
    """
    prior_period, prior_value = "_prior_period", "_prior_value"
    prior_count, centre_col, spread_col = "_prior_count", "_centre", "_spread"
    partition = _history_partition(value_col)
    key_cols = [f"_key_{index}" for index in range(len(partition))]

    keyed_lf = lf.with_row_index(_ROW_COL).with_columns(
        [expr.alias(key) for expr, key in zip(partition, key_cols)]
    )
    prior_lf = keyed_lf.filter(pl.col(value_col).is_not_null()).select(
        key_cols
        + [
            pl.col(Cols.period).alias(prior_period),
            pl.col(value_col).alias(prior_value),
        ]
    )
    stats_lf = (
        keyed_lf.select([_ROW_COL, Cols.period] + key_cols)
        .join(prior_lf, on=key_cols, how="inner")
        .filter(pl.col(prior_period) < pl.col(Cols.period))
        .group_by(_ROW_COL)
        .agg(
            pl.len().alias(prior_count),
            centre.alias(centre_col),
            spread.alias(spread_col),
        )
    )

    insufficient = pl.col(prior_count).fill_null(0) < MIN_PRIOR_STEPS
    has_band = ~insufficient & pl.col(value_col).is_not_null()
    return (
        keyed_lf.join(stats_lf, on=_ROW_COL, how="left")
        .with_columns(
            pl.when(has_band)
            .then(pl.col(centre_col) - k * pl.col(spread_col))
            .alias(Cols.lower),
            pl.when(has_band)
            .then(pl.col(centre_col) + k * pl.col(spread_col))
            .alias(Cols.upper),
            insufficient.alias(Cols.insufficient_history),
        )
        .with_columns(_breached(value_col))
        .sort(_ROW_COL)
        .select(
            lf.collect_schema().names()
            + [Cols.lower, Cols.upper, Cols.breached, Cols.insufficient_history]
        )
    )


def _breached(value_col: str) -> pl.Expr:
    # Strictly outside: a value exactly on a limit isn't flagged. Null where
    # there's no band.
    return (
        pl.when(pl.col(Cols.lower).is_not_null())
        .then(
            (pl.col(value_col) < pl.col(Cols.lower))
            | (pl.col(value_col) > pl.col(Cols.upper))
        )
        .alias(Cols.breached)
    )


def flag_mean_std(lf: pl.LazyFrame, value_col: str, k: float) -> pl.LazyFrame:
    """
    Method 1: flags values outside mean +/- k * standard deviation of the
    series' own earlier values (the same idea as clean_utils' CT dispersion
    filter).

    Args:
        lf (pl.LazyFrame): long table containing value_col.
        value_col (str): the change column to flag.
        k (float): band half-width in standard deviations.

    Returns:
        pl.LazyFrame: lf with lower, upper, breached and insufficient_history.
    """
    prior_value = pl.col("_prior_value")
    return _flag_against_history(
        lf, value_col, k, centre=prior_value.mean(), spread=prior_value.std()
    )


def flag_median_mad(lf: pl.LazyFrame, value_col: str, k: float) -> pl.LazyFrame:
    """
    Method 2: flags values outside median +/- k * median absolute deviation
    of the series' own earlier values.

    Unlike method 1, a single past outlier (e.g. the incident month itself)
    doesn't widen the band. Where every earlier value is identical the MAD is
    0, so any different value is flagged.

    Args:
        lf (pl.LazyFrame): long table containing value_col.
        value_col (str): the change column to flag.
        k (float): band half-width in MADs.

    Returns:
        pl.LazyFrame: lf with lower, upper, breached and insufficient_history.
    """
    prior_value = pl.col("_prior_value")
    return _flag_against_history(
        lf,
        value_col,
        k,
        centre=prior_value.median(),
        spread=(prior_value - prior_value.median()).abs().median(),
    )


def _limit_for_interval_type(limits: dict[str, float]) -> pl.Expr:
    return pl.col(Cols.interval_type).replace_strict(
        limits, default=None, return_dtype=pl.Float64
    )


def flag_fixed_band(
    lf: pl.LazyFrame, value_col: str, tolerances: dict[str, float]
) -> pl.LazyFrame:
    """
    Method 3: flags values outside a fixed +/- tolerance.

    Needs no history. Quarterly steps are naturally bigger than monthly ones,
    so each interval type has its own tolerance.

    Args:
        lf (pl.LazyFrame): long table containing value_col and interval_type.
        value_col (str): the change column to flag.
        tolerances (dict[str, float]): tolerance per interval type, as a net
            change fraction (0.02 = +/-2%).

    Returns:
        pl.LazyFrame: lf with lower, upper and breached.
    """
    tolerance = pl.when(pl.col(value_col).is_not_null()).then(
        _limit_for_interval_type(tolerances)
    )
    return lf.with_columns(
        (-tolerance).alias(Cols.lower), tolerance.alias(Cols.upper)
    ).with_columns(_breached(value_col))


def flag_cross_sectional(
    lf: pl.LazyFrame, value_col: str, limits: dict[str, float]
) -> pl.LazyFrame:
    """
    Method 4: flags a base service type moving away from the others.

    Each base type's value is compared with the median of the other base
    types' values for the same metric and period - a pipeline error usually
    hits one segment. Rollups (All CQC locations, CQC care homes) are totals
    of the base types, so they're neither flagged nor compared against. Null
    when no other base type has a value for that period.

    Args:
        lf (pl.LazyFrame): long table containing value_col and interval_type.
        value_col (str): the change column to flag.
        limits (dict[str, float]): the largest allowed gap per interval type,
            as a net change fraction (0.02 = 2 percentage points).

    Returns:
        pl.LazyFrame: lf with lower, upper and breached.
    """
    other_service_type, other_value, others_median = (
        "_other_service_type",
        "_other_value",
        "_others_median",
    )
    period_keys = [Cols.metric, Cols.period]
    keyed_lf = lf.with_row_index(_ROW_COL)
    base_lf = keyed_lf.filter(
        pl.col(Cols.service_type).is_in(BASE_SERVICE_TYPES)
        & pl.col(value_col).is_not_null()
    )
    others_lf = base_lf.select(
        period_keys
        + [
            pl.col(Cols.service_type).alias(other_service_type),
            pl.col(value_col).alias(other_value),
        ]
    )
    stats_lf = (
        base_lf.select([_ROW_COL, Cols.service_type] + period_keys)
        .join(others_lf, on=period_keys, how="inner")
        .filter(pl.col(other_service_type) != pl.col(Cols.service_type))
        .group_by(_ROW_COL)
        .agg(pl.col(other_value).median().alias(others_median))
    )
    limit = _limit_for_interval_type(limits)
    return (
        keyed_lf.join(stats_lf, on=_ROW_COL, how="left")
        .with_columns(
            (pl.col(others_median) - limit).alias(Cols.lower),
            (pl.col(others_median) + limit).alias(Cols.upper),
        )
        .with_columns(_breached(value_col))
        .sort(_ROW_COL)
        .select(lf.collect_schema().names() + [Cols.lower, Cols.upper, Cols.breached])
    )


def assign_tier(warn_breached_col: str, error_breached_col: str) -> pl.Expr:
    """
    Combines a sensitive (warn) and a strict (error) setting into one tier.

    "error" if the strict setting is breached, else "warn" if the sensitive
    one is, else "none". Where the warn setting couldn't be evaluated
    (insufficient history, or no value), "insufficient_history".

    Args:
        warn_breached_col (str): breached column from the warn setting.
        error_breached_col (str): breached column from the error setting.

    Returns:
        pl.Expr: string expression aliased to tier.
    """
    return (
        pl.when(pl.col(error_breached_col))
        .then(pl.lit(Tier.error))
        .when(pl.col(warn_breached_col))
        .then(pl.lit(Tier.warn))
        .when(pl.col(warn_breached_col).is_null())
        .then(pl.lit(Tier.insufficient_history))
        .otherwise(pl.lit(Tier.none))
        .alias(Cols.tier)
    )


BACKTEST_KEYS: list[str] = [
    Cols.method,
    Cols.setting,
    Cols.basis,
    Cols.metric,
    Cols.service_type,
]


def summarise_backtest(
    filter_on_flags_lf: pl.LazyFrame,
    filter_off_flags_lf: pl.LazyFrame,
    expected_breaks_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Scores each method and setting on the BD214 back-test.

    On filter-on data (the published figures) every flag is a false alarm.
    On filter-off data, a break is detected if its expected period is
    flagged. The output carries counts only - no metric values - so it's the
    only thing that needs to leave AWS.

    Args:
        filter_on_flags_lf (pl.LazyFrame): flag rows (BACKTEST_KEYS, period,
            breached) from the filter-on data.
        filter_off_flags_lf (pl.LazyFrame): the same from the filter-off data.
        expected_breaks_lf (pl.LazyFrame): metric, service_type and period of
            each break the filter-off data should show.

    Returns:
        pl.LazyFrame: one row per BACKTEST_KEYS combination with
            evaluated_periods, false_alarms, expected_breaks and
            detected_breaks.
    """
    flagged = pl.col(Cols.breached).fill_null(False)
    filter_on_summary_lf = filter_on_flags_lf.group_by(BACKTEST_KEYS).agg(
        pl.col(Cols.breached).is_not_null().sum().alias(Cols.evaluated_periods),
        flagged.sum().alias(Cols.false_alarms),
    )
    detection_lf = (
        filter_off_flags_lf.join(
            expected_breaks_lf, on=[Cols.metric, Cols.service_type, Cols.period]
        )
        .group_by(BACKTEST_KEYS)
        .agg(
            pl.len().alias(Cols.expected_breaks),
            flagged.sum().alias(Cols.detected_breaks),
        )
    )
    return (
        filter_on_summary_lf.join(detection_lf, on=BACKTEST_KEYS, how="left")
        .with_columns(
            pl.col(Cols.expected_breaks, Cols.detected_breaks)
            .fill_null(0)
            .cast(pl.UInt32)
        )
        .select(
            BACKTEST_KEYS
            + [
                Cols.evaluated_periods,
                Cols.false_alarms,
                Cols.expected_breaks,
                Cols.detected_breaks,
            ]
        )
    )
