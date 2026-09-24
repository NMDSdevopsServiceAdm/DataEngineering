import polars as pl

import projects._03_independent_cqc.utils.cleaning_utils as cleaningUtils
from polars_utils import filtering_utils
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EmploymentStatusFilteringRule

LOCATION_STAFF_THRESHOLD = 10
LOCATION_PERMANENT_TEMPORARY_RATIO_THRESHOLD = 0.01
ORG_STAFF_THRESHOLD = 10
ORG_PERMANENT_TEMPORARY_RATIO_THRESHOLD = 0.05

DEDUP_TO_CLEAN_COUNT_COLUMNS: dict[str, str] = {
    EmpStatus.permanent_count_dedup: EmpStatus.permanent_count_clean,
    EmpStatus.temporary_count_dedup: EmpStatus.temporary_count_clean,
    EmpStatus.bank_or_pool_count_dedup: EmpStatus.bank_or_pool_count_clean,
    EmpStatus.agency_count_dedup: EmpStatus.agency_count_clean,
    EmpStatus.other_count_dedup: EmpStatus.other_count_clean,
}

PERCENTAGE_TO_CLEAN_PERCENTAGE_COLUMNS: dict[str, str] = {
    EmpStatus.permanent_percentage: EmpStatus.permanent_percentage_clean,
    EmpStatus.temporary_percentage: EmpStatus.temporary_percentage_clean,
    EmpStatus.bank_or_pool_percentage: EmpStatus.bank_or_pool_percentage_clean,
    EmpStatus.agency_percentage: EmpStatus.agency_percentage_clean,
    EmpStatus.other_percentage: EmpStatus.other_percentage_clean,
}

RATIO_TOO_LOW_COLUMN = "_ratio_too_low"


def _create_clean_columns_where_ratio_too_low(
    lf: pl.LazyFrame, source_to_clean: dict[str, str]
) -> pl.LazyFrame:
    """Creates each clean column from its source, nulled wherever RATIO_TOO_LOW_COLUMN
    is True and copied from the source otherwise - leaving the source untouched."""
    return lf.with_columns(
        [
            pl.when(pl.col(RATIO_TOO_LOW_COLUMN))
            .then(None)
            .otherwise(pl.col(source))
            .alias(clean)
            for source, clean in source_to_clean.items()
        ]
    )


def _null_clean_columns_where_ratio_too_low(
    lf: pl.LazyFrame, clean_columns: list[str]
) -> pl.LazyFrame:
    """Further nulls each already-created clean column wherever RATIO_TOO_LOW_COLUMN
    is True, leaving it as-is otherwise."""
    return lf.with_columns(
        [
            pl.when(pl.col(RATIO_TOO_LOW_COLUMN))
            .then(None)
            .otherwise(pl.col(clean))
            .alias(clean)
            for clean in clean_columns
        ]
    )


def create_employment_status_percentage_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Deduplicates the 5 employment status count columns as a single unit, then
    adds a percentage-share column per employment status.

    A row's counts are only treated as a repeat of the prior row in its
    location/job-role timeline (and nulled) if all 5 are unchanged; if any one
    changes, all 5 survive. Percentage-share columns are then computed from the
    deduplicated counts, so a repeated (nulled) row's percentages are also null
    rather than carrying forward a stale share.

    Args:
        lf (pl.LazyFrame): dataset containing the merged employment status count
            columns.

    Returns:
        pl.LazyFrame: dataset with 5 "<count>_dedup" and 5
            "emplstat_<status>_percentage" columns added.
    """
    lf = cleaningUtils.remove_repeated_values_over_time_as_group(
        lf,
        columns_to_clean=[
            EmpStatus.permanent_count,
            EmpStatus.temporary_count,
            EmpStatus.bank_or_pool_count,
            EmpStatus.agency_count,
            EmpStatus.other_count,
        ],
        partition_by_columns=[IndCQC.location_id, IndCQC.published_job_role_label],
        date_column=IndCQC.cqc_location_import_date,
    )

    lf = cleaningUtils.percentage_share_horizontal(
        lf,
        columns=[
            EmpStatus.permanent_count_dedup,
            EmpStatus.temporary_count_dedup,
            EmpStatus.bank_or_pool_count_dedup,
            EmpStatus.agency_count_dedup,
            EmpStatus.other_count_dedup,
        ],
        output_columns=[
            EmpStatus.permanent_percentage,
            EmpStatus.temporary_percentage,
            EmpStatus.bank_or_pool_percentage,
            EmpStatus.agency_percentage,
            EmpStatus.other_percentage,
        ],
    )

    return lf


def _dedup_total_staff_expr() -> pl.Expr:
    """Sums a job-role row's 5 deduplicated employment status counts.

    Reconciles to worker_records_bounded for a populated row, but the 5
    _dedup columns are nulled together as a group for a row that's an
    unchanged repeat of its prior snapshot (see
    create_employment_status_percentage_columns), so a stale row's sum is
    null and drops out of any later `.sum()` over it entirely - unlike
    worker_records_bounded, which stays populated regardless. Using
    worker_records_bounded as the staff denominator let a stale row's
    numerator (permanent+temporary) shrink to null/0 while its share of the
    denominator stayed full, producing false positives for locations with
    genuinely stable, accurate staffing.
    """
    return (
        pl.col(EmpStatus.permanent_count_dedup)
        + pl.col(EmpStatus.temporary_count_dedup)
        + pl.col(EmpStatus.bank_or_pool_count_dedup)
        + pl.col(EmpStatus.agency_count_dedup)
        + pl.col(EmpStatus.other_count_dedup)
    )


def _dedup_permanent_temporary_total_expr() -> pl.Expr:
    """Sums a job-role row's permanent+temporary deduplicated counts."""
    return pl.col(EmpStatus.permanent_count_dedup) + pl.col(
        EmpStatus.temporary_count_dedup
    )


def null_counts_for_low_org_ratio(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls an org's employment status clean counts where too few of its staff
    have a recorded permanent/temporary status, and sets
    employment_status_filtering_rule.

    Must run before null_counts_for_low_location_ratio, which narrows the
    columns created here.

    The ticket's org rule is defined at org grain, but this data is at
    (location, published_job_role_label) grain - about 15 rows per
    location. This computes the equivalent of aggregating staff and
    permanent+temporary up to org grain, deciding org_ratio_too_low there,
    then joining that decision back onto every job-role row of the org -
    but as a broadcast window function (`.over()`) rather than an actual
    group_by + join, which costs several GB more peak memory for this kind
    of "attach one aggregate to every row" broadcast on a comparably-sized
    real frame (see the over-vs-join skill). A repeated job-role row (the
    same underlying ASCWDS submission seen through more than one CQC
    snapshot) is an exact duplicate, so it's already nulled by dedup's
    unchanged-since-prior-snapshot check - no separate distinct-row filter
    is needed to avoid double-counting it here.

    organisation_id can be null, so the rule is gated explicitly on it -
    otherwise `.over()` would pool unrelated null-org locations into one group.

    The 5 _dedup columns are confirmed null/populated together as a group
    (see percentage_share_horizontal's docstring), so permanent_count_dedup
    is used as a stand-in for "is this row missing data".

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            create_employment_status_percentage_columns.

    Returns:
        pl.LazyFrame: lf with a _clean column per deduplicated count column and
            per percentage column (the _dedup/percentage columns themselves
            are left untouched), plus employment_status_filtering_rule. The
            _clean columns are nulled for orgs with 10+ staff whose
            permanent+temporary workers make up 5% or less of that staff.
    """
    org_partition = [IndCQC.organisation_id, IndCQC.ascwds_workplace_import_date]

    org_total_staff = _dedup_total_staff_expr().sum().over(org_partition)
    org_permanent_temporary_total = (
        _dedup_permanent_temporary_total_expr().sum().over(org_partition)
    )

    org_ratio_too_low = (
        pl.col(IndCQC.organisation_id).is_not_null()
        & (org_total_staff >= ORG_STAFF_THRESHOLD)
        & (
            org_permanent_temporary_total / org_total_staff
            <= ORG_PERMANENT_TEMPORARY_RATIO_THRESHOLD
        )
    )

    # Materialised once: each window aggregation above would otherwise be
    # recomputed per clean column, since it's used in several expressions.
    lf = lf.with_columns(org_ratio_too_low.alias(RATIO_TOO_LOW_COLUMN))
    lf = _create_clean_columns_where_ratio_too_low(lf, DEDUP_TO_CLEAN_COUNT_COLUMNS)
    lf = _create_clean_columns_where_ratio_too_low(
        lf, PERCENTAGE_TO_CLEAN_PERCENTAGE_COLUMNS
    ).drop(RATIO_TOO_LOW_COLUMN)
    lf = filtering_utils.add_filtering_rule_column(
        lf,
        EmpStatus.filtering_rule,
        EmpStatus.permanent_count_dedup,
        EmploymentStatusFilteringRule.populated,
        EmploymentStatusFilteringRule.missing_data,
        categorical_type=CatColType.EmploymentStatusFilteringRuleCatType,
    )
    return filtering_utils.update_filtering_rule(
        lf,
        EmpStatus.filtering_rule,
        EmpStatus.permanent_count_dedup,
        EmpStatus.permanent_count_clean,
        EmploymentStatusFilteringRule.populated,
        EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio,
        categorical_type=CatColType.EmploymentStatusFilteringRuleCatType,
    )


def null_counts_for_low_location_ratio(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Further nulls a location's employment status clean counts where too few
    of its staff have a recorded permanent/temporary status, updating
    employment_status_filtering_rule where it's still 'populated'.

    Must run after null_counts_for_low_org_ratio, which creates the columns
    this narrows further. Same shape as the org rule (see its docstring):
    aggregates staff and permanent+temporary up to location grain, decides
    location_ratio_too_low there, and broadcasts it back onto every
    job-role row of the location via `.over()` instead of a join, for the
    same peak-memory reason.

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            null_counts_for_low_org_ratio.

    Returns:
        pl.LazyFrame: lf with the _clean count and percentage columns further
            nulled, and employment_status_filtering_rule updated, for
            locations with 10+ staff whose permanent+temporary workers make
            up 1% or less of that staff.
    """
    location_partition = [
        IndCQC.location_id,
        IndCQC.ascwds_workplace_import_date,
    ]

    location_total_staff = _dedup_total_staff_expr().sum().over(location_partition)
    location_permanent_temporary_total = (
        _dedup_permanent_temporary_total_expr().sum().over(location_partition)
    )

    location_ratio_too_low = (location_total_staff >= LOCATION_STAFF_THRESHOLD) & (
        location_permanent_temporary_total / location_total_staff
        <= LOCATION_PERMANENT_TEMPORARY_RATIO_THRESHOLD
    )

    # Materialised once: see null_counts_for_low_org_ratio for why.
    lf = lf.with_columns(location_ratio_too_low.alias(RATIO_TOO_LOW_COLUMN))
    lf = _null_clean_columns_where_ratio_too_low(
        lf,
        [
            *DEDUP_TO_CLEAN_COUNT_COLUMNS.values(),
            *PERCENTAGE_TO_CLEAN_PERCENTAGE_COLUMNS.values(),
        ],
    ).drop(RATIO_TOO_LOW_COLUMN)
    return filtering_utils.update_filtering_rule(
        lf,
        EmpStatus.filtering_rule,
        EmpStatus.permanent_count_dedup,
        EmpStatus.permanent_count_clean,
        EmploymentStatusFilteringRule.populated,
        EmploymentStatusFilteringRule.location_level_low_permanent_temporary_ratio,
        categorical_type=CatColType.EmploymentStatusFilteringRuleCatType,
    )
