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

RAW_TO_CLEAN_COUNT_COLUMNS: dict[str, str] = {
    EmpStatus.permanent_count: EmpStatus.permanent_count_clean,
    EmpStatus.temporary_count: EmpStatus.temporary_count_clean,
    EmpStatus.bank_or_pool_count: EmpStatus.bank_or_pool_count_clean,
    EmpStatus.agency_count: EmpStatus.agency_count_clean,
    EmpStatus.other_count: EmpStatus.other_count_clean,
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


def deduplicate_employment_status_counts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Deduplicates the 5 employment status count columns as a single unit.

    A row's counts are only treated as a repeat of the prior row in its
    location/job-role timeline (and nulled) if all 5 are unchanged; if any
    one changes, all 5 survive. Not used by this branch's own ratio rules
    (see null_counts_for_low_org_ratio's docstring for why) - kept because
    the _dedup columns are a shared, reusable output other consumers rely
    on.

    Args:
        lf (pl.LazyFrame): dataset containing the merged employment status count
            columns.

    Returns:
        pl.LazyFrame: dataset with 5 "<count>_dedup" columns added.
    """
    return cleaningUtils.remove_repeated_values_over_time_as_group(
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


def create_employment_status_percentage_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a percentage-share column per employment status, computed from the
    _clean counts rather than the raw/dedup counts.

    Must run last, after both ratio rules: a row's percentages come out null
    wherever its _clean counts are null - so no separate _clean variant of
    the percentage columns is needed.

    Args:
        lf (pl.LazyFrame): dataset already processed by
            null_counts_for_low_location_ratio (has the 5 "<count>_clean"
            columns).

    Returns:
        pl.LazyFrame: dataset with 5 "emplstat_<status>_percentage" columns added.
    """
    return cleaningUtils.percentage_share_horizontal(
        lf,
        columns=[
            EmpStatus.permanent_count_clean,
            EmpStatus.temporary_count_clean,
            EmpStatus.bank_or_pool_count_clean,
            EmpStatus.agency_count_clean,
            EmpStatus.other_count_clean,
        ],
        output_columns=[
            EmpStatus.permanent_percentage,
            EmpStatus.temporary_percentage,
            EmpStatus.bank_or_pool_percentage,
            EmpStatus.agency_percentage,
            EmpStatus.other_percentage,
        ],
    )


def _total_staff_expr() -> pl.Expr:
    """Sums a job-role row's 5 raw employment status counts.

    Reconciles to worker_records_bounded: every worker record falls into
    exactly one of the 5 employment status counts.
    """
    return (
        pl.col(EmpStatus.permanent_count)
        + pl.col(EmpStatus.temporary_count)
        + pl.col(EmpStatus.bank_or_pool_count)
        + pl.col(EmpStatus.agency_count)
        + pl.col(EmpStatus.other_count)
    )


def _permanent_temporary_total_expr() -> pl.Expr:
    """Sums a job-role row's raw permanent+temporary counts."""
    return pl.col(EmpStatus.permanent_count) + pl.col(EmpStatus.temporary_count)


def null_counts_for_low_org_ratio(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls an org's employment status clean counts where too few of its staff
    have a recorded permanent/temporary status, and sets
    employment_status_filtering_rule.

    Must run before null_counts_for_low_location_ratio, which narrows the
    columns created here.

    Uses raw counts, not _dedup: _dedup nulls a job-role row whenever it's
    unchanged from its prior snapshot (a "hasn't resubmitted this period"
    signal, unrelated to whether the data is real and recorded). Summed
    across an org's many locations, that means only the subset of
    locations that happened to resubmit *this specific period* would count
    towards the org's total staff - a stable, accurately-staffed org where
    only some locations resubmit in a given month would have its total
    understated by however many locations stayed quiet, potentially
    swinging the ratio decision on submission timing rather than actual
    coverage. Raw carries a location's last-known values forward
    regardless of whether they changed, so the org total reflects its real
    footprint. Confirmed via Athena that (location_id, published_job_role_
    label, ascwds_workplace_import_date) is already a unique key across the
    full dataset - no repeated-row double-counting risk from summing raw
    directly, so no distinct-row guard is needed here.

    Org staff is summed across the org's job-role rows via `.over()`, not a
    join, which costs more peak memory for this kind of broadcast (see the
    over-vs-join skill).

    organisation_id can be null, so the rule is gated explicitly on it -
    otherwise `.over()` would pool unrelated null-org locations into one group.

    permanent_count is used as the stand-in for "is this row missing data":
    unlike _dedup, raw is null only when the job role's employment status
    was never recorded at all, not because it's unchanged since last time.

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            deduplicate_employment_status_counts.

    Returns:
        pl.LazyFrame: lf with a _clean column per raw count column (the raw
            columns themselves are left untouched), plus
            employment_status_filtering_rule. The _clean columns are nulled
            for orgs with 10+ staff whose permanent+temporary workers make
            up 5% or less of that staff.
    """
    org_partition = [IndCQC.organisation_id, IndCQC.ascwds_workplace_import_date]

    org_total_staff = _total_staff_expr().sum().over(org_partition)
    org_permanent_temporary_total = (
        _permanent_temporary_total_expr().sum().over(org_partition)
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
    lf = _create_clean_columns_where_ratio_too_low(lf, RAW_TO_CLEAN_COUNT_COLUMNS).drop(
        RATIO_TOO_LOW_COLUMN
    )
    lf = filtering_utils.add_filtering_rule_column(
        lf,
        EmpStatus.filtering_rule,
        EmpStatus.permanent_count,
        EmploymentStatusFilteringRule.populated,
        EmploymentStatusFilteringRule.missing_data,
        categorical_type=CatColType.EmploymentStatusFilteringRuleCatType,
    )
    return filtering_utils.update_filtering_rule(
        lf,
        EmpStatus.filtering_rule,
        EmpStatus.permanent_count,
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
    sums raw staff and permanent+temporary across a location's job-role
    rows via `.over()`.

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            null_counts_for_low_org_ratio.

    Returns:
        pl.LazyFrame: lf with the _clean count columns further nulled, and
            employment_status_filtering_rule updated, for locations with
            10+ staff whose permanent+temporary workers make up 1% or less
            of that staff.
    """
    location_partition = [
        IndCQC.location_id,
        IndCQC.ascwds_workplace_import_date,
    ]

    location_total_staff = _total_staff_expr().sum().over(location_partition)
    location_permanent_temporary_total = (
        _permanent_temporary_total_expr().sum().over(location_partition)
    )

    location_ratio_too_low = (location_total_staff >= LOCATION_STAFF_THRESHOLD) & (
        location_permanent_temporary_total / location_total_staff
        <= LOCATION_PERMANENT_TEMPORARY_RATIO_THRESHOLD
    )

    # Materialised once: see null_counts_for_low_org_ratio for why.
    lf = lf.with_columns(location_ratio_too_low.alias(RATIO_TOO_LOW_COLUMN))
    lf = _null_clean_columns_where_ratio_too_low(
        lf, list(RAW_TO_CLEAN_COUNT_COLUMNS.values())
    ).drop(RATIO_TOO_LOW_COLUMN)
    return filtering_utils.update_filtering_rule(
        lf,
        EmpStatus.filtering_rule,
        EmpStatus.permanent_count,
        EmpStatus.permanent_count_clean,
        EmploymentStatusFilteringRule.populated,
        EmploymentStatusFilteringRule.location_level_low_permanent_temporary_ratio,
        categorical_type=CatColType.EmploymentStatusFilteringRuleCatType,
    )
