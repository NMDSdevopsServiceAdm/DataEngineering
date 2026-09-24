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

RATIO_TOO_LOW_COLUMN = "_ratio_too_low"
LOCATION_TOTAL_STAFF_COLUMN = "_location_total_staff"
LOCATION_PERMANENT_TEMPORARY_TOTAL_COLUMN = "_location_permanent_temporary_total"
LOCATION_RATIO_TOO_LOW_COLUMN = "_location_ratio_too_low"
ORG_TOTAL_STAFF_COLUMN = "_org_total_staff"
ORG_PERMANENT_TEMPORARY_TOTAL_COLUMN = "_org_permanent_temporary_total"
ORG_RATIO_TOO_LOW_COLUMN = "_org_ratio_too_low"


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
    one changes, all 5 survive.

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
    wherever its _clean counts are null, whether that's from the
    pre-existing dedup-staleness reason or from this stage's own
    ratio-too-low rules - so no separate _clean variant of the percentage
    columns is needed.

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


def _dedup_total_staff_expr() -> pl.Expr:
    """Sums a job-role row's 5 deduplicated employment status counts.

    Reconciles to worker_records_bounded for a populated row, but the 5
    _dedup columns are nulled together as a group for a row that's an
    unchanged repeat of its prior snapshot (see
    deduplicate_employment_status_counts), so a stale row's sum is null and
    drops out of any later `.sum()` over it entirely.
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


def _aggregate_dedup_totals_to_location(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Aggregates deduplicated employment status counts up to location grain.

    One row per (location_id, organisation_id, ascwds_workplace_import_date) -
    the small frame the location ratio is decided on, and that
    _aggregate_location_totals_to_org aggregates further from.
    """
    return lf.group_by(
        [
            IndCQC.location_id,
            IndCQC.organisation_id,
            IndCQC.ascwds_workplace_import_date,
        ]
    ).agg(
        _dedup_total_staff_expr().sum().alias(LOCATION_TOTAL_STAFF_COLUMN),
        _dedup_permanent_temporary_total_expr()
        .sum()
        .alias(LOCATION_PERMANENT_TEMPORARY_TOTAL_COLUMN),
    )


def _aggregate_location_totals_to_org(
    location_totals_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Aggregates location-grain totals up to org grain.

    Excludes null organisation_id locations first, so they can't be pooled
    into one fake org.
    """
    return (
        location_totals_lf.filter(pl.col(IndCQC.organisation_id).is_not_null())
        .group_by([IndCQC.organisation_id, IndCQC.ascwds_workplace_import_date])
        .agg(
            pl.col(LOCATION_TOTAL_STAFF_COLUMN).sum().alias(ORG_TOTAL_STAFF_COLUMN),
            pl.col(LOCATION_PERMANENT_TEMPORARY_TOTAL_COLUMN)
            .sum()
            .alias(ORG_PERMANENT_TEMPORARY_TOTAL_COLUMN),
        )
    )


def join_ratio_too_low_flags(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Aggregates deduplicated counts to location grain then org grain, decides
    both ratio-too-low flags on those small aggregated frames, and joins
    both flags back onto every job-role row.

    This is the "aggregate, filter, join back" shape, as an alternative to
    computing the same decision directly via `.over()` window functions on
    the (much wider) job-role-grain frame - see the over-vs-join skill for
    the trade-off being spiked here. Must run before
    null_counts_for_low_org_ratio/null_counts_for_low_location_ratio, which
    now just consume the joined columns rather than computing them.

    Joins on [location_id, organisation_id, ascwds_workplace_import_date],
    not just location_id + date: if a location ever had more than one
    distinct organisation_id across its job-role rows (a data
    inconsistency), joining on location_id + date alone would fan out every
    one of that location's rows across all matching organisation_id groups.
    `.over()` has no equivalent risk, since it never performs a join.
    `nulls_equal=True` is required on that final join: organisation_id can
    be null, and a plain join treats null != null (so a null-org location
    would never match its own row, silently losing both flags) - `.over()`
    has no equivalent gotcha either, since a null partition key is still one
    consistent group to a window function.

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            deduplicate_employment_status_counts.

    Returns:
        pl.LazyFrame: lf with _org_ratio_too_low and _location_ratio_too_low
            boolean columns added.
    """
    location_totals_lf = _aggregate_dedup_totals_to_location(lf)
    org_totals_lf = _aggregate_location_totals_to_org(location_totals_lf)

    org_flags_lf = org_totals_lf.with_columns(
        (
            (pl.col(ORG_TOTAL_STAFF_COLUMN) >= ORG_STAFF_THRESHOLD)
            & (
                pl.col(ORG_PERMANENT_TEMPORARY_TOTAL_COLUMN)
                / pl.col(ORG_TOTAL_STAFF_COLUMN)
                <= ORG_PERMANENT_TEMPORARY_RATIO_THRESHOLD
            )
        ).alias(ORG_RATIO_TOO_LOW_COLUMN)
    ).select(
        [
            IndCQC.organisation_id,
            IndCQC.ascwds_workplace_import_date,
            ORG_RATIO_TOO_LOW_COLUMN,
        ]
    )

    location_flags_lf = (
        location_totals_lf.with_columns(
            (
                (pl.col(LOCATION_TOTAL_STAFF_COLUMN) >= LOCATION_STAFF_THRESHOLD)
                & (
                    pl.col(LOCATION_PERMANENT_TEMPORARY_TOTAL_COLUMN)
                    / pl.col(LOCATION_TOTAL_STAFF_COLUMN)
                    <= LOCATION_PERMANENT_TEMPORARY_RATIO_THRESHOLD
                )
            ).alias(LOCATION_RATIO_TOO_LOW_COLUMN)
        )
        .join(
            org_flags_lf,
            on=[IndCQC.organisation_id, IndCQC.ascwds_workplace_import_date],
            how="left",
        )
        .with_columns(pl.col(ORG_RATIO_TOO_LOW_COLUMN).fill_null(False))
        .select(
            [
                IndCQC.location_id,
                IndCQC.organisation_id,
                IndCQC.ascwds_workplace_import_date,
                LOCATION_RATIO_TOO_LOW_COLUMN,
                ORG_RATIO_TOO_LOW_COLUMN,
            ]
        )
    )

    return lf.join(
        location_flags_lf,
        on=[
            IndCQC.location_id,
            IndCQC.organisation_id,
            IndCQC.ascwds_workplace_import_date,
        ],
        how="left",
        nulls_equal=True,
    )


def null_counts_for_low_org_ratio(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls an org's employment status clean counts where too few of its staff
    have a recorded permanent/temporary status, and sets
    employment_status_filtering_rule.

    Must run after join_ratio_too_low_flags, which computes
    _org_ratio_too_low, and before null_counts_for_low_location_ratio, which
    narrows the columns created here.

    The 5 _dedup columns are confirmed null/populated together as a group
    (see percentage_share_horizontal's docstring), so permanent_count_dedup
    is used as a stand-in for "is this row missing data".

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            join_ratio_too_low_flags.

    Returns:
        pl.LazyFrame: lf with a _clean column per deduplicated count column
            (the _dedup columns themselves are left untouched), plus
            employment_status_filtering_rule. The _clean columns are nulled
            for orgs with 10+ staff whose permanent+temporary workers make
            up 5% or less of that staff.
    """
    lf = lf.rename({ORG_RATIO_TOO_LOW_COLUMN: RATIO_TOO_LOW_COLUMN})
    lf = _create_clean_columns_where_ratio_too_low(
        lf, DEDUP_TO_CLEAN_COUNT_COLUMNS
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
    this narrows further, and consumes the _location_ratio_too_low column
    join_ratio_too_low_flags computed.

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            null_counts_for_low_org_ratio.

    Returns:
        pl.LazyFrame: lf with the _clean count columns further nulled, and
            employment_status_filtering_rule updated, for locations with
            10+ staff whose permanent+temporary workers make up 1% or less
            of that staff.
    """
    lf = lf.rename({LOCATION_RATIO_TOO_LOW_COLUMN: RATIO_TOO_LOW_COLUMN})
    lf = _null_clean_columns_where_ratio_too_low(
        lf, list(DEDUP_TO_CLEAN_COUNT_COLUMNS.values())
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
