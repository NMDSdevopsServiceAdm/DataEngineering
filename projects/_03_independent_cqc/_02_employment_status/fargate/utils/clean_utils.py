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

PERCENTAGE_COLUMNS: list[str] = [
    EmpStatus.permanent_percentage,
    EmpStatus.temporary_percentage,
    EmpStatus.bank_or_pool_percentage,
    EmpStatus.agency_percentage,
    EmpStatus.other_percentage,
]

RATIO_TOO_LOW_COLUMN = "_ratio_too_low"


def _null_same_named_columns_where_ratio_too_low(
    lf: pl.LazyFrame, columns: list[str]
) -> pl.LazyFrame:
    """Nulls each of `columns` in place wherever RATIO_TOO_LOW_COLUMN is True."""
    return lf.with_columns(
        [
            pl.when(pl.col(RATIO_TOO_LOW_COLUMN))
            .then(None)
            .otherwise(pl.col(column))
            .alias(column)
            for column in columns
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


def null_counts_for_low_org_ratio(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls an org's employment status clean counts where too few of its staff
    have a recorded permanent/temporary status, and sets
    employment_status_filtering_rule.

    Must run before null_counts_for_low_location_ratio, which narrows the
    columns created here.

    Org staff is worker_records_bounded summed once per distinct location_id
    (not per job-role row, or the total inflates with job-role count). Uses
    `.filter(is_first_distinct)` rather than a join, which costs more peak
    memory for this kind of broadcast (see the over-vs-join skill).
    location_id alone is enough: worker_records_bounded varies per location_id
    even under a shared establishment_id (a grouped-provider submission).

    Partitions by ascwds_workplace_import_date, not cqc_location_import_date
    (which create_employment_status_percentage_columns dedups on) - that's
    the date the staff data is actually from. A repeat ASCWDS submission seen
    through several CQC snapshots is already nulled by that dedup, so it
    won't be double-counted here.

    organisation_id can be null, so the rule is gated explicitly on it -
    otherwise `.over()` would pool unrelated null-org locations into one group.

    The 5 _dedup columns are confirmed null/populated together as a group
    (see percentage_share_horizontal's docstring), so permanent_count_dedup
    is used as a stand-in for "is this row missing data".

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            create_employment_status_percentage_columns.

    Returns:
        pl.LazyFrame: lf with a _clean column per deduplicated count column,
            plus employment_status_filtering_rule. Both the _clean and
            percentage columns are nulled for orgs with 10+ staff whose
            permanent+temporary workers make up 5% or less of that staff.
    """
    org_partition = [IndCQC.organisation_id, IndCQC.ascwds_workplace_import_date]
    location_is_first_distinct = pl.col(IndCQC.location_id).is_first_distinct()

    org_total_staff = (
        pl.col(IndCQC.worker_records_bounded)
        .filter(location_is_first_distinct)
        .sum()
        .over(org_partition)
    )
    org_permanent_temporary_total = (
        (
            pl.col(EmpStatus.permanent_count_dedup)
            + pl.col(EmpStatus.temporary_count_dedup)
        )
        .sum()
        .over(org_partition)
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
    # recomputed per _clean column, since it's used in several expressions.
    lf = lf.with_columns(org_ratio_too_low.alias(RATIO_TOO_LOW_COLUMN))
    lf = lf.with_columns(
        [
            pl.when(pl.col(RATIO_TOO_LOW_COLUMN))
            .then(None)
            .otherwise(pl.col(dedup))
            .alias(clean)
            for dedup, clean in DEDUP_TO_CLEAN_COUNT_COLUMNS.items()
        ]
    )
    lf = _null_same_named_columns_where_ratio_too_low(lf, PERCENTAGE_COLUMNS).drop(
        RATIO_TOO_LOW_COLUMN
    )
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
    this narrows further. Sums permanent+temporary across a location's
    job-role rows via `.over()`; worker_records_bounded is already
    location-wide, so no dedup step is needed here (unlike the org rule,
    which sums across locations too).

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            null_counts_for_low_org_ratio.

    Returns:
        pl.LazyFrame: lf with the _clean and percentage columns further
            nulled, and employment_status_filtering_rule updated, for
            locations with 10+ staff whose permanent+temporary workers make
            up 1% or less of that staff.
    """
    location_partition = [
        IndCQC.location_id,
        IndCQC.ascwds_workplace_import_date,
    ]
    location_permanent_temporary_total = (
        (
            pl.col(EmpStatus.permanent_count_dedup)
            + pl.col(EmpStatus.temporary_count_dedup)
        )
        .sum()
        .over(location_partition)
    )

    location_ratio_too_low = (
        pl.col(IndCQC.worker_records_bounded) >= LOCATION_STAFF_THRESHOLD
    ) & (
        location_permanent_temporary_total / pl.col(IndCQC.worker_records_bounded)
        <= LOCATION_PERMANENT_TEMPORARY_RATIO_THRESHOLD
    )

    # Materialised once: see null_counts_for_low_org_ratio for why.
    lf = lf.with_columns(location_ratio_too_low.alias(RATIO_TOO_LOW_COLUMN))
    lf = _null_same_named_columns_where_ratio_too_low(
        lf, [*DEDUP_TO_CLEAN_COUNT_COLUMNS.values(), *PERCENTAGE_COLUMNS]
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
