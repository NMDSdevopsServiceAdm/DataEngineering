import polars as pl

import projects._03_independent_cqc.utils.cleaning_utils as cleaningUtils
from polars_utils import filtering_utils
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EmploymentStatusFilteringRule

LOCATION_PERMANENT_TEMPORARY_RATIO_THRESHOLD = 0.01
ORG_PERMANENT_TEMPORARY_RATIO_THRESHOLD = 0.05

DEDUP_TO_CLEAN_COUNT_COLUMNS: dict[str, str] = {
    EmpStatus.permanent_count_dedup: EmpStatus.permanent_count_clean,
    EmpStatus.temporary_count_dedup: EmpStatus.temporary_count_clean,
    EmpStatus.bank_or_pool_count_dedup: EmpStatus.bank_or_pool_count_clean,
    EmpStatus.agency_count_dedup: EmpStatus.agency_count_clean,
    EmpStatus.other_count_dedup: EmpStatus.other_count_clean,
}
RAW_COUNT_COLUMNS = [
    EmpStatus.permanent_count,
    EmpStatus.temporary_count,
    EmpStatus.bank_or_pool_count,
    EmpStatus.agency_count,
    EmpStatus.other_count,
]
CLEAN_COUNT_COLUMNS = list(DEDUP_TO_CLEAN_COUNT_COLUMNS.values())


def deduplicate_employment_status_counts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls the 5 employment status count columns where a workplace's counts are stale.

    Staleness is judged at workplace level (location_id + import date), not per
    job role. Each role's counts are compared with its own prior row, and all
    roles' rows for a workplace/date are kept if any role changed (a true repeat
    = no role changed).

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
        workplace_columns=[IndCQC.location_id, IndCQC.cqc_location_import_date],
    )


def create_clean_count_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Creates a _clean column per count as a copy of its _dedup column, and starts
    employment_status_filtering_rule.

    The rule is 'populated' where the counts are present and 'missing_data'
    where they're null. The 5 _dedup columns are confirmed null/populated
    together as a group (see percentage_share_horizontal's docstring), so
    permanent_count_dedup is used as a stand-in for "is this row missing data".
    Later ratio rules null the _clean columns and update the rule.

    Args:
        lf (pl.LazyFrame): dataset already processed by
            deduplicate_employment_status_counts.

    Returns:
        pl.LazyFrame: lf with 5 "<count>_clean" columns and
            employment_status_filtering_rule added.
    """
    lf = lf.with_columns(
        [
            pl.col(dedup).alias(clean)
            for dedup, clean in DEDUP_TO_CLEAN_COUNT_COLUMNS.items()
        ]
    )
    return filtering_utils.add_filtering_rule_column(
        lf,
        EmpStatus.filtering_rule,
        EmpStatus.permanent_count_dedup,
        EmploymentStatusFilteringRule.populated,
        EmploymentStatusFilteringRule.missing_data,
        categorical_type=CatColType.EmploymentStatusFilteringRuleCatType,
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


def null_counts_for_low_org_ratio(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls an org's employment status clean counts where too few of its staff
    have a recorded permanent/temporary status, and updates
    employment_status_filtering_rule where it's still 'populated'.

    Must run after create_clean_count_columns. Should run before
    null_counts_for_low_location_ratio: the two ratio calculations are
    independent (each reads source counts, not the _clean columns), but only the
    first rule to fail a row records its reason, so running org first records the
    org-level reason when both fail.

    The ticket's org rule is defined at org grain, but this data is at
    (location, published_job_role_label) grain - see
    cleaningUtils.null_columns_where_group_share_too_low, which sums up to org
    grain and broadcasts the decision back to every job-role row of the org.

    The ratio uses raw counts, not _dedup: _dedup nulls every row of a workplace
    whose counts are unchanged since its prior snapshot, so summing it across an
    org would only count the locations that changed this period, and a small
    stable location would drop out of the org total. (location_id,
    published_job_role_label, ascwds_workplace_import_date) is a unique key, so
    summing raw counts can't double-count. The _clean columns are created from
    _dedup, so unchanged rows stay null.

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            create_clean_count_columns.

    Returns:
        pl.LazyFrame: lf with the _clean count columns nulled, and
            employment_status_filtering_rule updated, for orgs whose
            permanent+temporary workers make up 5% or less of their staff.
    """
    lf = cleaningUtils.null_columns_where_group_share_too_low(
        lf,
        partition_by_columns=[
            IndCQC.organisation_id,
            IndCQC.ascwds_workplace_import_date,
        ],
        total_columns=RAW_COUNT_COLUMNS,
        share_columns=[EmpStatus.permanent_count, EmpStatus.temporary_count],
        columns_to_null=CLEAN_COUNT_COLUMNS,
        maximum_share=ORG_PERMANENT_TEMPORARY_RATIO_THRESHOLD,
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

    Run after create_clean_count_columns, which creates the _clean columns
    this narrows further. The ratio doesn't depend on the org rule's result
    (it reads the _dedup counts), but running second means the org-level reason
    wins when a row fails both. Same shape as the org rule otherwise.

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            null_counts_for_low_org_ratio.

    Returns:
        pl.LazyFrame: lf with the _clean count columns further nulled, and
            employment_status_filtering_rule updated, for locations whose
            permanent+temporary workers make up 1% or less of their staff.
    """
    lf = cleaningUtils.null_columns_where_group_share_too_low(
        lf,
        partition_by_columns=[
            IndCQC.location_id,
            IndCQC.ascwds_workplace_import_date,
        ],
        total_columns=list(DEDUP_TO_CLEAN_COUNT_COLUMNS),
        share_columns=[
            EmpStatus.permanent_count_dedup,
            EmpStatus.temporary_count_dedup,
        ],
        columns_to_null=CLEAN_COUNT_COLUMNS,
        maximum_share=LOCATION_PERMANENT_TEMPORARY_RATIO_THRESHOLD,
    )
    return filtering_utils.update_filtering_rule(
        lf,
        EmpStatus.filtering_rule,
        EmpStatus.permanent_count_dedup,
        EmpStatus.permanent_count_clean,
        EmploymentStatusFilteringRule.populated,
        EmploymentStatusFilteringRule.location_level_low_permanent_temporary_ratio,
        categorical_type=CatColType.EmploymentStatusFilteringRuleCatType,
    )
