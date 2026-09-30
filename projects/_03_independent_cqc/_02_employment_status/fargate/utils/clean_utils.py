import polars as pl

import projects._03_independent_cqc.utils.cleaning_utils as cleaningUtils
from polars_utils import filtering_utils
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EmploymentStatusFilteringRule

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
    Creates a _clean copy of each _dedup count and starts
    employment_status_filtering_rule.

    The rule is 'populated' where the counts are present and 'missing_data'
    where they're null. The 5 _dedup columns are null together, so
    permanent_count_dedup stands in for all of them.

    Args:
        lf (pl.LazyFrame): dataset with the 5 "<count>_dedup" columns.

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
    Adds a percentage-share column per employment status from the _clean counts.

    Percentages are null wherever the _clean counts are null, so no separate
    _clean variant is needed.

    Args:
        lf (pl.LazyFrame): dataset with the 5 "<count>_clean" columns.

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
    Nulls an org's _clean counts where permanent+temporary is 5% or less of its
    staff, and updates employment_status_filtering_rule where still 'populated'.

    The ratio uses raw counts, not _dedup: dedup nulls every row of a workplace
    whose counts are unchanged, so a small stable location would drop out of the
    org total. (location_id, published_job_role_label,
    ascwds_workplace_import_date) is a unique key, so raw counts can't
    double-count.

    Args:
        lf (pl.LazyFrame): dataset with the raw and _clean counts and
            employment_status_filtering_rule.

    Returns:
        pl.LazyFrame: lf with the _clean counts nulled and the rule updated for
            flagged orgs.
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
