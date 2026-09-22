import polars as pl

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


def seed_employment_status_clean_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Copies each raw employment status count column into its clean counterpart.

    Also seeds the shared filtering rule column as populated/missing_data, ahead
    of downstream rules narrowing it further.

    Args:
        lf (pl.LazyFrame): merged employment status data with raw
            emplstat_*_count columns.

    Returns:
        pl.LazyFrame: lf with a _clean column added per raw count column, plus
            the employment_status_filtering_rule column.
    """
    lf = lf.with_columns(
        [pl.col(raw).alias(clean) for raw, clean in RAW_TO_CLEAN_COUNT_COLUMNS.items()]
    )
    return filtering_utils.add_filtering_rule_column(
        lf,
        EmpStatus.filtering_rule,
        EmpStatus.permanent_count,
        EmploymentStatusFilteringRule.populated,
        EmploymentStatusFilteringRule.missing_data,
        categorical_type=CatColType.EmploymentStatusFilteringRuleCatType,
    )


def null_employment_status_counts_where_org_permanent_temporary_ratio_is_too_low(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Nulls an org's employment status clean counts where too few of its staff
    have a recorded permanent/temporary status to trust the split.

    An org's total staff is worker_records_bounded summed once per distinct
    location under the org (not once per job-role row, which would inflate the
    total by however many job roles each location has), via
    `.filter(is_first_distinct)` rather than a join - a join broadcasting an
    aggregate back onto every row measurably costs more peak memory than
    `.over()`'s in-memory fallback for this fan-out-free case (see the
    over-vs-join skill).

    Args:
        lf (pl.LazyFrame): merged employment status data, already seeded by
            seed_employment_status_clean_columns.

    Returns:
        pl.LazyFrame: lf with the _clean count columns nulled, and
            employment_status_filtering_rule updated, for orgs with 10+ staff
            whose permanent+temporary workers make up 5% or less of that staff.
    """
    org_partition = [IndCQC.organisation_id, IndCQC.ascwds_workplace_import_date]
    location_key_is_first_distinct = pl.struct(
        [IndCQC.location_id, IndCQC.establishment_id]
    ).is_first_distinct()

    org_total_staff = (
        pl.col(IndCQC.worker_records_bounded)
        .filter(location_key_is_first_distinct)
        .sum()
        .over(org_partition)
    )
    org_permanent_temporary_total = (
        (pl.col(EmpStatus.permanent_count) + pl.col(EmpStatus.temporary_count))
        .sum()
        .over(org_partition)
    )

    org_ratio_too_low = (org_total_staff >= ORG_STAFF_THRESHOLD) & (
        org_permanent_temporary_total / org_total_staff
        <= ORG_PERMANENT_TEMPORARY_RATIO_THRESHOLD
    )

    lf = lf.with_columns(
        [
            pl.when(org_ratio_too_low).then(None).otherwise(pl.col(clean)).alias(clean)
            for clean in RAW_TO_CLEAN_COUNT_COLUMNS.values()
        ]
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


def null_employment_status_counts_where_location_permanent_temporary_ratio_is_too_low(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Nulls a location's employment status clean counts where too few of its
    staff have a recorded permanent/temporary status to trust the split.

    Sums permanent+temporary across all of a location's job-role rows via
    `.over()`; worker_records_bounded is already location-wide, so needs no
    equivalent dedup step.

    Args:
        lf (pl.LazyFrame): merged employment status data, already seeded by
            seed_employment_status_clean_columns.

    Returns:
        pl.LazyFrame: lf with the _clean count columns nulled, and
            employment_status_filtering_rule updated, for locations with 10+
            staff whose permanent+temporary workers make up 1% or less of that
            staff.
    """
    location_partition = [
        IndCQC.location_id,
        IndCQC.establishment_id,
        IndCQC.ascwds_workplace_import_date,
    ]
    location_permanent_temporary_total = (
        (pl.col(EmpStatus.permanent_count) + pl.col(EmpStatus.temporary_count))
        .sum()
        .over(location_partition)
    )

    location_ratio_too_low = (
        pl.col(IndCQC.worker_records_bounded) >= LOCATION_STAFF_THRESHOLD
    ) & (
        location_permanent_temporary_total / pl.col(IndCQC.worker_records_bounded)
        <= LOCATION_PERMANENT_TEMPORARY_RATIO_THRESHOLD
    )

    lf = lf.with_columns(
        [
            pl.when(location_ratio_too_low)
            .then(None)
            .otherwise(pl.col(clean))
            .alias(clean)
            for clean in RAW_TO_CLEAN_COUNT_COLUMNS.values()
        ]
    )
    return filtering_utils.update_filtering_rule(
        lf,
        EmpStatus.filtering_rule,
        EmpStatus.permanent_count,
        EmpStatus.permanent_count_clean,
        EmploymentStatusFilteringRule.populated,
        EmploymentStatusFilteringRule.location_level_low_permanent_temporary_ratio,
        categorical_type=CatColType.EmploymentStatusFilteringRuleCatType,
    )
