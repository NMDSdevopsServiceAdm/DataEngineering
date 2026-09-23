import polars as pl

import projects._03_independent_cqc.utils.cleaning_utils as cleaningUtils
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


def null_employment_status_counts_where_org_permanent_temporary_ratio_is_too_low(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Creates the 5 employment status clean count columns from their _dedup
    counterparts, nulling an org's where too few of its staff have a recorded
    permanent/temporary status to trust the split, and sets
    employment_status_filtering_rule accordingly.

    Must run before
    null_employment_status_counts_where_location_permanent_temporary_ratio_is_too_low,
    which narrows the _clean/employment_status_filtering_rule columns this
    creates rather than re-deriving them.

    An org's total staff is worker_records_bounded summed once per distinct
    location under the org (not once per job-role row, which would inflate the
    total by however many job roles each location has), via
    `.filter(is_first_distinct)` rather than a join - a join broadcasting an
    aggregate back onto every row measurably costs more peak memory than
    `.over()`'s in-memory fallback for this fan-out-free case (see the
    over-vs-join skill). worker_records_bounded genuinely varies per
    location_id even under a shared establishment_id/organisation_id (a
    grouped-provider ASCWDS submission covering many locations), so location_id
    alone identifies a location here - no need to pair it with
    establishment_id.

    organisation_id can be null. `.over()` would otherwise group every
    null-organisation_id row together as if they were one org, so the rule is
    explicitly gated on organisation_id being populated - those rows are left
    to the location-level rule instead.

    A row's own _dedup counts can be null regardless of whether its org's
    ratio is too low (e.g. a job role with no matching worker records at all),
    so employment_status_filtering_rule is set to missing_data for those rows
    rather than populated - otherwise a null _clean column could be labelled
    populated whenever the rest of the org's rows keep the ratio above
    threshold.

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            create_employment_status_percentage_columns.

    Returns:
        pl.LazyFrame: lf with a _clean column added per deduplicated count
            column (nulled for orgs with 10+ staff whose permanent+temporary
            workers make up 5% or less of that staff), plus
            employment_status_filtering_rule.
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

    lf = lf.with_columns(
        [
            pl.when(org_ratio_too_low).then(None).otherwise(pl.col(dedup)).alias(clean)
            for dedup, clean in DEDUP_TO_CLEAN_COUNT_COLUMNS.items()
        ]
    )
    return lf.with_columns(
        pl.when(org_ratio_too_low)
        .then(
            pl.lit(
                EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio
            )
        )
        .when(pl.col(EmpStatus.permanent_count_dedup).is_null())
        .then(pl.lit(EmploymentStatusFilteringRule.missing_data))
        .otherwise(pl.lit(EmploymentStatusFilteringRule.populated))
        .cast(CatColType.EmploymentStatusFilteringRuleCatType)
        .alias(EmpStatus.filtering_rule)
    )


def null_employment_status_counts_where_location_permanent_temporary_ratio_is_too_low(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Further nulls a location's employment status clean count columns where too
    few of its staff have a recorded permanent/temporary status to trust the
    split, updating employment_status_filtering_rule where it's still
    'populated'.

    Must run after
    null_employment_status_counts_where_org_permanent_temporary_ratio_is_too_low,
    which creates the _clean/employment_status_filtering_rule columns this
    narrows further. Sums permanent+temporary across all of a location's
    job-role rows via `.over()`; worker_records_bounded is already
    location-wide, so needs no equivalent dedup step.

    Partitions on location_id alone (plus import date) - worker_records_bounded
    genuinely varies per location_id even under a shared establishment_id (a
    grouped-provider ASCWDS submission covering many locations), so
    establishment_id adds nothing here.

    Args:
        lf (pl.LazyFrame): merged employment status data, already processed by
            null_employment_status_counts_where_org_permanent_temporary_ratio_is_too_low.

    Returns:
        pl.LazyFrame: lf with the _clean count columns further nulled, and
            employment_status_filtering_rule updated, for locations with 10+
            staff whose permanent+temporary workers make up 1% or less of that
            staff.
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

    lf = lf.with_columns(
        [
            pl.when(location_ratio_too_low)
            .then(None)
            .otherwise(pl.col(clean))
            .alias(clean)
            for clean in DEDUP_TO_CLEAN_COUNT_COLUMNS.values()
        ]
    )
    return lf.with_columns(
        pl.when(
            location_ratio_too_low
            & (
                pl.col(EmpStatus.filtering_rule)
                == EmploymentStatusFilteringRule.populated
            )
        )
        .then(
            pl.lit(
                EmploymentStatusFilteringRule.location_level_low_permanent_temporary_ratio
            )
        )
        .otherwise(pl.col(EmpStatus.filtering_rule))
        .cast(CatColType.EmploymentStatusFilteringRuleCatType)
        .alias(EmpStatus.filtering_rule)
    )
