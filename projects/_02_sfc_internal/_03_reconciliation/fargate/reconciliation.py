import sys
from datetime import date

import polars as pl

from polars_utils import cleaning_utils as cUtils
from polars_utils import utils
from projects._02_sfc_internal.utils.utils import add_parents_or_singles_and_subs_column
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.reconciliation_columns import (
    ReconciliationColumns as ReconColumn,
)
from utils.column_values.ascwds_labelled_vocab import (
    REGION_ID_CODE_TO_LABEL,
    IsParent,
    MainServiceID,
    RegistrationType,
)
from utils.column_values.categorical_column_values import (
    ParentsOrSinglesAndSubs,
    RegistrationStatus,
    SingleSubDescription,
    Subject,
)

reconciliation_labels_dict = {AWPClean.region_id: REGION_ID_CODE_TO_LABEL}


def main(
    cqc_locations_snapshot_source: str,
    ascwds_workplace_source: str,
    reconciliation_single_and_subs_destination: str,
    reconciliation_parents_destination: str,
) -> None:
    """Builds the ASC-WDS reconciliation reports for deregistered CQC locations.

    Produces two manual-review reports: one for single/sub accounts and one for
    parent accounts, each listing ASC-WDS workplaces associated with CQC locations
    that have deregistered (or have no registration status) and need following up.

    Args:
        cqc_locations_snapshot_source (str): Source s3 directory for the latest run
            CQC locations snapshot dataset.
        ascwds_workplace_source (str): Source s3 directory for the ASC-WDS workplace
            parquet dataset.
        reconciliation_single_and_subs_destination (str): Destination s3 directory
            for the singles and subs reconciliation report.
        reconciliation_parents_destination (str): Destination s3 directory for the
            parents reconciliation report.
    """
    cqc_location_lf = utils.scan_parquet(cqc_locations_snapshot_source)
    ascwds_workplace_lf = utils.scan_parquet(ascwds_workplace_source)

    ascwds_workplace_lf = ascwds_workplace_lf.filter(
        pl.col(AWPClean.workplace_last_active_date) >= pl.col(AWPClean.purge_date)
    ).drop(AWPClean.workplace_last_active_date, AWPClean.purge_date)

    (
        first_of_most_recent_month,
        first_of_previous_month,
    ) = get_reconciliation_month_boundaries(cqc_location_lf)

    (
        cqc_registered_workplace_lf,
        ascwds_parent_accounts_lf,
    ) = prepare_latest_cleaned_ascwds_workforce_data(ascwds_workplace_lf)

    cqc_location_lf = cqc_location_lf.select(
        CQCL.location_id, CQCL.registration_status, CQCL.deregistration_date
    )

    merged_ascwds_cqc_lf = cqc_registered_workplace_lf.rename(
        {AWPClean.location_id: CQCL.location_id}
    ).join(cqc_location_lf, on=CQCL.location_id, how="left")

    # Collected here to avoid recomputing the scan/join/filter chain for each of the two output branches below.
    reconciliation_df = filter_to_locations_relevant_to_reconciliation_process(
        merged_ascwds_cqc_lf, first_of_most_recent_month, first_of_previous_month
    ).collect()

    single_and_sub_lf = build_single_and_subs_output(reconciliation_df.lazy())
    parents_lf = build_parents_output(
        ascwds_parent_accounts_lf, reconciliation_df.lazy(), first_of_previous_month
    )

    utils.sink_to_parquet(single_and_sub_lf, reconciliation_single_and_subs_destination)
    utils.sink_to_parquet(parents_lf, reconciliation_parents_destination)


def get_reconciliation_month_boundaries(
    cqc_location_lf: pl.LazyFrame,
) -> tuple[date, date]:
    """Returns the first day of the most recent import month and of the month before it.

    Args:
        cqc_location_lf (pl.LazyFrame): CQC locations snapshot data.

    Returns:
        tuple[date, date]: First day of the most recent import month, and first day of
            the previous month.
    """
    most_recent_month_col = "first_of_most_recent_month"
    previous_month_col = "first_of_previous_month"

    # Collecting here is cheap: it's a single-row aggregate, not a full materialisation.
    month_boundaries = (
        cqc_location_lf.select(
            pl.col(CQCLClean.cqc_location_import_date)
            .max()
            .dt.truncate("1mo")
            .alias(most_recent_month_col)
        )
        .with_columns(
            pl.col(most_recent_month_col).dt.offset_by("-1mo").alias(previous_month_col)
        )
        .collect()
    )
    return (
        month_boundaries[most_recent_month_col].item(),
        month_boundaries[previous_month_col].item(),
    )


def prepare_latest_cleaned_ascwds_workforce_data(
    ascwds_workplace_lf: pl.LazyFrame,
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Filters ASC-WDS workplace data to the latest import and classifies each account.

    Args:
        ascwds_workplace_lf (pl.LazyFrame): Cleaned ASC-WDS workplace data.

    Returns:
        tuple[pl.LazyFrame, pl.LazyFrame]: CQC-registered, non-head-office workplaces
            ready to join to CQC location data, and the ASC-WDS parent accounts lookup.
    """
    ascwds_workplace_lf = utils.filter_to_maximum_value_in_column(
        ascwds_workplace_lf, AWPClean.ascwds_workplace_import_date
    )
    labels_lf = cUtils.build_labels_lf(reconciliation_labels_dict)
    ascwds_workplace_lf = cUtils.apply_categorical_labels(
        ascwds_workplace_lf,
        labels_lf,
        list(reconciliation_labels_dict.keys()),
        add_as_new_column=False,
    )
    ascwds_workplace_lf = add_parents_or_singles_and_subs_column(ascwds_workplace_lf)

    ascwds_parent_accounts_lf = ascwds_workplace_lf.filter(
        pl.col(AWPClean.is_parent) == IsParent.is_parent
    ).select(
        AWPClean.nmds_id,
        AWPClean.establishment_id,
        AWPClean.establishment_name,
        AWPClean.organisation_id,
        AWPClean.establishment_type,
        AWPClean.region_id,
    )

    cqc_registered_workplace_lf = ascwds_workplace_lf.filter(
        pl.col(AWPClean.registration_type) == RegistrationType.cqc_regulated
    ).filter(
        pl.col(AWPClean.location_id).is_not_null()
        | (
            pl.col(AWPClean.location_id).is_null()
            & (pl.col(AWPClean.main_service_id) != MainServiceID.head_office_services)
        )
    )

    return cqc_registered_workplace_lf, ascwds_parent_accounts_lf


def filter_to_locations_relevant_to_reconciliation_process(
    merged_ascwds_cqc_lf: pl.LazyFrame,
    first_of_most_recent_month: date,
    first_of_previous_month: date,
) -> pl.LazyFrame:
    """Filters locations relevant for the reconciliation process.

    Includes locations that are either:
    1. Deregistered before the start of the current month and are parent accounts.
    2. Singles or sub-accounts that deregistered during the previous month.

    Args:
        merged_ascwds_cqc_lf (pl.LazyFrame): ASC-WDS workplace data joined to CQC
            location data.
        first_of_most_recent_month (date): First day of the most recent month.
        first_of_previous_month (date): First day of the previous month.

    Returns:
        pl.LazyFrame: Filtered LazyFrame relevant for reconciliation.
    """
    deregistered_before_current_month = (
        pl.col(CQCL.registration_status) == RegistrationStatus.deregistered
    ) & (pl.col(CQCL.deregistration_date) < first_of_most_recent_month)
    deregistered_since_previous_month = (
        pl.col(CQCL.deregistration_date) >= first_of_previous_month
    )
    null_registration_status = pl.col(CQCL.registration_status).is_null()
    is_parent_account = (
        pl.col(ReconColumn.parents_or_singles_and_subs)
        == ParentsOrSinglesAndSubs.parents
    )
    is_single_or_sub_account = (
        pl.col(ReconColumn.parents_or_singles_and_subs)
        == ParentsOrSinglesAndSubs.singles_and_subs
    )

    return merged_ascwds_cqc_lf.filter(
        null_registration_status
        | (
            deregistered_before_current_month
            & (
                is_parent_account
                | (is_single_or_sub_account & deregistered_since_previous_month)
            )
        )
    )


def build_single_and_subs_output(reconciliation_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Builds the reconciliation report for ASC-WDS single and sub accounts.

    Args:
        reconciliation_lf (pl.LazyFrame): Locations relevant to the reconciliation
            process.

    Returns:
        pl.LazyFrame: The singles and subs reconciliation report.
    """
    single_and_sub_lf = reconciliation_lf.filter(
        pl.col(ReconColumn.parents_or_singles_and_subs)
        == ParentsOrSinglesAndSubs.singles_and_subs
    ).with_columns(
        pl.when(pl.col(CQCL.deregistration_date).is_not_null())
        .then(pl.lit(SingleSubDescription.single_sub_deregistered_description))
        .otherwise(pl.lit(SingleSubDescription.single_sub_reg_type_description))
        .alias(ReconColumn.description),
        pl.lit(Subject.single_sub_subject_value).alias(ReconColumn.subject),
    )
    single_and_sub_lf = create_missing_columns_required_for_output(single_and_sub_lf)
    return final_column_selection(single_and_sub_lf)


def build_parents_output(
    ascwds_parent_accounts_lf: pl.LazyFrame,
    reconciliation_lf: pl.LazyFrame,
    first_of_previous_month: date,
) -> pl.LazyFrame:
    """Builds the reconciliation report for ASC-WDS parent accounts.

    Args:
        ascwds_parent_accounts_lf (pl.LazyFrame): ASC-WDS parent accounts lookup.
        reconciliation_lf (pl.LazyFrame): Locations relevant to the reconciliation
            process.
        first_of_previous_month (date): First day of the previous month.

    Returns:
        pl.LazyFrame: The parent accounts reconciliation report.
    """
    parents_lf = reconciliation_lf.filter(
        pl.col(ReconColumn.parents_or_singles_and_subs)
        == ParentsOrSinglesAndSubs.parents
    )
    new_issues_lf = parents_lf.filter(
        pl.col(CQCL.deregistration_date) >= first_of_previous_month
    )
    old_issues_lf = parents_lf.filter(
        pl.col(CQCL.deregistration_date) < first_of_previous_month
    )
    missing_or_incorrect_lf = parents_lf.filter(
        pl.col(CQCL.deregistration_date).is_null()
    )

    ascwds_parent_accounts_lf = join_nmds_ids_into_parent_accounts(
        new_issues_lf, ReconColumn.new_potential_subs, ascwds_parent_accounts_lf
    )
    ascwds_parent_accounts_lf = join_nmds_ids_into_parent_accounts(
        old_issues_lf, ReconColumn.old_potential_subs, ascwds_parent_accounts_lf
    )
    ascwds_parent_accounts_lf = join_nmds_ids_into_parent_accounts(
        missing_or_incorrect_lf,
        ReconColumn.missing_or_incorrect_potential_subs,
        ascwds_parent_accounts_lf,
    )

    ascwds_parent_accounts_lf = create_description_column_for_parent_accounts(
        ascwds_parent_accounts_lf
    )
    ascwds_parent_accounts_lf = ascwds_parent_accounts_lf.filter(
        pl.col(ReconColumn.description).str.len_chars() > 1
    ).with_columns(pl.lit(Subject.parent_subject_value).alias(ReconColumn.subject))

    ascwds_parent_accounts_lf = create_missing_columns_required_for_output(
        ascwds_parent_accounts_lf
    )
    return final_column_selection(ascwds_parent_accounts_lf)


def join_nmds_ids_into_parent_accounts(
    lf_with_issues: pl.LazyFrame,
    new_column_name: str,
    parent_accounts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Folds a labelled, comma-separated list of sub-account NMDS IDs onto each parent.

    IDs are sorted for a deterministic, human-readable output, since this feeds a
    manual-review report rather than anything order-sensitive.

    Args:
        lf_with_issues (pl.LazyFrame): Sub-accounts with an outstanding issue of one
            kind (e.g. newly deregistered).
        new_column_name (str): Name of the column to add, and its label prefix in the
            concatenated string (e.g. "new_potential_subs").
        parent_accounts_lf (pl.LazyFrame): The running parent-accounts LazyFrame to
            join the new column onto.

    Returns:
        pl.LazyFrame: `parent_accounts_lf` with `new_column_name` added.
    """
    subs_at_parent_lf = (
        lf_with_issues.select(AWPClean.organisation_id, AWPClean.nmds_id)
        .group_by(AWPClean.organisation_id)
        .agg(pl.col(AWPClean.nmds_id).unique().sort().alias(new_column_name))
        .with_columns(
            (
                pl.lit(f"{new_column_name}: ") + pl.col(new_column_name).list.join(", ")
            ).alias(new_column_name)
        )
    )
    return parent_accounts_lf.join(
        subs_at_parent_lf, on=AWPClean.organisation_id, how="left"
    )


def create_description_column_for_parent_accounts(
    parent_accounts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Concatenates the new/old/missing-or-incorrect potential-subs columns into one.

    Args:
        parent_accounts_lf (pl.LazyFrame): Parent accounts with the three
            potential-subs columns already joined in.

    Returns:
        pl.LazyFrame: The same data with `description` added.
    """

    def not_null_with_trailing_space(column: str) -> pl.Expr:
        return (
            pl.when(pl.col(column).is_not_null())
            .then(pl.col(column) + " ")
            .otherwise(pl.lit(""))
        )

    return parent_accounts_lf.with_columns(
        (
            not_null_with_trailing_space(ReconColumn.new_potential_subs)
            + not_null_with_trailing_space(ReconColumn.old_potential_subs)
            + not_null_with_trailing_space(
                ReconColumn.missing_or_incorrect_potential_subs
            )
        ).alias(ReconColumn.description)
    )


def create_missing_columns_required_for_output(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Adds the fixed and derived columns required for the reconciliation report.

    Most of these are fixed literals required by the destination system (a manual
    call-log import), not business logic.

    Args:
        lf (pl.LazyFrame): Reconciliation data for one output branch.

    Returns:
        pl.LazyFrame: The same data with the report's remaining columns added.
    """
    lf = lf.rename(
        {
            AWPClean.establishment_type: ReconColumn.sector,
            AWPClean.region_id: ReconColumn.sfc_region,
            AWPClean.establishment_name: ReconColumn.name,
        }
    )
    requester_name = pl.col(AWPClean.nmds_id) + " " + pl.col(ReconColumn.name)

    return lf.with_columns(
        pl.col(AWPClean.nmds_id).alias(ReconColumn.nmds),
        pl.col(AWPClean.nmds_id).alias(ReconColumn.workplace_id),
        requester_name.alias(ReconColumn.requester_name),
        requester_name.alias(ReconColumn.requester_name_2),
        pl.lit("Open").alias(ReconColumn.status),
        pl.lit("_").alias(ReconColumn.technician),
        pl.lit("No").alias(ReconColumn.manual_call_log),
        pl.lit("Internal").alias(ReconColumn.mode),
        pl.lit("Priority 5").alias(ReconColumn.priority),
        pl.lit("CQC work").alias(ReconColumn.category),
        pl.lit("CQC work").alias(ReconColumn.sub_category),
        pl.lit("Yes").alias(ReconColumn.is_requester_named),
        pl.lit("N/A").alias(ReconColumn.security_question),
        pl.lit("ASC-WDS").alias(ReconColumn.website),
        pl.lit("CQC work").alias(ReconColumn.item),
        pl.lit(0).alias(ReconColumn.phone),
    )


def final_column_selection(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Selects and orders the reconciliation report's output columns.

    Args:
        lf (pl.LazyFrame): Reconciliation data with all report columns added.

    Returns:
        pl.LazyFrame: The report's columns, sorted by description then nmds.
    """
    return lf.select(
        ReconColumn.subject,
        ReconColumn.nmds,
        ReconColumn.name,
        ReconColumn.description,
        ReconColumn.requester_name_2,
        ReconColumn.requester_name,
        ReconColumn.sector,
        ReconColumn.status,
        ReconColumn.technician,
        ReconColumn.sfc_region,
        ReconColumn.manual_call_log,
        ReconColumn.mode,
        ReconColumn.priority,
        ReconColumn.category,
        ReconColumn.sub_category,
        ReconColumn.is_requester_named,
        ReconColumn.security_question,
        ReconColumn.website,
        ReconColumn.item,
        ReconColumn.phone,
        ReconColumn.workplace_id,
    ).sort(ReconColumn.description, ReconColumn.nmds)


if __name__ == "__main__":
    print("Fargate job 'reconciliation' starting...")
    print(f"Job parameters: {sys.argv}")

    args = utils.get_args(
        (
            "--cqc_locations_snapshot_source",
            "Source s3 directory for latest run CQC locations snapshot dataset",
        ),
        (
            "--ascwds_workplace_source",
            "Source s3 directory for ASC-WDS workplace parquet dataset",
        ),
        (
            "--reconciliation_single_and_subs_destination",
            "Destination s3 directory for the singles and subs reconciliation report",
        ),
        (
            "--reconciliation_parents_destination",
            "Destination s3 directory for the parents reconciliation report",
        ),
    )
    main(
        args.cqc_locations_snapshot_source,
        args.ascwds_workplace_source,
        args.reconciliation_single_and_subs_destination,
        args.reconciliation_parents_destination,
    )

    print("Fargate job 'reconciliation' complete")
