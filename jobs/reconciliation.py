import sys
from pyspark.sql import DataFrame, functions as F
from typing import Tuple
from datetime import date

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
    CqcLocationCleanedValues as CQCLValues,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.reconciliation_utils.reconciliation_values import (
    ReconciliationColumns as ReconColumn,
    ReconciliationValues as ReconValues,
    ReconciliationDict as ReconDict,
)

cqc_locations_columns_to_import = [
    Keys.import_date,
    CQCL.location_id,
    CQCL.registration_status,
    CQCL.deregistration_date,
]
cleaned_ascwds_workplace_columns_to_import = [
    AWPClean.ascwds_workplace_import_date,
    AWPClean.establishment_id,
    AWPClean.nmds_id,
    AWPClean.is_parent,
    AWPClean.parent_id,
    AWPClean.organisation_id,
    AWPClean.parent_permission,
    AWPClean.establishment_type,
    AWPClean.registration_type,
    AWPClean.location_id,
    AWPClean.main_service_id,
    AWPClean.establishment_name,
    AWPClean.region_id,
]


def main(
    cqc_location_api_source: str,
    cleaned_ascwds_workplace_source: str,
    reconciliation_single_and_subs_destination: str,
    reconciliation_parents_destination: str,
) -> DataFrame:
    cqc_location_df = utils.read_from_parquet(
        cqc_location_api_source, cqc_locations_columns_to_import
    )
    ascwds_workplace_df = utils.read_from_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=cleaned_ascwds_workplace_columns_to_import,
    )

    cqc_location_df = prepare_most_recent_cqc_location_df(cqc_location_df)
    (
        first_of_most_recent_month,
        first_of_previous_month,
    ) = collect_dates_to_use(cqc_location_df)

    (
        latest_ascwds_workplace_df,
        ascwds_parent_accounts_df,
    ) = prepare_latest_cleaned_ascwds_workforce_data(ascwds_workplace_df)

    merged_ascwds_cqc_df = join_cqc_location_data_into_ascwds_workplace_df(
        latest_ascwds_workplace_df, cqc_location_df
    )

    reconciliation_df = filter_to_locations_relevant_to_reconcilition_process(
        merged_ascwds_cqc_df, first_of_most_recent_month, first_of_previous_month
    )
    reconciliation_df = remove_ascwds_head_office_accounts_without_location_ids(
        reconciliation_df
    )
    single_and_sub_df = create_reconciliation_output_for_ascwds_single_and_sub_accounts(
        reconciliation_df
    )
    parents_df = create_reconciliation_output_for_ascwds_parent_accounts(
        ascwds_parent_accounts_df, reconciliation_df, first_of_previous_month
    )

    write_to_csv(single_and_sub_df, reconciliation_single_and_subs_destination)
    write_to_csv(parents_df, reconciliation_parents_destination)


def prepare_most_recent_cqc_location_df(cqc_location_df: DataFrame) -> DataFrame:
    cqc_location_df = cUtils.column_to_date(
        cqc_location_df, Keys.import_date, CQCLClean.cqc_location_import_date
    ).drop(Keys.import_date)
    cqc_location_df = utils.filter_df_to_maximum_value_in_column(
        cqc_location_df, CQCLClean.cqc_location_import_date
    )
    cqc_location_df = utils.format_date_fields(
        cqc_location_df,
        date_column_identifier=CQCL.deregistration_date,
        raw_date_format="yyyy-MM-dd",
    )
    return cqc_location_df


def collect_dates_to_use(df: DataFrame) -> Tuple[date, date]:
    most_recent: str = "most_recent"
    start_of_month: str = "start_of_month"
    start_of_previous_month: str = "start_of_previous_month"

    dates_df = df.select(F.max(CQCLClean.cqc_location_import_date).alias(most_recent))
    dates_df = dates_df.withColumn(start_of_month, F.trunc(F.col(most_recent), "MM"))
    dates_df = dates_df.withColumn(
        start_of_previous_month, F.add_months(F.col(start_of_month), -1)
    )
    dates_collected = dates_df.collect()
    first_of_most_recent_month = dates_collected[0][start_of_month]
    first_of_previous_month = dates_collected[0][start_of_previous_month]

    return (
        first_of_most_recent_month,
        first_of_previous_month,
    )


def prepare_latest_cleaned_ascwds_workforce_data(
    ascwds_workplace_df: str,
) -> Tuple[DataFrame, DataFrame]:
    df = utils.filter_df_to_maximum_value_in_column(
        ascwds_workplace_df, AWPClean.ascwds_workplace_import_date
    )
    df = df.replace(ReconDict.region_id_dict, subset=[AWPClean.region_id])
    df = df.replace(
        ReconDict.establishment_type_dict, subset=[AWPClean.establishment_type]
    )
    df = add_parent_sub_or_single_col_to_df(df)
    df = add_ownership_col_to_df(df)
    df = add_potentials_col_to_df(df)
    cqc_registered_accounts_df = filter_to_cqc_registration_type_only(df)
    parent_accounts_df = get_ascwds_parent_accounts(df)

    return cqc_registered_accounts_df, parent_accounts_df


def add_parent_sub_or_single_col_to_df(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.parent_sub_or_single,
        F.when(
            (F.col(AWPClean.is_parent) == 1),
            F.lit(ReconValues.parent),
        )
        .when(
            ((F.col(AWPClean.is_parent) == 0) & (F.col(AWPClean.parent_id) > 0)),
            F.lit(ReconValues.subsidiary),
        )
        .otherwise(F.lit(ReconValues.single)),
    ).drop(AWPClean.parent_id)


def add_ownership_col_to_df(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.ownership,
        F.when(
            (F.col(AWPClean.parent_permission) == 1),
            F.lit(ReconValues.parent),
        ).otherwise(F.lit(ReconValues.workplace)),
    ).drop(AWPClean.parent_permission)


def add_potentials_col_to_df(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.potentials,
        F.when(
            (
                (
                    (F.col(ReconColumn.parent_sub_or_single) == ReconValues.single)
                    | (
                        F.col(ReconColumn.parent_sub_or_single)
                        == ReconValues.subsidiary
                    )
                )
                & (F.col(ReconColumn.ownership) == ReconValues.workplace)
            ),
            F.lit(ReconValues.singles_and_subs),
        ).otherwise(F.lit(ReconValues.parents)),
    ).drop(ReconColumn.parent_sub_or_single, ReconColumn.ownership)


def filter_to_cqc_registration_type_only(df: DataFrame) -> DataFrame:
    return df.filter(F.col(AWPClean.registration_type) == "2")


def get_ascwds_parent_accounts(df: DataFrame) -> DataFrame:
    return df.filter(F.col(AWPClean.is_parent) == "1").select(
        AWPClean.nmds_id,
        AWPClean.establishment_id,
        AWPClean.establishment_name,
        AWPClean.organisation_id,
        AWPClean.establishment_type,
        AWPClean.region_id,
    )


def join_cqc_location_data_into_ascwds_workplace_df(
    ascwds_workplace_df: DataFrame,
    cqc_location_df: DataFrame,
) -> DataFrame:
    ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed(
        AWPClean.location_id, CQCL.location_id
    )
    return ascwds_workplace_df.join(cqc_location_df, CQCL.location_id, "left")


def filter_to_locations_relevant_to_reconcilition_process(
    df: DataFrame, first_of_most_recent_month: date, first_of_previous_month: date
) -> DataFrame:
    return df.where(
        F.col(CQCL.registration_status).isNull()
        | (
            (
                (F.col(CQCL.registration_status) == CQCLValues.deregistered)
                & (F.col(CQCL.deregistration_date) < first_of_most_recent_month)
            )
            & (
                (F.col(ReconColumn.potentials) == ReconValues.parents)
                | (
                    (F.col(ReconColumn.potentials) == ReconValues.singles_and_subs)
                    & (F.col(CQCL.deregistration_date) >= first_of_previous_month)
                )
            )
        )
    )


def remove_ascwds_head_office_accounts_without_location_ids(
    df: DataFrame,
) -> DataFrame:
    return df.where(
        (F.col(CQCL.registration_status).isNotNull())
        | (
            (F.col(CQCL.registration_status).isNull())
            & (F.col(AWPClean.main_service_id) != "72")
        )
    )


def create_reconciliation_output_for_ascwds_single_and_sub_accounts(
    reconciliation_df: DataFrame,
) -> DataFrame:
    singles_and_subs_df = utils.select_rows_with_value(
        reconciliation_df, ReconColumn.potentials, ReconValues.singles_and_subs
    )
    singles_and_subs_df = add_singles_and_sub_description_column(singles_and_subs_df)
    singles_and_subs_df = add_subject_column(
        singles_and_subs_df, ReconValues.single_sub_subject_value
    )
    singles_and_subs_df = create_missing_columns_required_for_output(
        singles_and_subs_df
    )
    return final_column_selection(singles_and_subs_df)


def add_subject_column(df: DataFrame, subject_value: str):
    return df.withColumn(ReconColumn.subject, F.lit(subject_value))


def add_singles_and_sub_description_column(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.description,
        F.when(
            F.col(CQCL.deregistration_date).isNotNull(),
            F.lit(ReconValues.single_sub_deregistered_description),
        ).otherwise(F.lit(ReconValues.single_sub_reg_type_description)),
    )


def create_reconciliation_output_for_ascwds_parent_accounts(
    ascwds_parent_accounts_df: DataFrame,
    reconciliation_df: DataFrame,
    first_of_previous_month: str,
) -> DataFrame:
    reconciliation_parents_df = utils.select_rows_with_value(
        reconciliation_df, ReconColumn.potentials, ReconValues.parents
    )
    new_issues_df = reconciliation_parents_df.where(
        F.col(CQCL.deregistration_date) >= first_of_previous_month
    )
    old_issues_df = reconciliation_parents_df.where(
        F.col(CQCL.deregistration_date) < first_of_previous_month
    )
    missing_or_incorrect_df = reconciliation_parents_df.where(
        F.col(CQCL.deregistration_date).isNull()
    )

    ascwds_parent_accounts_df = join_array_of_nmdsids_into_parent_account_df(
        new_issues_df, ReconColumn.new_potential_subs, ascwds_parent_accounts_df
    )
    ascwds_parent_accounts_df = join_array_of_nmdsids_into_parent_account_df(
        old_issues_df, ReconColumn.old_potential_subs, ascwds_parent_accounts_df
    )
    ascwds_parent_accounts_df = join_array_of_nmdsids_into_parent_account_df(
        missing_or_incorrect_df,
        ReconColumn.missing_or_incorrect_potential_subs,
        ascwds_parent_accounts_df,
    )

    ascwds_parent_accounts_df = create_description_column_for_parent_accounts(
        ascwds_parent_accounts_df
    )
    ascwds_parent_accounts_df = ascwds_parent_accounts_df.where(
        F.length(ReconColumn.description) > 1
    )

    ascwds_parent_accounts_df = add_subject_column(
        ascwds_parent_accounts_df, ReconValues.parent_subject_value
    )

    ascwds_parent_accounts_df = create_missing_columns_required_for_output(
        ascwds_parent_accounts_df
    )

    return final_column_selection(ascwds_parent_accounts_df)


def join_array_of_nmdsids_into_parent_account_df(
    df_with_issues: DataFrame, new_col_name: str, unique_df: DataFrame
) -> DataFrame:
    unique_issues_parents_df = organisation_id_with_array_of_nmdsids(
        df_with_issues, new_col_name
    )
    return unique_df.join(unique_issues_parents_df, AWPClean.organisation_id, "left")


def organisation_id_with_array_of_nmdsids(
    df: DataFrame, new_col_name: str
) -> DataFrame:
    subs_at_parent_df = df.select(AWPClean.organisation_id, AWPClean.nmds_id)
    subs_at_parent_df = subs_at_parent_df.groupby(AWPClean.organisation_id).agg(
        F.collect_set(AWPClean.nmds_id).alias(new_col_name)
    )
    subs_at_parent_df = subs_at_parent_df.withColumn(
        new_col_name, F.concat_ws(", ", F.col(new_col_name))
    )
    return subs_at_parent_df.withColumn(
        new_col_name, F.concat(F.lit(new_col_name), F.lit(": "), F.col(new_col_name))
    )


def create_description_column_for_parent_accounts(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.description,
        F.concat(
            F.when(
                F.col(ReconColumn.new_potential_subs).isNotNull(),
                F.concat(F.col(ReconColumn.new_potential_subs), F.lit(" ")),
            ).otherwise(F.lit("")),
            F.when(
                F.col(ReconColumn.old_potential_subs).isNotNull(),
                F.concat(F.col(ReconColumn.old_potential_subs), F.lit(" ")),
            ).otherwise(F.lit("")),
            F.when(
                F.col(ReconColumn.missing_or_incorrect_potential_subs).isNotNull(),
                F.concat(
                    F.col(ReconColumn.missing_or_incorrect_potential_subs), F.lit(" ")
                ),
            ).otherwise(F.lit("")),
        ),
    )


def create_missing_columns_required_for_output(df: DataFrame) -> DataFrame:
    df = df.withColumnRenamed(AWPClean.establishment_type, ReconColumn.sector)
    df = df.withColumnRenamed(AWPClean.region_id, ReconColumn.sfc_region)
    df = df.withColumnRenamed(AWPClean.establishment_name, ReconColumn.name)
    return (
        df.withColumn(ReconColumn.nmds, F.col(AWPClean.nmds_id))
        .withColumn(ReconColumn.workplace_id, F.col(AWPClean.nmds_id))
        .withColumn(
            ReconColumn.requester_name,
            F.concat(ReconColumn.nmds, F.lit(" "), ReconColumn.name),
        )
        .withColumn(
            ReconColumn.requester_name_2,
            F.concat(ReconColumn.nmds, F.lit(" "), ReconColumn.name),
        )
        .withColumn(ReconColumn.status, F.lit("Open"))
        .withColumn(ReconColumn.technician, F.lit("_"))
        .withColumn(ReconColumn.manual_call_log, F.lit("No"))
        .withColumn(ReconColumn.mode, F.lit("Internal"))
        .withColumn(ReconColumn.priority, F.lit("Priority 5"))
        .withColumn(ReconColumn.category, F.lit("Workplace"))
        .withColumn(ReconColumn.sub_category, F.lit("Reports"))
        .withColumn(ReconColumn.is_requester_named, F.lit("Yes"))
        .withColumn(ReconColumn.security_question, F.lit("N/A"))
        .withColumn(ReconColumn.website, F.lit("ASC-WDS"))
        .withColumn(ReconColumn.item, F.lit("CQC work"))
        .withColumn(ReconColumn.phone, F.lit(0))
    )


def final_column_selection(df: DataFrame) -> DataFrame:
    return df.select(
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


def write_to_csv(df: DataFrame, output_dir: str):
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)


if __name__ == "__main__":
    print("Spark job 'reconciliation' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_api_source,
        cleaned_ascwds_workplace_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    ) = utils.collect_arguments(
        (
            "--cqc_location_api_source",
            "Source s3 directory for initial CQC location api dataset",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for cleaned parquet ASCWDS workplace dataset",
        ),
        (
            "--reconciliation_single_and_subs_destination",
            "Destination s3 directory for singles and subs reconciliation CSV dataset",
        ),
        (
            "--reconciliation_parents_destination",
            "Destination s3 directory for parents reconciliation CSV dataset",
        ),
    )
    main(
        cqc_location_api_source,
        cleaned_ascwds_workplace_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    )

    print("Spark job 'reconciliation' complete")
