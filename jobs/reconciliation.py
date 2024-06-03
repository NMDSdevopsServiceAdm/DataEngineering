import sys
from pyspark.sql import DataFrame, functions as F
from typing import Tuple
from datetime import date

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
    CqcLocationCleanedValues as CQCLValues,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.reconciliation_utils.reconciliation_values import (
    ReconciliationColumns as ReconColumn,
    ReconciliationValues as ReconValues,
)
from utils.value_labels.reconciliation.label_dictionary import (
    labels_dict as reconciliation_labels_dict,
)

cqc_locations_columns_to_import = [
    Keys.import_date,
    CQCL.location_id,
    CQCL.registration_status,
    CQCL.deregistration_date,
]


def main(
    cqc_location_api_source: str,
    ascwds_reconciliation_source: str,
    reconciliation_single_and_subs_destination: str,
    reconciliation_parents_destination: str,
):
    spark = utils.get_spark()
    spark.sql("set spark.sql.broadcastTimeout = 1000")

    cqc_location_df = utils.read_from_parquet(
        cqc_location_api_source, cqc_locations_columns_to_import
    )
    ascwds_workplace_df = utils.read_from_parquet(ascwds_reconciliation_source)

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

    latest_ascwds_workplace_df.unpersist()
    cqc_location_df.unpersist()

    reconciliation_df = filter_to_locations_relevant_to_reconcilition_process(
        merged_ascwds_cqc_df, first_of_most_recent_month, first_of_previous_month
    )
    merged_ascwds_cqc_df.unpersist()

    single_and_sub_df = create_reconciliation_output_for_ascwds_single_and_sub_accounts(
        reconciliation_df
    )
    parents_df = create_reconciliation_output_for_ascwds_parent_accounts(
        ascwds_parent_accounts_df, reconciliation_df, first_of_previous_month
    )
    reconciliation_df.unpersist()

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
    ascwds_workplace_df: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    df = utils.filter_df_to_maximum_value_in_column(
        ascwds_workplace_df, AWPClean.ascwds_workplace_import_date
    )
    df = cUtils.apply_categorical_labels(
        df,
        reconciliation_labels_dict,
        reconciliation_labels_dict.keys(),
        add_as_new_column=False,
    )
    df = add_parents_or_singles_and_subs_col_to_df(df)

    cqc_registered_accounts_df = filter_to_cqc_registration_type_only(df)
    cqc_registered_accounts_df = (
        remove_ascwds_head_office_accounts_without_location_ids(
            cqc_registered_accounts_df
        )
    )

    parent_accounts_df = get_ascwds_parent_accounts(df)

    return cqc_registered_accounts_df, parent_accounts_df


def add_parents_or_singles_and_subs_col_to_df(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        ReconColumn.parents_or_singles_and_subs,
        F.when(
            (
                (F.col(AWPClean.is_parent) == "Yes")
                | (
                    (F.col(AWPClean.is_parent) == "No")
                    & (F.col(AWPClean.parent_permission) == "Parent has ownership")
                )
            ),
            F.lit(ReconValues.parents),
        ).otherwise(F.lit(ReconValues.singles_and_subs)),
    )
    return df


def filter_to_cqc_registration_type_only(df: DataFrame) -> DataFrame:
    return df.filter(F.col(AWPClean.registration_type) == "CQC regulated")


def get_ascwds_parent_accounts(df: DataFrame) -> DataFrame:
    df = df.filter(F.col(AWPClean.is_parent) == ReconValues.is_parent).select(
        AWPClean.nmds_id,
        AWPClean.establishment_id,
        AWPClean.establishment_name,
        AWPClean.organisation_id,
        AWPClean.establishment_type,
        AWPClean.region_id,
    )
    return df


def remove_ascwds_head_office_accounts_without_location_ids(
    df: DataFrame,
) -> DataFrame:
    df = df.where(
        (F.col(AWPClean.location_id).isNotNull())
        | (
            (F.col(AWPClean.location_id).isNull())
            & (F.col(AWPClean.main_service_id) != "Head office services")
        )
    )
    return df


def join_cqc_location_data_into_ascwds_workplace_df(
    ascwds_workplace_df: DataFrame,
    cqc_location_df: DataFrame,
) -> DataFrame:
    ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed(
        AWPClean.location_id, CQCL.location_id
    )
    ascwds_workplace_df = ascwds_workplace_df.join(
        cqc_location_df, CQCL.location_id, "left"
    )
    return ascwds_workplace_df


def filter_to_locations_relevant_to_reconcilition_process(
    df: DataFrame, first_of_most_recent_month: date, first_of_previous_month: date
) -> DataFrame:
    df = df.where(
        F.col(CQCL.registration_status).isNull()
        | (
            (
                (F.col(CQCL.registration_status) == CQCLValues.deregistered)
                & (F.col(CQCL.deregistration_date) < first_of_most_recent_month)
            )
            & (
                (F.col(ReconColumn.parents_or_singles_and_subs) == ReconValues.parents)
                | (
                    (
                        F.col(ReconColumn.parents_or_singles_and_subs)
                        == ReconValues.singles_and_subs
                    )
                    & (F.col(CQCL.deregistration_date) >= first_of_previous_month)
                )
            )
        )
    )
    return df


def create_reconciliation_output_for_ascwds_single_and_sub_accounts(
    reconciliation_df: DataFrame,
) -> DataFrame:
    singles_and_subs_df = utils.select_rows_with_value(
        reconciliation_df,
        ReconColumn.parents_or_singles_and_subs,
        ReconValues.singles_and_subs,
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
    df = df.withColumn(
        ReconColumn.description,
        F.when(
            F.col(CQCL.deregistration_date).isNotNull(),
            F.lit(ReconValues.single_sub_deregistered_description),
        ).otherwise(F.lit(ReconValues.single_sub_reg_type_description)),
    )
    return df


def create_reconciliation_output_for_ascwds_parent_accounts(
    ascwds_parent_accounts_df: DataFrame,
    reconciliation_df: DataFrame,
    first_of_previous_month: str,
) -> DataFrame:
    reconciliation_parents_df = utils.select_rows_with_value(
        reconciliation_df, ReconColumn.parents_or_singles_and_subs, ReconValues.parents
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
    subs_at_parent_df = df_with_issues.select(
        AWPClean.organisation_id, AWPClean.nmds_id
    )
    subs_at_parent_df = subs_at_parent_df.groupby(AWPClean.organisation_id).agg(
        F.collect_set(AWPClean.nmds_id).alias(new_col_name)
    )
    subs_at_parent_df = subs_at_parent_df.withColumn(
        new_col_name, F.concat_ws(", ", F.col(new_col_name))
    )
    subs_at_parent_df = subs_at_parent_df.withColumn(
        new_col_name, F.concat(F.lit(new_col_name), F.lit(": "), F.col(new_col_name))
    )
    df_with_array_of_ids = unique_df.join(
        subs_at_parent_df, AWPClean.organisation_id, "left"
    )
    return df_with_array_of_ids


def create_description_column_for_parent_accounts(df: DataFrame) -> DataFrame:
    df = df.withColumn(
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
    return df


def create_missing_columns_required_for_output(df: DataFrame) -> DataFrame:
    df = df.withColumnRenamed(AWPClean.establishment_type, ReconColumn.sector)
    df = df.withColumnRenamed(AWPClean.region_id, ReconColumn.sfc_region)
    df = df.withColumnRenamed(AWPClean.establishment_name, ReconColumn.name)
    df = (
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
    return df


def final_column_selection(df: DataFrame) -> DataFrame:
    df = df.select(
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

    return df


def write_to_csv(df: DataFrame, output_dir: str):
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)


if __name__ == "__main__":
    print("Spark job 'reconciliation' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_api_source,
        ascwds_reconciliation_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    ) = utils.collect_arguments(
        (
            "--cqc_location_api_source",
            "Source s3 directory for initial CQC location api dataset",
        ),
        (
            "--ascwds_reconciliation_source",
            "Source s3 directory for ASCWDS reconciliation parquet dataset",
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
        ascwds_reconciliation_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    )

    print("Spark job 'reconciliation' complete")
